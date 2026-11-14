#!/usr/bin/env python3
"""场景回放端到端测试：在真实运行的行情源 + 采集器上验证场景 API 全链路。

用法：
    python3 scripts/scenario_replay_test.py \
        [--source http://localhost:8000] [--collector http://localhost:8001]

覆盖需求：
1. 加载初始双侧盘口 + 有序多档增量，手动步进，每次只向 /ws 发布一条；
2. /snapshot 返回当前步进状态；重复加载同一场景得到完全相同的序号与盘口；
3. 加载/清除场景断开旧 WS 订阅（采集器随之失步再重连）；
4. 切换前发起的延迟快照返回 scenario_changed(409)，读不到新场景；
5. 非法场景返回 4xx 且不改变当前流；
6. 清除后恢复随机行情，故障注入重新可用；
7. /docs 与 openapi 暴露全部场景接口。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import threading
import time

import httpx
import websockets

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))
    if not cond:
        FAILURES.append(name)


def wait_until(src: httpx.Client, col: httpx.Client, predicate, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    return False


SCENARIO = {
    "symbol": "CU2601",
    "name": "cu-e2e-replay",
    "base_seq": 100,
    "bids": [[69990, 5], [69980, 3], [69970, 8]],
    "asks": [[70010, 7], [70020, 2]],
    "increments": [
        [{"side": "bid", "price": 69960, "qty": 4}],                                   # 101 新增
        [{"side": "ask", "price": 70010, "qty": 0},                                    # 102 删档
         {"side": "bid", "price": 69990, "qty": 9}],                                   #     改量
        [{"side": "ask", "price": 70030, "qty": 6}, {"side": "bid", "price": 69980, "qty": 0}],  # 103
    ],
}


async def _reader_loop(uri: str, frames: list[dict], errors: list, stop: asyncio.Event) -> None:
    """持续读取一个 WS 连接的全部帧，直到连接关闭或收到停止信号。"""
    try:
        async with websockets.connect(uri) as ws:
            stop_task = asyncio.create_task(stop.wait())
            recv_task = asyncio.create_task(ws.recv())
            while not stop.is_set():
                done, _ = await asyncio.wait({stop_task, recv_task}, return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done:
                    recv_task.cancel()
                    await asyncio.gather(recv_task, return_exceptions=True)
                    await ws.close()
                    break
                frames.append(json.loads(recv_task.result()))
                recv_task = asyncio.create_task(ws.recv())
    except Exception as exc:  # 连接被服务端关闭是正常路径
        errors.append(exc)


class FrameCollector:
    """一个后台常驻 WS 订阅；步进必须在订阅存活期间发生才能收到帧。"""

    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.frames: list[dict] = []
        self.errors: list = []
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.ready = threading.Event()
        self._stop: asyncio.Event | None = None

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._stop = asyncio.Event()
        self.loop.call_soon_threadsafe(self.ready.set)
        self.loop.run_until_complete(_reader_loop(self.uri, self.frames, self.errors, self._stop))

    def __enter__(self) -> "FrameCollector":
        self.thread.start()
        self.ready.wait(5)
        time.sleep(0.3)  # 等握手完成，确保后续步进帧不会错过
        return self

    def __exit__(self, *exc) -> None:
        assert self._stop is not None
        self.loop.call_soon_threadsafe(self._stop.set)
        self.thread.join(timeout=3)

    def increments_since(self, index: int) -> list[dict]:
        return [f for f in self.frames[index:] if f.get("type") == "increment"]

    def wait_increment_count(self, count: int, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if sum(1 for f in self.frames if f.get("type") == "increment") >= count:
                return True
            time.sleep(0.05)
        return False


async def _expect_disconnect(uri: str, ready: threading.Event) -> bool:
    """建立 WS 后持续读，直到服务端关闭连接（心跳/增量都不算结束）。"""
    try:
        async with websockets.connect(uri) as ws:
            ready.set()
            while True:
                await ws.recv()
    except websockets.ConnectionClosed:
        return True
    except Exception:
        return False


def expect_disconnect(uri: str) -> bool:
    ready = threading.Event()

    async def run():
        return await _expect_disconnect(uri, ready)

    task = {"result": None}

    def worker():
        task["result"] = asyncio.run(run())

    th = threading.Thread(target=worker, daemon=True)
    th.start()
    ready.wait(5)
    th.join(8)
    return task["result"] is True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="http://localhost:8000")
    ap.add_argument("--collector", default="http://localhost:8001")
    args = ap.parse_args()

    src = httpx.Client(base_url=args.source, timeout=10)
    col = httpx.Client(base_url=args.collector, timeout=10)
    ws_uri = args.source.replace("http://", "ws://") + "/ws"

    # 起点：清除残留，确认随机流与采集器同步
    src.delete("/scenario")
    src.post("/control/snapshot_delay", params={"seconds": 0})
    wait_until(src, col, lambda: col.get("/status").json().get("synced") is True)
    check("初始采集器已同步", col.get("/status").json()["synced"] is True)

    print("\n== 1. 非法场景不得改变当前流 ==")
    before = src.get("/snapshot").json()
    epoch_before = src.get("/status").json()["epoch"]
    for bad in [
        {**SCENARIO, "symbol": "WRONG"},
        {**SCENARIO, "bids": []},
        {**SCENARIO, "increments": [[{"side": "bid", "price": 1, "qty": -1}]]},
    ]:
        r = src.post("/scenario/load", json=bad)
        check(f"非法场景被拒绝 ({r.status_code})", r.status_code in (400, 422), r.text[:80])
    after = src.get("/snapshot").json()
    st = src.get("/status").json()
    check("非法场景后盘口序号不变", after == before, f"seq {before['seq']} -> {after['seq']}")
    check("非法场景后 epoch 不变", st["epoch"] == epoch_before)
    check("非法场景后仍为随机流", st["mode"] == "random")

    print("\n== 2. 加载场景：断开旧订阅，采集器失步后重新衔接到场景快照 ==")
    r = src.post("/scenario/load", json=SCENARIO)
    check("加载返回 200", r.status_code == 200, r.text)
    check("加载断开了旧连接", r.json()["disconnected"] >= 1)
    check("场景模式已激活", src.get("/status").json()["mode"] == "scenario")
    snap0 = src.get("/snapshot").json()
    check("快照序号=base_seq", snap0["seq"] == 100, str(snap0["seq"]))
    check("初始买盘正确", snap0["bids"] == [[69990, 5], [69980, 3], [69970, 8]])
    check("初始卖盘正确", snap0["asks"] == [[70010, 7], [70020, 2]])
    # 采集器因断线失步，随后用场景快照重新同步
    check("断线后采集器进入失步", wait_until(src, col, lambda: col.get("/status").json()["synced"] is False))
    check("采集器重新同步到场景", wait_until(src, col, lambda: col.get("/status").json().get("synced") is True))
    book0 = col.get("/book").json()
    check("采集器盘口序号=100", book0["seq"] == 100, str(book0["seq"]))
    check("采集器盘口与行情源一致", book0["bids"] == snap0["bids"] and book0["asks"] == snap0["asks"])

    print("\n== 3. 手动步进：每次只发布一条，采集器逐条衔接 ==")
    with FrameCollector(ws_uri) as collector_ws:
        time.sleep(1.0)
        check("不步进时 WS 无增量（仅心跳）",
              all(f.get("type") == "heartbeat" for f in collector_ws.frames))
        for expected_seq in (101, 102, 103):
            seen_before = len(collector_ws.frames)
            r = src.post("/scenario/step")
            check(f"步进 {expected_seq} 返回 200", r.status_code == 200)
            check("步进发布的序号正确", r.json()["published"]["seq"] == expected_seq)
            # 等待且只等到这一条新增量
            got_one = collector_ws.wait_increment_count(expected_seq - 100)
            time.sleep(0.4)  # 留窗口证明没有第二条增量被补发
            incs = collector_ws.increments_since(seen_before)
            check(f"步进 {expected_seq} 只发布一条增量",
                  got_one and len(incs) == 1 and incs[0]["seq"] == expected_seq,
                  f"got {[f.get('seq') for f in incs]}")
            check("采集器逐条跟到该序号",
                  wait_until(src, col, lambda: col.get("/book").json().get("seq") == expected_seq))
            source_snap = src.get("/snapshot").json()
            collector_book = col.get("/book").json()
            check(f"序号 {expected_seq} 两端盘口一致",
                  collector_book["bids"] == source_snap["bids"]
                  and collector_book["asks"] == source_snap["asks"])
    # 多档原子性：102 含删档+改量，103 含两侧各一档
    final_snap = src.get("/snapshot").json()
    check("删档生效（70010 消失）", all(p != 70010 for p, _ in final_snap["asks"]))
    check("改量生效（69990=9）", dict(final_snap["bids"]).get(69990) == 9)
    check("步进状态显示播完", src.get("/scenario").json()["finished"] is True)
    r = src.post("/scenario/step")
    check("播完再步进返回 409", r.status_code == 409 and r.json()["error"] == "scenario_finished")

    print("\n== 4. 重复加载同一场景：序号与盘口完全确定 ==")
    def record_timeline() -> list[dict]:
        src.post("/scenario/load", json=SCENARIO)
        timeline = [src.get("/snapshot").json()]
        for _ in range(3):
            src.post("/scenario/step")
            timeline.append(src.get("/snapshot").json())
        return timeline

    run_a = record_timeline()
    run_b = record_timeline()
    check("两次回放快照序列完全一致", run_a == run_b)
    check("序号序列确定 [100..103]", [s["seq"] for s in run_a] == [100, 101, 102, 103])

    print("\n== 5. 加载/清除断开旧订阅 ==")
    src.post("/scenario/load", json=SCENARIO)  # 确保处于场景模式
    # 建立订阅后清除场景：连接必须被服务端关闭
    th_result = {}

    def disconnect_watch():
        th_result["closed"] = expect_disconnect(ws_uri)

    watcher = threading.Thread(target=disconnect_watch, daemon=True)
    watcher.start()
    time.sleep(0.6)  # 等订阅建立
    r = src.delete("/scenario")
    watcher.join(8)
    check("清除场景断开旧订阅", th_result.get("closed") is True)
    check("清除返回 had_scenario", r.json()["had_scenario"] is True)

    print("\n== 6. 清除后恢复随机行情与故障注入 ==")
    check("模式恢复 random", src.get("/status").json()["mode"] == "random")
    check("随机序号从 0 重新开始", src.get("/status").json()["seq"] == 0)
    check("随机流恢复（序号开始增长）",
          wait_until(src, col, lambda: src.get("/status").json()["seq"] > 0))
    r = src.post("/control/gap", params={"count": 2})
    check("故障注入重新可用", r.status_code == 200 and r.json()["pending_gap"] == 2)
    check("采集器在随机流上恢复同步",
          wait_until(src, col, lambda: col.get("/status").json().get("synced") is True))

    print("\n== 7. 切换前发起的延迟快照：场景已变化错误，读不到新场景 ==")
    src.post("/control/snapshot_delay", params={"seconds": 2.0})
    captured: dict = {}

    def slow_snapshot():
        captured["resp"] = src.get("/snapshot")

    worker = threading.Thread(target=slow_snapshot, daemon=True)
    worker.start()
    time.sleep(0.5)  # 快照在途
    load_resp = src.post("/scenario/load", json=SCENARIO)
    check("在途快照期间可切换场景", load_resp.status_code == 200)
    fresh = src.get("/snapshot").json()
    worker.join(6)
    late = captured["resp"]
    check("迟到快照返回 409", late.status_code == 409, f"got {late.status_code}")
    body = late.json()
    check("错误类型 scenario_changed", body.get("error") == "scenario_changed")
    check("迟到响应不含新场景盘口", "bids" not in body and "asks" not in body)
    check("新场景快照本身正常", fresh["seq"] == 100)
    src.post("/control/snapshot_delay", params={"seconds": 0})
    src.delete("/scenario")

    print("\n== 8. /docs 与 OpenAPI 暴露场景接口 ==")
    check("/docs 可访问", src.get("/docs").status_code == 200)
    paths = src.get("/openapi.json").json()["paths"]
    for p in ("/scenario/load", "/scenario/step", "/scenario"):
        check(f"OpenAPI 包含 {p}", p in paths)

    print()
    if FAILURES:
        print(f"E2E FAILED: {len(FAILURES)} 项未过 -> {FAILURES}")
        return 1
    print("E2E OK: 场景回放全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
