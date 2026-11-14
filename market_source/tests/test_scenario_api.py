"""场景 API 端到端集成测试（FastAPI TestClient：HTTP + WS 真实帧）。"""
from __future__ import annotations

import threading
import time

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.main import app
from app.state import MarketSource, source

BIDS = [[69990, 5], [69980, 3]]
ASKS = [[70010, 7], [70020, 2]]
INCREMENTS = [
    [{"side": "bid", "price": 69970, "qty": 4}],
    [{"side": "ask", "price": 70010, "qty": 0}, {"side": "bid", "price": 69990, "qty": 9}],
    [{"side": "ask", "price": 70030, "qty": 6}],
]


def body(**ov) -> dict:
    base = {
        "symbol": "CU2601", "bids": BIDS, "asks": ASKS,
        "increments": INCREMENTS, "name": "replay-1", "base_seq": 100,
    }
    base.update(ov)
    return base


@pytest.fixture
def client():
    # 每个用例从全新随机行情开始（清除可能残留的场景/延迟注入）
    source.clear_scenario()
    source.snapshot_delay = 0.0
    source.pending_gap = 0
    source.paused_until = 0.0
    with TestClient(app) as c:
        yield c
    source.clear_scenario()
    source.snapshot_delay = 0.0


# ---------------- 加载 / 状态 / 确定性 ----------------

def test_load_returns_snapshot_and_status(client: TestClient):
    r = client.post("/scenario/load", json=body())
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["scenario"]["active"] is True
    assert data["scenario"]["cursor"] == 0
    assert data["scenario"]["total"] == 3
    assert data["snapshot"]["seq"] == 100
    assert data["snapshot"]["bids"] == [[69990, 5], [69980, 3]]

    r = client.get("/scenario")
    assert r.json()["active"] is True
    assert r.json()["base_seq"] == 100

    r = client.get("/status")
    st = r.json()
    assert st["mode"] == "scenario"
    assert st["scenario"]["cursor"] == 0
    assert st["seq"] == 100


def test_repeated_load_same_scenario_is_deterministic(client: TestClient):
    """重复加载同一场景：每个步进位置的序号与盘口必须完全一致。"""
    runs = []
    for _ in range(2):
        client.post("/scenario/load", json=body())
        timeline = [client.get("/snapshot").json()]
        for _ in range(3):
            client.post("/scenario/step")
            timeline.append(client.get("/snapshot").json())
        runs.append(timeline)
    assert runs[0] == runs[1]
    assert [s["seq"] for s in runs[0]] == [100, 101, 102, 103]
    # 最终盘口：新增/改量/删档都已生效
    final = runs[0][-1]
    assert final["asks"] == [[70020, 2], [70030, 6]]   # 70010 被删，70030 新增
    assert [p for p, _ in final["bids"]] == [69990, 69980, 69970]
    assert dict(final["bids"])[69990] == 9              # 改量


def test_step_endpoint_publishes_one_frame_and_advances_cursor(client: TestClient):
    client.post("/scenario/load", json=body())
    with client.websocket_connect("/ws") as ws:
        reader = _Reader(ws)
        reader.start()
        try:
            r = client.post("/scenario/step")
            assert r.status_code == 200
            frame = r.json()["published"]
            assert frame["seq"] == 101 and frame["type"] == "increment"
            assert frame["changes"] == [{"side": "bid", "price": 69970, "qty": 4}]

            # WS 上确实收到这一条，且就是这一条
            received = reader.wait_frames(timeout=0.6)
            incs = [f for f in received if f.get("type") == "increment"]
            assert incs == [frame]

            # 不再步进就没有新的增量帧（只有心跳）
            time.sleep(1.2)
            again = reader.snapshot()
            assert all(f.get("type") == "heartbeat" for f in again[len(received):])

            # 多档变更在同一条增量里原子发布
            r = client.post("/scenario/step")
            frame2 = r.json()["published"]
            assert frame2["seq"] == 102 and len(frame2["changes"]) == 2
            assert reader.wait_for_increment(seq=102, timeout=1.0) == frame2
        finally:
            reader.stop()


def test_step_beyond_end_returns_409(client: TestClient):
    client.post("/scenario/load", json=body())
    for _ in range(3):
        assert client.post("/scenario/step").status_code == 200
    r = client.post("/scenario/step")
    assert r.status_code == 409
    assert r.json()["error"] == "scenario_finished"


def test_step_without_scenario_returns_409(client: TestClient):
    r = client.post("/scenario/step")
    assert r.status_code == 409
    assert r.json()["error"] == "no_scenario"


def test_reload_restarts_from_beginning(client: TestClient):
    client.post("/scenario/load", json=body())
    client.post("/scenario/step")
    client.post("/scenario/step")
    assert client.get("/scenario").json()["cursor"] == 2
    # 重新加载同一场景 -> 回到初始盘口、cursor=0、序号重来
    client.post("/scenario/load", json=body())
    view = client.get("/scenario").json()
    assert view["cursor"] == 0 and view["seq"] == 100
    snap = client.get("/snapshot").json()
    assert snap["bids"] == [[69990, 5], [69980, 3]]   # 改量/新增都已复位


# ---------------- 非法场景不得改变当前流 ----------------

@pytest.mark.parametrize(
    "bad_body, err_field",
    [
        (body(symbol="XX2601"), "invalid_scenario"),
        (body(bids=[]), "invalid_scenario"),
        (body(bids=[[69990, 0]]), "invalid_scenario"),
        (body(bids=[[69990, 1], [69990, 2]]), "invalid_scenario"),
        (body(increments=[[{"side": "bid", "price": 1, "qty": -1}]]), "invalid_scenario"),
        (body(bids=[[70000 - 10 * i, 1] for i in range(1, 22)]), "invalid_scenario"),
        (body(increments=[[{"side": "mid", "price": 1, "qty": 1}]]), "detail"),  # pydantic 422
    ],
)
def test_invalid_scenario_does_not_change_stream(client: TestClient, bad_body, err_field):
    # 先进入一个已知场景，记录其盘口与世代
    client.post("/scenario/load", json=body())
    client.post("/scenario/step")
    before = client.get("/snapshot").json()
    epoch_before = client.get("/status").json()["epoch"]

    r = client.post("/scenario/load", json=bad_body)
    assert r.status_code in (400, 422)
    assert err_field in r.text

    after = client.get("/snapshot").json()
    st = client.get("/status").json()
    assert after == before                       # 盘口/序号原样
    assert st["epoch"] == epoch_before           # 世代未推进，流未被切换
    assert st["mode"] == "scenario"
    # 当前场景仍可继续步进
    assert client.post("/scenario/step").status_code == 200


def test_structurally_invalid_body_returns_422(client: TestClient):
    r = client.post("/scenario/load", json={"symbol": "CU2601"})  # 缺 bids/asks
    assert r.status_code == 422
    r = client.post("/scenario/load", json=body(bids=[[True, 1]]))
    assert r.status_code == 422                  # bool 不是合法整数


# ---------------- 加载/清除断开旧订阅 ----------------

def test_load_disconnects_existing_subscribers(client: TestClient):
    with client.websocket_connect("/ws") as ws:
        assert client.get("/status").json()["clients"] == 1
        r = client.post("/scenario/load", json=body())
        assert r.json()["disconnected"] == 1
        # 旧连接已被服务端关闭，下一次接收抛 WebSocketDisconnect
        with pytest.raises(WebSocketDisconnect):
            for _ in range(20):
                ws.receive_json()
                time.sleep(0.05)
    # 新连接可以正常建立，并收到步进帧
    with client.websocket_connect("/ws") as ws2:
        reader = _Reader(ws2)
        reader.start()
        try:
            client.post("/scenario/step")
            frame = reader.wait_for_increment(seq=101, timeout=1.0)
            assert frame is not None and frame["seq"] == 101
        finally:
            reader.stop()


def test_clear_disconnects_subscribers(client: TestClient):
    client.post("/scenario/load", json=body())
    with client.websocket_connect("/ws") as ws:
        r = client.delete("/scenario")
        assert r.status_code == 200
        assert r.json()["had_scenario"] is True
        assert r.json()["disconnected"] == 1
        with pytest.raises(WebSocketDisconnect):
            for _ in range(20):
                ws.receive_json()
                time.sleep(0.05)
    assert client.get("/status").json()["mode"] == "random"


# ---------------- 迟到快照：场景已变化错误，读不到新场景 ----------------

def test_late_snapshot_after_load_returns_409_and_no_new_data(client: TestClient):
    client.post("/control/snapshot_delay", params={"seconds": 1.5})
    captured = {}
    def worker():
        captured["resp"] = client.get("/snapshot")

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.4)  # 确保快照请求已在途（睡眠在延迟窗口内）
    client.post("/scenario/load", json=body())
    new_snapshot = client.get("/snapshot").json()
    t.join(timeout=5)

    late = captured["resp"]
    assert late.status_code == 409
    payload = late.json()
    assert payload["error"] == "scenario_changed"
    assert payload["current_epoch"] > payload["request_epoch"]
    # 迟到响应里绝不携带新场景盘口（只有错误元信息）
    assert "bids" not in payload and "asks" not in payload
    assert new_snapshot["seq"] == 100  # 新场景快照本身正常


def test_late_snapshot_after_switch_and_clear(client: TestClient):
    client.post("/control/snapshot_delay", params={"seconds": 1.2})
    client.post("/scenario/load", json=body())  # 场景 A
    captured = {}
    def worker():
        captured["resp"] = client.get("/snapshot")

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.3)
    # A -> B 切换，再清除回随机：在途快照的世代连续两次落后
    client.post("/scenario/load", json=body(name="replay-2", base_seq=500))
    client.delete("/scenario")
    t.join(timeout=5)
    late = captured["resp"]
    assert late.status_code == 409
    assert late.json()["error"] == "scenario_changed"
    assert client.get("/status").json()["mode"] == "random"


def test_snapshot_after_epoch_settles_returns_200(client: TestClient):
    client.post("/scenario/load", json=body())
    # 无延迟、世代稳定时快照正常（场景与随机两种模式都验一下）
    assert client.get("/snapshot").json()["seq"] == 100
    client.delete("/scenario")
    assert client.get("/snapshot").status_code == 200


# ---------------- 清除后恢复随机行情与故障注入 ----------------

def test_clear_restores_random_stream_and_injection(client: TestClient):
    # 故障注入配置在进入场景前设置，清除后应保留并继续生效
    client.post("/control/snapshot_delay", params={"seconds": 0})
    client.post("/scenario/load", json=body())
    # 场景模式下随机增量静默
    time.sleep(1.2)
    seq_while_scenario = client.get("/status").json()["seq"]
    assert seq_while_scenario == 100

    client.delete("/scenario")
    st0 = client.get("/status").json()
    assert st0["mode"] == "random" and st0["seq"] == 0

    # 随机流恢复：序号开始自增
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if client.get("/status").json()["seq"] > 0:
            break
        time.sleep(0.2)
    assert client.get("/status").json()["seq"] > 0

    # 故障注入仍可用：gap 注入生效（下一条随机增量跳号）
    r = client.post("/control/gap", params={"count": 2})
    assert r.status_code == 200 and r.json()["pending_gap"] == 2


def test_fault_injection_blocked_while_scenario_active(client: TestClient):
    client.post("/scenario/load", json=body())
    for path, params in [
        ("/control/gap", {"count": 1}),
        ("/control/snapshot_delay", {"seconds": 3}),
        ("/control/duplicate", {}),
        ("/control/pause", {"seconds": 1}),
    ]:
        r = client.post(path, params=params)
        assert r.status_code == 409, (path, r.status_code, r.text)
        assert r.json()["error"] == "scenario_active"
    # disconnect 属于连接管理，场景下仍允许
    assert client.post("/control/disconnect").status_code == 200


def test_docs_and_openapi_include_scenario(client: TestClient):
    spec = client.get("/openapi.json").json()
    for path in ["/scenario/load", "/scenario/step", "/scenario"]:
        assert path in spec["paths"]
    assert client.get("/docs").status_code == 200


# ---------------- 辅助 ----------------

class _Reader:
    """单个后台线程持续读 WS 帧并入队，避免多个读取线程互相抢帧。"""

    def __init__(self, ws) -> None:
        self.ws = ws
        self.frames: list[dict] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "_Reader":
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.frames.append(self.ws.receive_json())
            except Exception:
                return  # 连接关闭：线程自然退出

    def wait_frames(self, timeout: float) -> list[dict]:
        time.sleep(timeout)
        return list(self.frames)

    def wait_for_increment(self, seq: int, timeout: float) -> dict | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for f in self.frames:
                if f.get("type") == "increment" and f.get("seq") == seq:
                    return f
            time.sleep(0.02)
        return None

    def snapshot(self) -> list[dict]:
        return list(self.frames)

    def stop(self) -> None:
        self._stop.set()
        try:
            self.ws.close()
        except Exception:
            pass
