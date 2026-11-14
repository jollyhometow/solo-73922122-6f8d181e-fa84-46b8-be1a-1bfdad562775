"""本地铜期货行情源（含故障注入与可回放场景，仅供开发/测试）。

- GET    /snapshot        全量快照：序号 + 完整买卖盘（支持 ?delay= 或控制面延迟）
- WS     /ws              增量流：连续序号、多档变更、qty=0 删档；另有周期心跳
- GET    /status          运行状态（含场景播放进度、场景世代 epoch）
- POST   /control/*       故障注入（场景模式下拒绝，避免污染确定性回放）
- POST   /scenario/load   加载场景：初始双侧盘口 + 有序多档增量（校验不过不动当前流）
- POST   /scenario/step   手动步进：每次只向 /ws 发布一条增量
- GET    /scenario        当前步进状态
- DELETE /scenario        清除场景：断开旧订阅并恢复随机行情与故障注入

场景切换（load/clear）会推进 epoch 并断开全部 WS；切换前发起、切换后才返回的
快照一律得到 409 scenario_changed，绝不会读到新场景的盘口。
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from .models import ScenarioError, ScenarioLoad, parse_levels, validate_scenario
from .state import SYMBOL, broadcast, disconnect_all, source

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("market-source")


async def tick_loop() -> None:
    while True:
        await asyncio.sleep(source.tick_interval)
        # 场景模式下随机增量完全静默，只允许人工 step 推送
        if source.scenario_active or time.monotonic() < source.paused_until:
            continue
        await broadcast(source.make_increment())


async def heartbeat_loop() -> None:
    while True:
        await asyncio.sleep(1.0)
        await broadcast({"type": "heartbeat", "ts": time.time()})


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(tick_loop()), asyncio.create_task(heartbeat_loop())]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(
    title="Local Copper Market Source",
    version="1.1.0",
    description=(
        "本地行情源：快照 + 连续增量，支持延迟快照/丢序/断线等故障注入，"
        "并提供可手动步进、确定性回放的场景 API（/scenario/*）。"
    ),
    lifespan=lifespan,
)


def _scenario_conflict(operation: str) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "error": "scenario_active",
            "detail": f"场景模式下故障注入已停用：{operation}；清除场景（DELETE /scenario）后再试",
        },
    )


@app.get("/", summary="导航")
async def index() -> dict:
    return {
        "service": app.title,
        "symbol": SYMBOL,
        "links": {
            "snapshot": "/snapshot",
            "ws": "/ws",
            "status": "/status",
            "scenario": "/scenario",
            "docs": "/docs",
        },
    }


@app.get(
    "/snapshot",
    summary="全量快照（序号 + 完整买卖盘）",
    responses={409: {"description": "请求在途期间场景发生切换，快照世代已过期"}},
)
async def get_snapshot(
    symbol: str = SYMBOL,
    delay: float | None = Query(default=None, description="本次响应额外延迟秒数"),
):
    epoch = source.epoch  # 入口世代：延迟期间若 load/clear 推进了世代，本响应作废
    wait = source.snapshot_delay if delay is None else delay
    if wait > 0:
        await asyncio.sleep(wait)
    if source.epoch != epoch:
        log.warning("snapshot rejected: epoch changed %d -> %d", epoch, source.epoch)
        return JSONResponse(
            status_code=409,
            content={
                "error": "scenario_changed",
                "detail": "快照在途期间场景已加载/切换/清除，拒绝读取当前（新）场景",
                "request_epoch": epoch,
                "current_epoch": source.epoch,
            },
        )
    if symbol != source.symbol:
        return JSONResponse({"error": "unknown_symbol", "symbol": symbol}, status_code=404)
    return source.snapshot()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    source.clients.add(ws)
    log.info("client connected, total=%d", len(source.clients))
    try:
        while True:
            await ws.receive_text()  # 忽略客户端消息，仅用于感知断开
    except WebSocketDisconnect:
        pass
    finally:
        source.clients.discard(ws)
        log.info("client disconnected, total=%d", len(source.clients))


@app.get("/status", summary="行情源状态（含场景播放进度）")
async def get_status() -> dict:
    return {
        "symbol": source.symbol,
        "mode": "scenario" if source.scenario_active else "random",
        "epoch": source.epoch,
        "seq": source.seq,
        "levels": {"bids": len(source.bids), "asks": len(source.asks)},
        "clients": len(source.clients),
        "snapshot_delay": source.snapshot_delay,
        "pending_gap": source.pending_gap,
        "tick_interval": source.tick_interval,
        "paused": time.monotonic() < source.paused_until,
        "scenario": source.scenario_view(),
    }


# ---------------- 场景 API ----------------

@app.post(
    "/scenario/load",
    summary="加载场景（初始盘口 + 有序多档增量），断开旧订阅并进入手动步进",
    responses={400: {"description": "场景语义非法：当前流不发生任何改变"}},
    tags=["scenario"],
)
async def load_scenario(payload: ScenarioLoad):
    # 1) 完整校验在临时副本上完成；任何异常都不触碰线上状态（非法场景不改当前流）
    try:
        validate_scenario(payload)
    except ScenarioError as exc:
        log.info("scenario rejected: %s", exc)
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_scenario", "detail": str(exc)},
        )
    bids = parse_levels(payload.bids, "bids")
    asks = parse_levels(payload.asks, "asks")

    # 2) 先断开旧订阅，再原子切换：所有现存 WS 客户端必须重新连接才能读到新流
    disconnected = await disconnect_all()
    source.load_scenario(payload, bids, asks)
    log.info("scenario load closed %d stale ws connection(s)", disconnected)
    return {
        "ok": True,
        "disconnected": disconnected,
        "scenario": source.scenario_view(),
        "snapshot": source.snapshot(),
    }


@app.post(
    "/scenario/step",
    summary="手动步进：播放下一条增量，每次只向 /ws 发布一条",
    responses={409: {"description": "未加载场景，或场景增量已全部播完"}},
    tags=["scenario"],
)
async def step_scenario():
    state = source.scenario
    if state is None:
        return JSONResponse(
            status_code=409,
            content={"error": "no_scenario", "detail": "尚未加载场景，请先 POST /scenario/load"},
        )
    if state.finished:
        return JSONResponse(
            status_code=409,
            content={
                "error": "scenario_finished",
                "detail": f"全部 {state.total} 条增量已播完；重新 load 可从头回放",
                "scenario": source.scenario_view(),
            },
        )
    frame = source.step_scenario()
    await broadcast(frame)  # 每次步进只发布这一条
    return {
        "ok": True,
        "published": frame,
        "scenario": source.scenario_view(),
        "snapshot": source.snapshot(),
    }


@app.get("/scenario", summary="当前场景步进状态（无场景时 active=false）", tags=["scenario"])
async def get_scenario() -> dict:
    view = source.scenario_view()
    if view is None:
        return {"active": False, "epoch": source.epoch}
    return view


@app.delete(
    "/scenario",
    summary="清除场景：断开旧订阅，恢复随机行情与故障注入",
    tags=["scenario"],
)
async def clear_scenario():
    disconnected = await disconnect_all()
    existed = source.clear_scenario()
    return {
        "ok": True,
        "had_scenario": existed,
        "disconnected": disconnected,
        "mode": "random",
        "scenario": source.scenario_view(),
    }


# ---------------- 故障注入控制面（仅随机模式） ----------------

@app.post("/control/gap", summary="制造丢序：下一条随机增量序号跳过 count 个")
async def control_gap(count: int = 1) -> dict:
    if source.scenario_active:
        return _scenario_conflict("gap")
    source.pending_gap = max(0, min(count, 1000))
    return {"ok": True, "pending_gap": source.pending_gap}


@app.post("/control/disconnect", summary="断线：关闭全部 WS 连接")
async def control_disconnect() -> dict:
    clients = await disconnect_all()
    return {"ok": True, "disconnected": clients}


@app.post("/control/snapshot_delay", summary="设置快照响应延迟（秒，0 关闭；清除场景后保留）")
async def control_snapshot_delay(seconds: float = 5.0) -> dict:
    if source.scenario_active:
        return _scenario_conflict("snapshot_delay")
    source.snapshot_delay = max(0.0, seconds)
    return {"ok": True, "snapshot_delay": source.snapshot_delay}


@app.post("/control/duplicate", summary="重放上一条增量（测试恰好应用一次）")
async def control_duplicate() -> dict:
    if source.scenario_active:
        return _scenario_conflict("duplicate")
    if source.last_increment is None:
        return {"ok": False, "reason": "no increment sent yet"}
    await broadcast(source.last_increment)
    return {"ok": True, "resent_seq": source.last_increment["seq"]}


@app.post("/control/pause", summary="暂停推送随机增量 seconds 秒（心跳继续）")
async def control_pause(seconds: float = 3.0) -> dict:
    if source.scenario_active:
        return _scenario_conflict("pause")
    source.paused_until = time.monotonic() + max(0.0, seconds)
    return {"ok": True, "paused_seconds": max(0.0, seconds)}
