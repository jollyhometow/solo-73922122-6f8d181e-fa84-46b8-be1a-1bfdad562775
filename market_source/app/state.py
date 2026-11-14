"""行情源运行状态：随机行情 / 场景步进两种模式，及故障注入开关。

- 随机模式（默认）：tick_loop 按 tick_interval 生成并广播随机增量；
- 场景模式：加载场景后随机增量暂停，只有 POST /scenario/step 才逐条播放；
- load/clear/switch 都会推进 epoch（场景世代）并断开全部 WS，使在途的旧快照失效；
- 清除场景后状态完全回到随机行情，并恢复此前的故障注入配置。
"""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass

from fastapi import WebSocket

from .models import (
    MAX_LEVELS,
    MID_PRICE,
    SYMBOL,
    TICK,
    Book,
    ScenarioChange,
    ScenarioLoad,
)

log = logging.getLogger("market-source")


@dataclass
class ScenarioState:
    """一个已加载场景的播放进度。"""

    name: str | None
    base_seq: int
    initial_bids: Book
    initial_asks: Book
    increments: list[list[ScenarioChange]]
    cursor: int = 0  # 已播放的增量条数（0..len(increments)）

    @property
    def total(self) -> int:
        return len(self.increments)

    @property
    def finished(self) -> bool:
        return self.cursor >= self.total


class MarketSource:
    def __init__(self) -> None:
        self.symbol = SYMBOL
        self.seq = 0
        self.mid = MID_PRICE
        self.bids: Book = {}
        self.asks: Book = {}
        self.clients: set[WebSocket] = set()
        # 故障注入配置
        self.snapshot_delay = 0.0      # 快照响应延迟（秒），控制面可调
        self.pending_gap = 0           # 下一条随机增量要跳过的序号个数
        self.tick_interval = 0.5       # 随机增量推送间隔（秒）
        self.paused_until = 0.0        # 随机增量暂停推送直到该 monotonic 时刻
        self.last_increment: dict | None = None
        # 场景状态
        self.scenario: ScenarioState | None = None
        # 场景世代：每次成功 load/clear +1；快照响应时核对，世代变了即拒答
        self.epoch = 0
        self._seed()

    # ---------- 随机行情初始化 ----------

    def _seed(self) -> None:
        self.bids = {self.mid - TICK * i: 20 + 3 * i for i in range(1, 11)}
        self.asks = {self.mid + TICK * i: 15 + 4 * i for i in range(1, 11)}

    @property
    def scenario_active(self) -> bool:
        return self.scenario is not None

    def snapshot(self) -> dict:
        return {
            "type": "snapshot",
            "symbol": self.symbol,
            "seq": self.seq,
            "bids": [[p, q] for p, q in sorted(self.bids.items(), reverse=True)],
            "asks": [[p, q] for p, q in sorted(self.asks.items())],
        }

    # ---------- 随机增量（仅随机模式由 tick_loop 调用） ----------

    def make_increment(self) -> dict:
        # 丢序注入：序号一次性多跳 pending_gap 个，被跳过的序号永不发送
        self.seq += 1 + self.pending_gap
        self.pending_gap = 0

        changes = []
        for _ in range(random.randint(1, 3)):
            side = random.choice(("bid", "ask"))
            book = self.bids if side == "bid" else self.asks
            sign = -1 if side == "bid" else 1
            slots = [self.mid + sign * TICK * (i + 1) for i in range(MAX_LEVELS)]
            r = random.random()
            if book and r < 0.20:
                price, qty = random.choice(list(book)), 0      # 删档
            elif len(book) >= MAX_LEVELS:
                price = random.choice(list(book))              # 已满：改量
                qty = random.randint(5, 100)
            else:
                price = random.choice(slots)                   # 新增或改量
                qty = random.randint(5, 100)
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty
            changes.append({"side": side, "price": price, "qty": qty})

        inc = {"type": "increment", "symbol": self.symbol, "seq": self.seq, "changes": changes}
        self.last_increment = inc
        return inc

    # ---------- 场景生命周期 ----------

    def load_scenario(self, payload: ScenarioLoad, bids: Book, asks: Book) -> ScenarioState:
        """安装已通过校验的场景。调用方负责先 validate_scenario 并断开旧 WS。"""
        state = ScenarioState(
            name=payload.name,
            base_seq=payload.base_seq,
            initial_bids=dict(bids),
            initial_asks=dict(asks),
            increments=[list(group) for group in payload.increments],
        )
        self.scenario = state
        # 场景从初始盘口、base_seq 开始；随机流的遗留状态全部作废
        self.bids = dict(state.initial_bids)
        self.asks = dict(state.initial_asks)
        self.seq = state.base_seq
        self.last_increment = None
        self.pending_gap = 0
        self.paused_until = 0.0
        self.epoch += 1
        log.info(
            "scenario loaded: name=%r base_seq=%d increments=%d epoch=%d",
            state.name, state.base_seq, state.total, self.epoch,
        )
        return state

    def clear_scenario(self) -> bool:
        """退出场景模式，恢复随机行情。返回是否原本处于场景模式。"""
        existed = self.scenario is not None
        self.scenario = None
        self.seq = 0
        self.last_increment = None
        self.pending_gap = 0
        self.paused_until = 0.0
        self._seed()
        self.epoch += 1
        if existed:
            log.info("scenario cleared, random market restored, epoch=%d", self.epoch)
        return existed

    def step_scenario(self) -> dict:
        """播放下一条场景增量并返回完整 WS 帧；播完返回 None。"""
        state = self.scenario
        assert state is not None, "step called without an active scenario"
        if state.finished:
            return None
        changes = state.increments[state.cursor]
        state.cursor += 1
        self.seq = state.base_seq + state.cursor  # 第 i 条 -> base_seq + i
        for c in changes:
            book = self.bids if c.side == "bid" else self.asks
            if c.qty == 0:
                book.pop(c.price, None)
            else:
                book[c.price] = c.qty
        frame = {
            "type": "increment",
            "symbol": self.symbol,
            "seq": self.seq,
            "changes": [c.model_dump() for c in changes],
        }
        self.last_increment = frame
        log.info(
            "scenario step %d/%d seq=%d epoch=%d",
            state.cursor, state.total, self.seq, self.epoch,
        )
        return frame

    def scenario_view(self) -> dict | None:
        state = self.scenario
        if state is None:
            return None
        return {
            "active": True,
            "name": state.name,
            "epoch": self.epoch,
            "base_seq": state.base_seq,
            "seq": self.seq,
            "cursor": state.cursor,
            "total": state.total,
            "finished": state.finished,
        }


async def broadcast(msg: dict) -> None:
    """向全部 WS 客户端发一帧；发送失败的连接自动摘除。"""
    if not source.clients:
        return
    data = json.dumps(msg)
    dead = []
    for ws in list(source.clients):
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        source.clients.discard(ws)


async def disconnect_all() -> int:
    """关闭全部 WS（加载/清除场景、断线注入复用）。返回关闭的连接数。"""
    targets = list(source.clients)
    for ws in targets:
        try:
            await ws.close(code=1012)  # 1012 = Service Restart
        except Exception:
            pass
        source.clients.discard(ws)
    return len(targets)


# 单例运行状态（测试通过 dependency override / 直接操作该对象）
source = MarketSource()
