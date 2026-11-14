"""场景 API 的请求模型与纯逻辑校验（不依赖 asyncio/FastAPI，便于单测）。

场景（scenario）= 初始双侧盘口 + 一组有序的多档增量，加载后由人工逐步播放，
每次步进只向 /ws 发布一条增量。校验全部通过之前不得触碰线上行情状态。
"""
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt

SYMBOL = "CU2601"
MAX_LEVELS = 20
TICK = 10  # 最小价格变动，铜期货为 10 元/吨
MID_PRICE = 70000

# 严格整数：拒绝 bool / float（True 不是合法价量，1.5 不是整数档位）
Level = tuple[StrictInt, StrictInt]
Side = Literal["bid", "ask"]

# 价位 -> 量 的单侧订单簿
Book = dict[int, int]


class ScenarioError(ValueError):
    """场景语义非法（与 Pydantic 的结构校验区分，统一转 400，且不改变当前流）。"""


class ScenarioChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    side: Side
    price: StrictInt = Field(description="整数价位，必须为正")
    qty: StrictInt = Field(description="整数量，0 表示删档，不允许负数")


class ScenarioLoad(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: Annotated[str, Field(description="合约，须与行情源一致")]
    bids: list[Level] = Field(description="初始买盘 [[价, 量] ...]，量必须为正")
    asks: list[Level] = Field(description="初始卖盘 [[价, 量] ...]，量必须为正")
    increments: list[list[ScenarioChange]] = Field(
        default_factory=list,
        description="有序增量；每个元素是一条增量，内含一档或多档变更（原子可见）",
    )
    name: Annotated[str | None, Field(description="可选场景名，便于识别")] = None
    base_seq: StrictInt = Field(
        default=0, ge=0, description="初始快照序号；第 i 条增量序号为 base_seq + i + 1"
    )


def parse_levels(raw: list[tuple[int, int]], side_name: str) -> Book:
    """把 [[价, 量] ...] 解析成 {价: 量}，校验正价正量且价位不重复。"""
    book: Book = {}
    for entry in raw:
        price, qty = entry
        if price <= 0:
            raise ScenarioError(f"{side_name} 价位必须为正: {price}")
        if qty <= 0:
            raise ScenarioError(f"{side_name} 量必须为正（删档请放进增量 qty=0）: price={price}")
        if price in book:
            raise ScenarioError(f"{side_name} 存在重复价位: {price}")
        book[price] = qty
    return book


def _check_levels(bids: Book, asks: Book) -> None:
    if len(bids) > MAX_LEVELS or len(asks) > MAX_LEVELS:
        raise ScenarioError(
            f"每侧档数不得超过 {MAX_LEVELS}: bids={len(bids)} asks={len(asks)}"
        )


def _apply_changes(bids: Book, asks: Book, changes: list[ScenarioChange], index: int) -> None:
    """在副本语义下校验并落一条增量；任何一档非法都抛错（整条不落簿）。"""
    for c in changes:
        if c.price <= 0:
            raise ScenarioError(f"增量 #{index} 价位必须为正: {c.price}")
        if c.qty < 0:
            raise ScenarioError(f"增量 #{index} 量不能为负: price={c.price}")
    for c in changes:
        book = bids if c.side == "bid" else asks
        if c.qty == 0:
            book.pop(c.price, None)  # 删档；不存在视为无操作
        else:
            book[c.price] = c.qty
    _check_levels(bids, asks)


def validate_scenario(payload: ScenarioLoad) -> None:
    """完整语义校验：合约、初始盘口、逐条干跑全部增量。

    全部在临时副本上进行，抛 ScenarioError 即代表场景非法，调用方不得改变当前流。
    """
    if payload.symbol != SYMBOL:
        raise ScenarioError(
            f"symbol 不匹配: 场景={payload.symbol!r} 行情源={SYMBOL!r}"
        )
    if not payload.bids or not payload.asks:
        raise ScenarioError("初始盘口必须同时包含非空 bids 与 asks")
    bids = parse_levels(payload.bids, "bids")
    asks = parse_levels(payload.asks, "asks")
    _check_levels(bids, asks)

    for index, changes in enumerate(payload.increments, start=1):
        if not changes:
            raise ScenarioError(f"增量 #{index} 不能为空（一条增量至少含一档变更）")
        _apply_changes(bids, asks, changes, index)
