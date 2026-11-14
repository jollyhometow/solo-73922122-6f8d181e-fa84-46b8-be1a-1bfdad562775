"""MarketSource 场景状态机单测：确定性回放、序号分配、清除恢复、epoch 推进。"""
from __future__ import annotations

from app.models import ScenarioLoad, validate_scenario
from app.state import MarketSource

BIDS = [[69990, 5], [69980, 3]]
ASKS = [[70010, 7], [70020, 2]]
INCREMENTS = [
    [{"side": "bid", "price": 69970, "qty": 4}],                       # seq base+1 新增
    [{"side": "ask", "price": 70010, "qty": 0},                        # seq base+2 删档
     {"side": "bid", "price": 69990, "qty": 9}],                       #             改量
]


def make_payload(base_seq: int = 0) -> ScenarioLoad:
    return ScenarioLoad(
        symbol="CU2601", bids=BIDS, asks=ASKS,
        increments=INCREMENTS, name="t1", base_seq=base_seq,
    )


def load(source: MarketSource, base_seq: int = 0) -> ScenarioLoad:
    payload = make_payload(base_seq)
    validate_scenario(payload)
    bids = {p: q for p, q in BIDS}
    asks = {p: q for p, q in ASKS}
    source.load_scenario(payload, bids, asks)
    return payload


def test_load_installs_initial_book_and_base_seq():
    s = MarketSource()
    s.seq = 42
    load(s, base_seq=100)
    assert s.scenario_active
    assert s.seq == 100
    assert s.bids == {69990: 5, 69980: 3}
    assert s.asks == {70010: 7, 70020: 2}
    view = s.scenario_view()
    assert view["cursor"] == 0 and view["total"] == 2 and view["base_seq"] == 100
    assert view["finished"] is False


def test_step_seq_assignment_and_book_changes():
    s = MarketSource()
    load(s, base_seq=100)

    f1 = s.step_scenario()
    assert f1["seq"] == 101 and f1["type"] == "increment"
    assert f1["changes"] == [{"side": "bid", "price": 69970, "qty": 4}]
    assert s.seq == 101 and s.bids == {69990: 5, 69980: 3, 69970: 4}

    f2 = s.step_scenario()
    assert f2["seq"] == 102
    assert s.seq == 102
    assert s.asks == {70020: 2}                 # 70010 已删
    assert s.bids[69990] == 9                   # 改量生效
    assert s.scenario_view()["finished"] is True
    assert s.step_scenario() is None            # 播完不再产出


def test_repeated_load_is_deterministic():
    """同一场景重复加载，任意步进位置的序号与盘口必须完全一致。"""
    runs: list[list[dict]] = []
    for _ in range(3):
        s = MarketSource()
        load(s, base_seq=50)
        timeline = [s.snapshot()]
        for _ in range(2):
            s.step_scenario()
            timeline.append(s.snapshot())
        runs.append(timeline)
    assert runs[0] == runs[1] == runs[2]
    # 关键序号确定性
    assert [sn["seq"] for sn in runs[0]] == [50, 51, 52]


def test_clear_restores_random_market():
    s = MarketSource()
    seeded = dict(s.bids), dict(s.asks)
    s.make_increment()  # 随机流跑过若干条
    load(s)
    s.step_scenario()
    assert s.scenario_active

    existed = s.clear_scenario()
    assert existed is True
    assert s.scenario is None and s.scenario_active is False
    assert s.seq == 0
    assert (dict(s.bids), dict(s.asks)) == seeded  # 恢复初始随机盘口
    assert s.pending_gap == 0 and s.last_increment is None


def test_epoch_advances_on_load_and_clear():
    s = MarketSource()
    epoch0 = s.epoch
    load(s)
    assert s.epoch == epoch0 + 1
    s.step_scenario()
    load(s)  # 重复加载（切换/回放）也推进世代
    assert s.epoch == epoch0 + 2
    s.clear_scenario()
    assert s.epoch == epoch0 + 3
    # 无场景时再 clear 仍推进（使清除前发起的迟到快照失效）
    s.clear_scenario()
    assert s.epoch == epoch0 + 4


def test_load_resets_random_injection_state():
    s = MarketSource()
    s.pending_gap = 7
    s.seq = 99
    load(s, base_seq=10)
    assert s.pending_gap == 0
    assert s.seq == 10          # 不继承随机流序号
    assert s.last_increment is None


def test_snapshot_view_sorted():
    s = MarketSource()
    load(s)
    snap = s.snapshot()
    assert snap["type"] == "snapshot" and snap["symbol"] == "CU2601"
    bid_prices = [p for p, _ in snap["bids"]]
    ask_prices = [p for p, _ in snap["asks"]]
    assert bid_prices == sorted(bid_prices, reverse=True)
    assert ask_prices == sorted(ask_prices)
