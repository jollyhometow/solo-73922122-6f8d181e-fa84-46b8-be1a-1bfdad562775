"""场景模型与校验的纯逻辑单测（不起服务、不碰网络）。"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models import ScenarioError, ScenarioLoad, parse_levels, validate_scenario

BIDS = [[69990, 5], [69980, 3]]
ASKS = [[70010, 7], [70020, 2]]


def payload(**overrides) -> ScenarioLoad:
    base = {
        "symbol": "CU2601",
        "bids": BIDS,
        "asks": ASKS,
        "increments": [
            [{"side": "bid", "price": 69970, "qty": 4}],
            [{"side": "ask", "price": 70010, "qty": 0}, {"side": "bid", "price": 69990, "qty": 9}],
        ],
    }
    base.update(overrides)
    return ScenarioLoad(**base)


def test_valid_scenario_passes():
    validate_scenario(payload())  # 不抛异常即通过


def test_wrong_symbol_rejected():
    with pytest.raises(ScenarioError, match="symbol"):
        validate_scenario(payload(symbol="XX2601"))


def test_empty_side_rejected():
    with pytest.raises(ScenarioError, match="非空"):
        validate_scenario(payload(bids=[]))
    with pytest.raises(ScenarioError, match="非空"):
        validate_scenario(payload(asks=[]))


def test_duplicate_price_in_initial_book_rejected():
    with pytest.raises(ScenarioError, match="重复价位"):
        validate_scenario(payload(bids=[[69990, 1], [69990, 2]]))


def test_nonpositive_qty_in_initial_book_rejected():
    with pytest.raises(ScenarioError, match="量必须为正"):
        validate_scenario(payload(bids=[[69990, 0]]))
    with pytest.raises(ScenarioError, match="量必须为正"):
        validate_scenario(payload(asks=[[70010, -3]]))


def test_initial_levels_over_max_rejected():
    many = [[70000 - 10 * i, 1] for i in range(1, 22)]  # 21 档
    with pytest.raises(ScenarioError, match="max_levels|档数"):
        validate_scenario(payload(bids=many))


def test_levels_over_max_after_increment_rejected():
    bids = [[70000 - 10 * i, 1] for i in range(1, 21)]  # 已 20 档
    inc = [[{"side": "bid", "price": 69000, "qty": 1}]]  # 新增第 21 档
    with pytest.raises(ScenarioError):
        validate_scenario(payload(bids=bids, increments=inc))


def test_delete_then_refill_keeps_within_limit_ok():
    bids = [[70000 - 10 * i, 1] for i in range(1, 21)]  # 20 档
    inc = [
        [{"side": "bid", "price": 69990, "qty": 0}],   # 删一档 -> 19
        [{"side": "bid", "price": 69000, "qty": 1}],   # 补一档 -> 20
    ]
    validate_scenario(payload(bids=bids, increments=inc))


def test_negative_qty_in_increment_rejected():
    inc = [[{"side": "bid", "price": 69990, "qty": -1}]]
    with pytest.raises(ScenarioError, match="不能为负"):
        validate_scenario(payload(increments=inc))


def test_empty_increment_rejected():
    with pytest.raises(ScenarioError, match="不能为空"):
        validate_scenario(payload(increments=[[]]))


def test_invalid_scenario_does_not_mutate_inputs():
    good = payload()
    validate_scenario(good)
    bad = payload(increments=[[{"side": "bid", "price": 1, "qty": -9}]])
    with pytest.raises(ScenarioError):
        validate_scenario(bad)
    # 原始模型数据保持原样（校验只在临时副本上干跑）
    assert good.increments[0][0].qty == 4
    assert bad.increments[0][0].qty == -9


def test_pydantic_rejects_bad_types():
    # bool 不是合法整数，float 不是整数档位
    with pytest.raises(ValidationError):
        payload(bids=[[True, 1]])
    with pytest.raises(ValidationError):
        payload(bids=[[69990, 1.5]])
    with pytest.raises(ValidationError):
        payload(increments=[[{"side": "mid", "price": 1, "qty": 1}]])
    with pytest.raises(ValidationError):
        ScenarioLoad(**{**payload().model_dump(), "extra_field": 1})


def test_base_seq_must_be_non_negative():
    with pytest.raises(ValidationError):
        payload(base_seq=-1)
    validate_scenario(payload(base_seq=100))


def test_parse_levels_dedup_and_sign():
    assert parse_levels([[10, 2], [20, 3]], "bids") == {10: 2, 20: 3}
    with pytest.raises(ScenarioError):
        parse_levels([[10, 2], [10, 3]], "bids")
