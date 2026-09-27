"""注册表二维路由 + 窗口质量门控（algo.md §7.2 / §5.2）。"""

from __future__ import annotations

import pytest
from algo.fdd.registry import RuleRegistry
from algo.fdd.rules import RULES
from algo.fdd.rules.chiller import ChillerDeltaTLow
from algo.tsdb.windows import gate_bucket, gated_series

from tests.helpers import bucket, eq_view, pt_view


def test_registry_routing_two_dimensions() -> None:
    reg = RuleRegistry(RULES)
    chiller = eq_view("eq-ch", "chiller")
    ch_points = [
        pt_view(1, "chw_supply_temp"),
        pt_view(2, "chw_return_temp"),
        pt_view(3, "power"),
        pt_view(4, "run_status"),
    ]
    bindings = reg.rules_for(chiller, ch_points)
    ids = {b.rule.rule_id for b in bindings}
    assert "chiller.delta_t_low" in ids  # 量类型覆盖满足
    # 第二维不满足 = 静默跳过（非错误）
    partial = eq_view("eq-ch2", "chiller")
    assert "chiller.delta_t_low" not in {
        b.rule.rule_id
        for b in reg.rules_for(partial, ch_points[:3])  # 缺 run_status
    }
    # 第一维不满足
    tower = eq_view("eq-ct", "cooling_tower")
    assert not any(b.rule.rule_id.startswith("chiller.") for b in reg.rules_for(tower, ch_points))


def test_registry_unknown_equipment_type_skipped() -> None:
    reg = RuleRegistry(RULES)
    assert reg.rules_for(eq_view("eq-x", "unknown_type"), []) == []


def test_registry_duplicate_quantity_takes_first() -> None:
    reg = RuleRegistry(RULES)
    chiller = eq_view("eq-ch", "chiller")
    pts = [
        pt_view(1, "chw_supply_temp"),
        pt_view(9, "chw_supply_temp"),  # 同量多点：取首个（§7.1 注）
        pt_view(2, "chw_return_temp"),
        pt_view(3, "power"),
        pt_view(4, "run_status"),
    ]
    binding = next(
        b for b in reg.rules_for(chiller, pts) if b.rule.rule_id == "chiller.delta_t_low"
    )
    supply = {p for p in binding.points if p.quantity_type == "chw_supply_temp"}
    assert supply == {pts[0]}


def test_registry_rejects_duplicate_rule_id() -> None:
    with pytest.raises(ValueError, match="重复 rule_id"):
        RuleRegistry((ChillerDeltaTLow(), ChillerDeltaTLow()))


def test_registry_rejects_bad_naming() -> None:
    class Bad(ChillerDeltaTLow):
        rule_id = "wrong.prefix"

    with pytest.raises(ValueError, match="命名违约"):
        RuleRegistry((Bad(),))


def test_gate_bucket_good_ratio() -> None:
    good = bucket(1, 0, 20.0, sample_count=10, bad_count=1)  # 0.9 ≥ 0.8
    assert gate_bucket(good, 0.8)
    bad = bucket(1, 0, 20.0, sample_count=10, bad_count=3)  # 0.7 < 0.8
    assert not gate_bucket(bad, 0.8)
    assert gate_bucket(bad, 0.6)  # 阈值放宽后放行（YAML 可按规则覆盖）


def test_gate_bucket_bit4_unit_unconverted() -> None:
    """bit4 置位 = 单位未归一，桶不可比——一律剔除（§5.2）。"""
    b = bucket(1, 0, 20.0, sample_count=10, bad_count=0, quality_mask=1 << 4)
    assert not gate_bucket(b, 0.0)  # 即便好样本占比 1.0


def test_gate_bucket_zero_samples() -> None:
    assert not gate_bucket(bucket(1, 0, None, sample_count=0), 0.8)


def test_gated_series_filters() -> None:
    rows = [
        bucket(1, 0, 20.0),
        bucket(1, 5, 20.0, sample_count=10, bad_count=5),
        bucket(1, 10, 20.0, quality_mask=1 << 4),
    ]
    out = gated_series(rows, 0.8)
    assert [b.bucket.minute for b in out] == [0]
