"""迟滞状态机（algo.md §7.6）：confirm / refresh / clear / 重启归零。"""

from __future__ import annotations

import pytest
from algo.fdd.base import RuleOutcome
from algo.fdd.hysteresis import ClearedEmission, HitEmission, Hysteresis

from tests.helpers import dt, thr

OUT = RuleOutcome(severity="warning", title="t", evidence={})


def test_confirm_requires_n_windows() -> None:
    h = Hysteresis()
    t = thr(confirm_windows=3, clear_windows=2)
    assert h.update("eq", "r", OUT, t, dt(0)) is None
    assert h.update("eq", "r", OUT, t, dt(5)) is None
    e = h.update("eq", "r", OUT, t, dt(10))
    assert isinstance(e, HitEmission)
    assert e.first_detected_at == dt(10)
    assert e.last_detected_at == dt(10)


def test_refresh_hit_keeps_first_detected() -> None:
    h = Hysteresis()
    t = thr(confirm_windows=1, clear_windows=2)
    h.update("eq", "r", OUT, t, dt(0))
    e2 = h.update("eq", "r", OUT, t, dt(5))  # 持续命中 = 刷新
    assert isinstance(e2, HitEmission)
    assert e2.first_detected_at == dt(0)  # 判定时间轴不变
    assert e2.last_detected_at == dt(5)


def test_clear_requires_n_windows_asymmetric() -> None:
    h = Hysteresis()
    t = thr(confirm_windows=1, clear_windows=3)
    h.update("eq", "r", OUT, t, dt(0))
    assert h.update("eq", "r", None, t, dt(5)) is None  # 蓄 1
    assert h.update("eq", "r", None, t, dt(10)) is None  # 蓄 2
    e = h.update("eq", "r", None, t, dt(15))  # 满 3 → cleared
    assert isinstance(e, ClearedEmission)
    assert e.cleared_at == dt(15)
    # cleared 后再未命中：无新动作
    assert h.update("eq", "r", None, t, dt(20)) is None
    # cleared 后再命中：重新走 confirm
    assert h.update("eq", "r", OUT, t, dt(20)) is not None


def test_hit_streak_resets_on_miss_before_confirm() -> None:
    h = Hysteresis()
    t = thr(confirm_windows=3, clear_windows=1)
    h.update("eq", "r", OUT, t, dt(0))
    h.update("eq", "r", OUT, t, dt(5))
    h.update("eq", "r", None, t, dt(10))  # 打断
    assert h.update("eq", "r", OUT, t, dt(15)) is None  # 重新从 1 计
    assert h.update("eq", "r", OUT, t, dt(20)) is None
    assert isinstance(h.update("eq", "r", OUT, t, dt(25)), HitEmission)


def test_restart_resets_state() -> None:
    """§1.4 无状态：重启清零（新实例 = confirm 重新计数、活跃态遗忘）。"""
    h = Hysteresis()
    t = thr(confirm_windows=1, clear_windows=1)
    h.update("eq", "r", OUT, t, dt(0))
    assert h.is_active("eq", "r")
    h2 = Hysteresis()  # 「重启」
    assert not h2.is_active("eq", "r")
    e = h2.update("eq", "r", OUT, t, dt(5))
    assert isinstance(e, HitEmission)  # confirm 侧最多延迟一个确认窗
    assert e.first_detected_at == dt(5)  # 首见时刻 = 新实例的现在


def test_thresholdset_shape() -> None:
    t = thr(params={"x": 1.5})
    assert t.p("x", 0.0) == 1.5
    assert t.p("missing", 2.5) == 2.5
    from dataclasses import FrozenInstanceError

    with pytest.raises(FrozenInstanceError):
        t.severity = "critical"  # type: ignore[misc]  # frozen：属性不可改
