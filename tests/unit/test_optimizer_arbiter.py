"""arbiter 单测：单点单条 / 冷却窗 / 值死区 / 噪声下限 / R3 豁免（optimizer.md §7）。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from algo.optimizer.arbiter import (
    SubmissionMemory,
    arbitrate,
    deadband_for,
    record_submitted,
)
from algo.optimizer.strategies.base import AdvisoryDraft

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def draft(
    point_id: int = 100,
    *,
    strategy_id: str = "chw.temp_reset",
    value: float = 7.5,
    saving: float = 6.0,
    target_quantity: str = "chw_supply_temp_setpoint",
    exempt: bool = False,
) -> AdvisoryDraft:
    return AdvisoryDraft(
        strategy_id=strategy_id,
        algo_path=f"optimizer/{strategy_id.replace('.', '-')}",
        equipment_id="eq-1",
        point_id=point_id,
        target_quantity=target_quantity,
        value=value,
        previous_value=value - 0.5,
        unit="degC",
        rationale="测试候选",
        expected_saving_kw=saving,
        confidence=0.5,
        exempt_min_saving=exempt,
    )


class TestFloor:
    def test_below_floor_dropped(self) -> None:
        r = arbitrate(
            [draft(saving=4.9)],
            memory=SubmissionMemory(),
            now=NOW,
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert r.candidates == [] and r.floored == 1

    def test_r3_exempt_from_floor(self) -> None:
        r = arbitrate(
            [draft(saving=-29.6, exempt=True)],
            memory=SubmissionMemory(),
            now=NOW,
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert len(r.candidates) == 1


class TestPerPointDedup:
    def test_highest_saving_wins_same_point(self) -> None:
        r = arbitrate(
            [
                draft(saving=6.0, strategy_id="chw.temp_reset"),
                draft(saving=12.0, strategy_id="other.strategy"),
            ],
            memory=SubmissionMemory(),
            now=NOW,
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert len(r.candidates) == 1 and r.candidates[0].strategy_id == "other.strategy"
        assert r.deduped == 1

    def test_cross_point_coexist(self) -> None:
        # §7.1：跨点不互斥（R1 抬温 + R2 停机同轮并存）
        r = arbitrate(
            [
                draft(point_id=105, saving=6.0),
                draft(point_id=104, saving=56.0, target_quantity="unit_enable"),
            ],
            memory=SubmissionMemory(),
            now=NOW,
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert len(r.candidates) == 2


class TestCooldownAndDeadband:
    def test_cooldown_blocks_resubmission(self) -> None:
        mem = SubmissionMemory()
        record_submitted(mem, [draft(point_id=105)], now=NOW, ttl_min=45)
        r = arbitrate(
            [draft(point_id=105)],
            memory=mem,
            now=NOW + timedelta(minutes=15),
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert r.candidates == [] and r.throttled == 1

    def test_cooldown_expires(self) -> None:
        mem = SubmissionMemory()
        record_submitted(mem, [draft(value=7.5, point_id=105)], now=NOW, ttl_min=45)
        r = arbitrate(
            [draft(value=8.2, point_id=105)],
            memory=mem,
            now=NOW + timedelta(minutes=46),
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert len(r.candidates) == 1  # 冷却窗已过 + 值差 0.7 ≥ 死区

    def test_deadband_skips_small_delta(self) -> None:
        mem = SubmissionMemory()
        record_submitted(mem, [draft(value=7.5, point_id=105)], now=NOW, ttl_min=45)
        # 过了冷却窗但新值与上次仅差 0.3 < 0.5 死区
        r = arbitrate(
            [draft(value=7.8, point_id=105)],
            memory=mem,
            now=NOW + timedelta(minutes=50),
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert r.candidates == [] and r.throttled == 1

    def test_deadband_zero_for_enable_commands(self) -> None:
        d = draft(target_quantity="unit_enable")
        assert deadband_for(d, 0.5) == 0.0
        assert deadband_for(draft(), 0.5) == 0.5


class TestMemory:
    def test_restart_clears(self) -> None:
        mem = SubmissionMemory()
        record_submitted(mem, [draft()], now=NOW, ttl_min=45)
        mem2 = SubmissionMemory()  # 「重启」= 新实例（进程内存语义）
        r = arbitrate(
            [draft()],
            memory=mem2,
            now=NOW + timedelta(minutes=1),
            defaults_min_saving_kw=5.0,
            deadband_c=0.5,
        )
        assert len(r.candidates) == 1
