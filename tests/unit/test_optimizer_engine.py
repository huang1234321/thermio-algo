"""引擎全链路单测（fake 外部件，CODE-TST-02/03）：装配 → 求值 → 仲裁 → 信封 → 提交。

覆盖 optimizer.md §1 循环与 §3.2 previous_value 三级链、§0 advisory 前置闸、
信封自检（algo.md §11.2 三条件）、persistence 降级（§5.2）与失败隔离（§8.1）。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest
from algo.kafka.consumer import LatestCache
from algo.kafka.topics import TelemetryRow
from algo.optimizer.config import OptimizerConfigStore
from algo.optimizer.engine import OptimizerEngine, WeatherViewReader
from algo.optimizer.forecast_store import ForecastStore
from algo.proposal.envelope import ProposalEnvelope
from algo.proposal.producer import ProposalSubmitter
from algo.semantics.types import AssetSnapshot, EquipmentView

from tests.optimizer_helpers import T0, cold_plant_points, telemetry_row

EQ1, EQ2, TOWER = "eq-ch1", "eq-ch2", "eq-ct1"
SYS = "sys-op"
BUILDING = "b-1"

# 场景 C（低温差 → R1）：供 7 / 回 8.6，双机 400 kW
VALUES = {
    100: 7.0,
    101: 8.6,
    102: 400.0,
    103: None,
    104: 1.0,
    105: 7.0,
    106: 7.0,
    107: 400.0,
    108: None,
    109: 1.0,
    110: 26.0,
    111: 20.0,
    112: 24.0,
}


class FakeTsdb:
    """telemetry_5min/1h 桶 + 原始点查的内存替身。"""

    def __init__(self, *, values: dict[int, float | None] | None = None) -> None:
        self.values = dict(values or VALUES)
        self.latest_calls: list[int] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        point_ids: list[int] = list(args[0])  # type: ignore[arg-type]
        if "telemetry_5min" in sql:
            lo, hi = args[1], args[2]  # type: ignore[misc]
            rows = []
            for pid in point_ids:
                v = self.values.get(pid)
                if v is None and pid not in (103, 108):  # run_status 枚态不入数值桶
                    continue
                b = T0 - timedelta(minutes=60)
                while b < hi:
                    if b >= lo:
                        rows.append(
                            {
                                "point_id": pid,
                                "bucket": b,
                                "avg": v,
                                "min": v,
                                "max": v,
                                "last": v,
                                "stddev": 0.1,
                                "sample_count": 12,
                                "bad_count": 0,
                                "quality_mask": 0,
                            }
                        )
                    b += timedelta(minutes=5)
            return rows
        assert "telemetry_1h" in sql
        return [
            {"point_id": pid, "bucket": T0 - timedelta(hours=1), "avg": self.values.get(pid)}
            for pid in point_ids
            if self.values.get(pid) is not None
        ]

    async def latest_telemetry(self, point_id: int) -> tuple[datetime, float | None] | None:
        self.latest_calls.append(point_id)
        v = self.values.get(point_id)
        return (T0, v) if v is not None else None


class FakeSnapshot:
    def __init__(self, snap: AssetSnapshot) -> None:
        self._snap = snap

    async def ensure_loaded(self) -> AssetSnapshot:
        return self._snap


class FakeSubmitter(ProposalSubmitter):
    """提交面替身：记录信封并全量「201」。"""

    def __init__(self) -> None:  # super 不接线（无 HTTP 面的提交替身）
        self.submitted: list[ProposalEnvelope] = []

    async def submit(  # type: ignore[override]
        self, envelopes: Any, *, now: datetime | None = None
    ) -> list[ProposalEnvelope]:
        self.submitted.extend(envelopes)
        return list(envelopes)


class FakeWeather:
    """15°C / 60% → 湿球 ≈ 10.52。"""

    async def latest_obs(self) -> Any:
        from algo.fdd.base import WeatherObs

        return WeatherObs(station_id="s", obs_ts=T0, temp_c=15.0, rh_pct=60.0)


def build_snapshot(
    *, control_mode: str = "advisory", clamp: tuple[float, float] = (5.0, 9.0)
) -> AssetSnapshot:
    pts = [
        p.model_copy(update={"control_mode": control_mode})
        if p.quantity_type == "chw_supply_temp_setpoint"
        else p
        for p in cold_plant_points(EQ1, EQ2, TOWER)
    ]
    pts = [
        p.model_copy(update={"clamp_min": clamp[0], "clamp_max": clamp[1]})
        if p.point_id == 105
        else p
        for p in pts
    ]
    return AssetSnapshot(
        generated_at=T0,
        equipments=[
            EquipmentView(
                equipment_id=EQ1,
                equipment_type="chiller",
                system_id=SYS,
                building_id=BUILDING,
                tenant_id="t-1",
                local_id="1#冷机",
                rated_params={
                    "rated_cooling_capacity_kw": 4220.0,
                    "rated_input_power_kw": 840.0,
                    "min_unload_ratio": 0.25,
                    "rated_cop": 5.024,
                },
            ),
            EquipmentView(
                equipment_id=EQ2,
                equipment_type="chiller",
                system_id=SYS,
                building_id=BUILDING,
                tenant_id="t-1",
                local_id="2#冷机",
                rated_params={
                    "rated_cooling_capacity_kw": 4220.0,
                    "rated_input_power_kw": 840.0,
                    "min_unload_ratio": 0.25,
                    "rated_cop": 5.024,
                },
            ),
            EquipmentView(
                equipment_id=TOWER,
                equipment_type="cooling_tower",
                system_id=SYS,
                building_id=BUILDING,
                tenant_id="t-1",
                local_id="1#塔",
                rated_params={"rated_fan_power_kw": 30.0},
            ),
        ],
        points=pts,
    )


def build_engine(
    tsdb: FakeTsdb,
    *,
    latest_rows: list[TelemetryRow] | None = None,
    snapshot: AssetSnapshot | None = None,
) -> tuple[OptimizerEngine, FakeSubmitter]:
    latest = LatestCache()
    for row in latest_rows or []:
        latest.offer(row)
    submitter = FakeSubmitter()
    engine = OptimizerEngine(
        tsdb=tsdb,  # type: ignore[arg-type]
        snapshot=FakeSnapshot(snapshot or build_snapshot()),  # type: ignore[arg-type]
        config=OptimizerConfigStore("/nonexistent-optimizer.yaml"),
        submitter=submitter,  # type: ignore[arg-type]
        latest=latest,
        forecast_store=ForecastStore(),
        weather_reader=WeatherViewReader(FakeWeather()),
    )
    return engine, submitter


@pytest.fixture()
def full_latest_rows() -> list[TelemetryRow]:
    return [
        telemetry_row(100, 7.0),
        telemetry_row(101, 8.6),
        telemetry_row(102, 400.0),
        telemetry_row(103, None, text="running"),
        telemetry_row(104, 1.0),
        telemetry_row(105, 7.0),
        telemetry_row(107, 400.0),
        telemetry_row(108, None, text="running"),
        telemetry_row(109, 1.0),
        telemetry_row(110, 26.0),
        telemetry_row(111, 20.0),
        telemetry_row(112, 24.0),
    ]


class TestFullRound:
    async def test_scenario_c_r1_emits_envelope(self, full_latest_rows) -> None:
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.errors == 0 and report.plants == 1
        assert len(report.submitted) == 1
        env = _submitter_submitted(engine)[0]
        # 信封逐字段（algo.md §11.1）
        assert env.algo == "optimizer/chw-temp-reset"
        assert env.algo_version == "0.1.0"  # 裸 semver（无指纹）
        assert env.target.point == "chw_supply_temp_setpoint"
        assert env.target.equipment_id == EQ1
        assert env.action.op == "set" and env.action.value == pytest.approx(7.5)
        assert env.action.unit == "degC"
        assert env.previous_value == pytest.approx(7.0)
        assert env.expected_saving_kw == pytest.approx(8.0)  # 0.02×0.5×800
        assert 0.0 <= env.confidence <= 1.0
        assert env.expires_at == report.evaluation_ts + timedelta(minutes=45)
        assert env.evidence["formula_id"] == "E1_temp_reset"
        assert env.evidence["previous_value_source"] == "point_latest"
        assert env.evidence["baseline"]["mean_p_kw"] == pytest.approx(800.0)
        assert env.evidence["window"]["from"] < env.evidence["window"]["to"]
        # persistence 降级（§5.2 常态路径）
        assert report.forecast_source == "persistence"
        assert env.evidence["forecast"]["source"] in ("persistence", "none")
        # 仅 R1 一条（R2 ratio 0.476 ≥ 0.45；R4 fan 20/30 且净负；R3 无高载）
        assert report.drafts and all(d.strategy_id == "chw.temp_reset" for d in report.drafts)

    async def test_throttle_three_rounds_first_only(self, full_latest_rows) -> None:
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows)
        r1 = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert len(r1.submitted) == 1
        r2 = await engine.run_round(now=T0 + timedelta(minutes=16))
        r3 = await engine.run_round(now=T0 + timedelta(minutes=31))
        assert r2.submitted == [] and r3.submitted == []  # 冷却窗内不重发
        assert len(_submitter_submitted(engine)) == 1
        assert r2.arbitrated and r2.arbitrated.throttled == 1

    async def test_previous_value_tsdb_fallback(self, full_latest_rows) -> None:
        rows = [r for r in full_latest_rows if r.point_id != 105]  # 拔掉 setpoint 的缓存值
        tsdb = FakeTsdb()
        engine, _submitter = build_engine(tsdb, latest_rows=rows)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert 105 in tsdb.latest_calls  # 走到 TSDB 点查
        assert len(report.submitted) == 1
        assert _submitter_submitted(engine)[0].previous_value == pytest.approx(7.0)

    async def test_previous_value_missing_skips_proposal(self, full_latest_rows) -> None:
        rows = [r for r in full_latest_rows if r.point_id != 105]
        tsdb = FakeTsdb(values={**VALUES, 105: None})  # TSDB 也无值
        engine, _submitter = build_engine(tsdb, latest_rows=rows)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.submitted == []  # §3.2：不可得 → 不产 proposal
        assert all(d.strategy_id != "chw.temp_reset" for d in report.drafts)

    async def test_non_advisory_target_blocks_all(self, full_latest_rows) -> None:
        engine, _submitter = build_engine(
            FakeTsdb(),
            latest_rows=full_latest_rows,
            snapshot=build_snapshot(control_mode="supervised"),
        )
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.drafts == [] and report.submitted == []  # §0 advisory-only 铁律

    async def test_supervised_mode_target_selfcheck(self, full_latest_rows) -> None:
        # 单点位 supervised：plant 级 target 非 advisory → 策略不适用（applicability 面）
        snap = build_snapshot()
        pts = []
        for p in snap.points:
            if p.point_id == 106:
                p = p.model_copy(update={"control_mode": "supervised"})
            pts.append(p)
        snap = AssetSnapshot(generated_at=T0, equipments=snap.equipments, points=pts)
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows, snapshot=snap)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        # eq-ch1 的 setpoint（105）仍 advisory → R1 照常（单点粒度前置闸）
        assert len(report.submitted) == 1

    async def test_clamp_domain_selfcheck(self, full_latest_rows) -> None:
        # setpoint 现值 8.8：+0.5 → 9.3 越出 clamp_max 9 → R1 判据步进越域已拦（§6.1）
        rows = [telemetry_row(105, 8.8) if r.point_id == 105 else r for r in full_latest_rows]
        engine, _submitter = build_engine(FakeTsdb(values={**VALUES, 105: 8.8}), latest_rows=rows)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.submitted == []

    async def test_strategy_error_isolation(self, full_latest_rows) -> None:
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows)
        from algo.optimizer.strategies.base import Strategy

        class Boom(Strategy):
            strategy_id = "x.boom"
            algo_path = "optimizer/x-boom"
            system_types = frozenset({"chilled_water"})
            required_quantities = frozenset({"power"})
            target_quantity = "unit_enable"

            def evaluate(self, ctx, plant, cfg):  # type: ignore[no-untyped-def]
                msg = "boom"
                raise RuntimeError(msg)

        engine._strategies = (*engine._strategies, Boom())  # type: ignore[attr-defined]
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.errors == 1  # 单策略异常不中断本轮
        assert len(report.submitted) == 1  # R1 照常出卡


class TestPlantScoping:
    async def test_system_without_chillers_skipped(self, full_latest_rows) -> None:
        snap = build_snapshot()
        eqs = [
            e.model_copy(update={"equipment_type": "chwp_pump"})
            if e.equipment_type == "chiller"
            else e
            for e in snap.equipments
        ]
        snap2 = AssetSnapshot(generated_at=T0, equipments=eqs, points=snap.points)
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows, snapshot=snap2)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.plants == 0 and report.drafts == []

    async def test_rated_params_missing_unit_skipped(self, full_latest_rows) -> None:
        snap = build_snapshot()
        eqs = [
            e.model_copy(update={"rated_params": {}}) if e.equipment_id == EQ2 else e
            for e in snap.equipments
        ]
        snap2 = AssetSnapshot(generated_at=T0, equipments=eqs, points=snap.points)
        engine, _submitter = build_engine(FakeTsdb(), latest_rows=full_latest_rows, snapshot=snap2)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.errors == 0  # WARN 跳过该机组（不硬算），非错误
        assert report.plants == 1  # 单机组仍成 plant（rated 兜底缺失只跳过该机组）
        # 剩余单机 400 kW：R1 照常求值但 E1=4.0 < 噪声下限 5.0 → 仲裁拦截（§7.2）
        assert report.arbitrated is not None and report.arbitrated.floored == 1
        assert report.submitted == []


class TestForecastDegradation:
    async def test_no_power_series_forecast_none(self, full_latest_rows) -> None:
        tsdb = FakeTsdb(values={**VALUES, 102: None, 107: None})  # 功率桶缺失
        engine, _submitter = build_engine(tsdb, latest_rows=full_latest_rows)
        report = await engine.run_round(now=T0 + timedelta(minutes=1))
        assert report.forecast_source == "none"  # §5.2 降级链终点


def _submitter_submitted(engine: OptimizerEngine) -> list[ProposalEnvelope]:
    sub = engine._submitter  # type: ignore[attr-defined]
    return list(getattr(sub, "submitted", []))
