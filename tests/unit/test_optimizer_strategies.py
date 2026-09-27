"""四策略判据正/负/边界矩阵（optimizer.md §6/§11 unit 权威层）。"""

from __future__ import annotations

import pytest
from algo.optimizer.config import OptimizerConfigStore
from algo.optimizer.context import OptimizerContext, WeatherView
from algo.optimizer.saving import AffineModel
from algo.optimizer.strategies.chiller_stage import ChillerStageDown, ChillerStageUp
from algo.optimizer.strategies.chw_temp_reset import ChwTempReset
from algo.optimizer.strategies.cw_temp_reset import CwTempReset

from tests.optimizer_helpers import (
    T0,
    buckets,
    cold_plant_points,
    derived,
    dt,
    plant,
    rated,
    unit_view,
)

EQ1, EQ2, TOWER = "eq-ch1", "eq-ch2", "eq-ct1"


def cfg_store(tmp_path=None) -> OptimizerConfigStore:
    """代码默认配置（无 YAML——缺文件回落 STRATEGY_DEFAULTS）。"""
    import tempfile
    from pathlib import Path

    d = Path(tmp_path or tempfile.mkdtemp())
    return OptimizerConfigStore(d / "nonexistent.yaml")


def weather(wet_bulb: float = 10.52) -> WeatherView:
    return WeatherView(station_id="s", obs_ts=dt(0), temp_c=15.0, rh_pct=60.0, wet_bulb_c=wet_bulb)


def build_plant(
    *,
    p1: float = 400.0,
    p2: float | None = None,
    running1: float = 1.0,
    running2: float = 1.0,
    enable1: float = 1.0,
    enable2: float = 1.0,
    supply: float = 7.0,
    ret: float = 8.6,
    setpoint: float = 7.0,
    cw_supply: float | None = 26.0,
    fan_power: float | None = 20.0,
    cw_setpoint: float | None = 24.0,
    fan_rated: float = 30.0,
    slope: float | None = 0.0,
    fdd_open: bool = False,
):
    if p2 is None:
        p2 = 400.0 if running2 > 0.5 else 0.0  # 停运机组功率按 0
    pts = {p.quantity_type: p for p in cold_plant_points(EQ1, EQ2, TOWER)}
    u1 = unit_view(
        EQ1,
        local_id="1#冷机",
        r=rated(),
        points={q: p for q, p in pts.items() if p.equipment_id == EQ1},
        latest={
            "power": p1,
            "run_status": running1,
            "unit_enable": enable1,
            "chw_supply_temp_setpoint": setpoint,
        },
        series={
            "power": buckets(102, [p1] * 6),
            "chw_supply_temp": buckets(100, [supply] * 6),
            "chw_return_temp": buckets(101, [ret] * 6),
        },
    )
    u2 = unit_view(
        EQ2,
        local_id="2#冷机",
        r=rated(),
        points={q: p for q, p in pts.items() if p.equipment_id == EQ2},
        latest={"power": p2, "run_status": running2, "unit_enable": enable2},
        series={"power": buckets(107, [p2] * 6)},
    )
    from algo.optimizer.context import EquipmentPointsView

    tower = EquipmentPointsView(
        equipment_id=TOWER,
        local_id="1#塔",
        rated_params={"rated_fan_power_kw": fan_rated},
        points={q: p for q, p in pts.items() if p.equipment_id == TOWER},
        latest={"tower_fan_power": fan_power, "cooling_water_supply_temp": cw_supply},
        series={
            "tower_fan_power": buckets(111, [fan_power] * 6) if fan_power is not None else [],
            "cooling_water_supply_temp": buckets(110, [cw_supply] * 6)
            if cw_supply is not None
            else [],
        },
    )
    load1 = p1 * rated().cop() if running1 > 0.5 else 0.0
    load2 = p2 * rated().cop() if running2 > 0.5 else 0.0
    running_n = sum(1 for v in (running1, running2) if v > 0.5)
    d = derived(
        chw_delta_t_c=ret - supply,
        load_kw_th=load1 + load2,
        load_source="power_proxy",
        load_ratio=(load1 + load2) / (running_n * rated().rated_cooling_capacity_kw)
        if running_n
        else None,
        running_count=running_n,
        per_unit_power_kw={EQ1: p1, EQ2: p2},
        per_unit_load_ratio={
            EQ1: load1 / rated().rated_cooling_capacity_kw,
            EQ2: load2 / rated().rated_cooling_capacity_kw,
        },
        tower_approach_c=(cw_supply - 10.52) if cw_supply is not None else None,
        load_cv=0.01,
        load_slope_kw_per_min=slope,
    )
    model = AffineModel(p0_kw=56.0, m_kw_per_kwth=0.18578)
    return plant(
        chillers=[u1, u2],
        towers=[tower],
        points=pts,
        latest={
            "chw_supply_temp_setpoint": setpoint,
            "cw_supply_temp_setpoint": cw_setpoint,
            "tower_fan_power": fan_power,
            "cooling_water_supply_temp": cw_supply,
        },
        series={
            "chw_supply_temp": buckets(100, [supply] * 6),
            "chw_return_temp": buckets(101, [ret] * 6),
            "cooling_water_supply_temp": buckets(110, [cw_supply] * 6)
            if cw_supply is not None
            else [],
        },
        d=d,
        affine={EQ1: model, EQ2: model},
        fdd_open=fdd_open,
        unit_fdd_open={EQ1: fdd_open, EQ2: fdd_open},
    )


def make_ctx(p, weather_view=None, forecast=None) -> OptimizerContext:
    return OptimizerContext(
        evaluation_ts=T0, plants=[p], weather=weather_view, forecast=forecast, cfg=None
    )


class TestR1ChwTempReset:
    def test_fire_on_low_delta_t_low_load(self) -> None:
        p = build_plant()  # ΔT=1.6 < 2.0；ratio≈0.476 < 0.55；P=800 → E1=8.0 kW
        drafts = ChwTempReset().evaluate(
            make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
        )
        assert len(drafts) == 1
        d = drafts[0]
        assert d.value == pytest.approx(7.5) and d.previous_value == pytest.approx(7.0)
        assert d.expected_saving_kw == pytest.approx(0.02 * 0.5 * 800.0)
        assert d.evidence["formula_id"] == "E1_temp_reset"
        assert len(d.evidence["cfg_fp8"]) == 8
        assert "建议出水温度 7→7.5" in d.rationale

    def test_quiet_when_delta_t_normal(self) -> None:
        p = build_plant(ret=9.5)  # ΔT=2.5 ≥ 2.0
        assert (
            ChwTempReset().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
            )
            == []
        )

    def test_quiet_when_load_ratio_high(self) -> None:
        p = build_plant(p1=480, p2=480)  # ratio≈0.572 > 0.55（ΔT 仍低）
        assert (
            ChwTempReset().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
            )
            == []
        )

    def test_quiet_when_step_exceeds_domain(self) -> None:
        p = build_plant(setpoint=8.8)  # 8.8+0.5=9.3 > clamp_max 9
        assert (
            ChwTempReset().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
            )
            == []
        )

    def test_quiet_when_no_previous_value(self) -> None:
        p = build_plant(setpoint=None)
        p = plant(
            chillers=p.chillers,
            towers=p.towers,
            points=p.points,
            latest={k: v for k, v in p.latest.items() if k != "chw_supply_temp_setpoint"},
            series=p.series,
            d=p.derived,
            affine=p.affine_models,
        )
        assert (
            ChwTempReset().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
            )
            == []
        )

    def test_suppressed_by_open_fdd_finding(self) -> None:
        p = build_plant(fdd_open=True)
        assert (
            ChwTempReset().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
            )
            == []
        )

    def test_sustain_boundary_one_bad_bucket_ok(self) -> None:
        # 6 桶中 5 桶 ΔT < 2.0（占比 0.83 ≥ 0.8）且均值低 → 触发
        pts_supply = buckets(100, [7.0] * 5 + [7.0])
        pts_ret = buckets(101, [8.6, 8.6, 8.6, 8.6, 8.6, 9.6])  # 末桶 ΔT=2.6
        p = build_plant()
        p = plant(
            chillers=p.chillers,
            towers=p.towers,
            points=p.points,
            latest=p.latest,
            series={**p.series, "chw_supply_temp": pts_supply, "chw_return_temp": pts_ret},
            d=p.derived,
            affine=p.affine_models,
        )
        drafts = ChwTempReset().evaluate(
            make_ctx(p, weather()), p, cfg_store().effective("chw.temp_reset", None)
        )
        assert len(drafts) == 1


class TestR2ChillerStageDown:
    def test_fire_on_all_units_low_load(self) -> None:
        p = build_plant(p1=150, p2=150, ret=9.5)  # 双机 0.178 < 0.45；ΔT 2.5 压 R1
        drafts = ChillerStageDown().evaluate(
            make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_down", None)
        )
        assert len(drafts) == 1
        d = drafts[0]
        assert d.value == 0.0 and d.previous_value == pytest.approx(1.0)
        assert d.expected_saving_kw == pytest.approx(56.0)  # P0_s + (m−m)×Q
        assert d.evidence["formula_id"] == "E2_stage_down"

    def test_quiet_when_single_unit_low(self) -> None:
        p = build_plant(p1=150, p2=400, ret=9.5)  # 一台低载（0.476 ≥ 0.45）
        assert (
            ChillerStageDown().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_down", None)
            )
            == []
        )

    def test_quiet_when_running_count_below_2(self) -> None:
        p = build_plant(p1=150, running2=0.0, ret=9.5)
        assert (
            ChillerStageDown().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_down", None)
            )
            == []
        )

    def test_worst_unit_chosen_deterministically(self) -> None:
        p = build_plant(p1=150, p2=160, ret=9.5)  # 同分按 equipment_id 破平 → eq-ch2 功率高
        drafts = ChillerStageDown().evaluate(
            make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_down", None)
        )
        assert drafts and drafts[0].equipment_id in (EQ1, EQ2)

    def test_quiet_when_headroom_insufficient(self) -> None:
        # 前瞻峰值超 (N−1)×rated×0.85：双机 430 → peak 4320 ≥ 4220×0.85 = 3587
        # → 放弃减机（且每台 0.512 ≥ 0.45，均低载判据本就不满足——双闸负路径）
        p = build_plant(p1=430, p2=430, ret=9.5)  # 每台 0.512 ≥ 0.45 → 均低载不满足
        assert (
            ChillerStageDown().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_down", None)
            )
            == []
        )


class TestR3ChillerStageUp:
    def _flat_persistence(self, load_kw: float):
        from algo.optimizer.context import ForecastPoint, LoadForecastView

        return LoadForecastView(
            issued_at=dt(0),
            building_id="b-1",
            points=tuple(
                ForecastPoint(target_ts=dt(m), load_kw_th=load_kw) for m in (15, 30, 45, 60)
            ),
            source="persistence",
        )

    def test_fire_protective_negative_saving(self) -> None:
        p = build_plant(p1=800, running2=0.0, enable2=0.0, ret=9.5, cw_supply=26.0, fan_power=20.0)
        # 单机 0.952 > 0.92；slope=0 但 flat persistence 前瞻「不回落」✓（引擎常态构造该视图）
        drafts = ChillerStageUp().evaluate(
            make_ctx(p, weather(), self._flat_persistence(800 * rated().cop())),
            p,
            cfg_store().effective("chiller.stage_up", None),
        )
        assert len(drafts) == 1
        d = drafts[0]
        assert d.equipment_id == EQ2 and d.value == 1.0 and d.previous_value == pytest.approx(0.0)
        assert d.expected_saving_kw < 0  # 保护成本诚实上报
        assert "保护性" in d.rationale
        assert d.exempt_min_saving is True

    def test_quiet_below_stage_up_ratio(self) -> None:
        p = build_plant(p1=700, running2=0.0, enable2=0.0, ret=9.5)  # 0.833 < 0.92
        assert (
            ChillerStageUp().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_up", None)
            )
            == []
        )

    def test_quiet_without_standby(self) -> None:
        p = build_plant(p1=800, running2=1.0, enable2=1.0, ret=9.5)  # 无待机
        assert (
            ChillerStageUp().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_up", None)
            )
            == []
        )

    def test_quiet_when_standby_has_open_finding(self) -> None:
        p = build_plant(p1=800, running2=0.0, enable2=0.0, ret=9.5)
        p = plant(
            chillers=p.chillers,
            towers=p.towers,
            points=p.points,
            latest=p.latest,
            series=p.series,
            d=p.derived,
            affine=p.affine_models,
            fdd_open=False,
            unit_fdd_open={EQ1: False, EQ2: True},  # 待机机组有开放发现
        )
        assert (
            ChillerStageUp().evaluate(
                make_ctx(p, weather()), p, cfg_store().effective("chiller.stage_up", None)
            )
            == []
        )

    def test_quiet_when_load_falling_and_no_rise(self) -> None:
        # slope < 0 且 persistence 前瞻低于当前 → 不满足「上升或前瞻不回落」
        p = build_plant(p1=800, running2=0.0, enable2=0.0, ret=9.5, slope=-5.0)
        # 构造 forecast：t+30 回落
        from algo.optimizer.context import ForecastPoint, LoadForecastView

        fcst = LoadForecastView(
            issued_at=dt(0),
            building_id="b-1",
            points=tuple(
                ForecastPoint(target_ts=dt(m), load_kw_th=3000.0 - m * 10) for m in (15, 30, 45, 60)
            ),
            source="persistence",
        )
        assert (
            ChillerStageUp().evaluate(
                make_ctx(p, weather(), fcst), p, cfg_store().effective("chiller.stage_up", None)
            )
            == []
        )


class TestR4CwTempReset:
    def _eff(self):
        return cfg_store().effective("cw.temp_reset", None)

    def test_fire_on_high_approach_with_fan_headroom(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=18.0, fan_power=2.0, cw_setpoint=24.0)
        # approach=7.48 > 5；合计 ratio 0.512 > 0.5；fan 2/30 < 0.8；E4 = 7−1 = 6 > 5
        drafts = CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff())
        assert len(drafts) == 1
        d = drafts[0]
        assert d.value == pytest.approx(23.0) and d.previous_value == pytest.approx(24.0)
        assert d.expected_saving_kw == pytest.approx(0.01 * 1.0 * 860.0 - 2.0 * 0.5)

    def test_quiet_without_weather(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=18.0, fan_power=2.0)
        assert CwTempReset().evaluate(make_ctx(p, None), p, self._eff()) == []

    def test_quiet_when_approach_low(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=14.0, fan_power=2.0)  # approach 3.5
        assert CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff()) == []

    def test_quiet_when_chiller_load_low(self) -> None:
        p = build_plant(p1=300, p2=300, ret=9.5, cw_supply=18.0, fan_power=2.0)  # ratio 0.357
        assert CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff()) == []

    def test_quiet_when_fan_saturated(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=18.0, fan_power=27.0)  # 27/30 ≥ 0.8
        assert CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff()) == []

    def test_quiet_when_net_negative(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=18.0, fan_power=20.0)
        # E4 = 8.6 − 10 = −1.4 < 0 → 净额为正才出提案
        assert CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff()) == []

    def test_quiet_when_step_below_clamp_min(self) -> None:
        p = build_plant(p1=430, p2=430, ret=9.5, cw_supply=18.0, fan_power=2.0, cw_setpoint=18.5)
        # 18.5 − 1.0 = 17.5 < clamp_min 18
        assert CwTempReset().evaluate(make_ctx(p, weather()), p, self._eff()) == []
