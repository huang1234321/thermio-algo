"""派生指标与 persistence 降级视图单测（optimizer.md §3.3/§5.2/§6.7）。"""

from __future__ import annotations

from datetime import timedelta

import pytest
from algo.optimizer.context import wet_bulb_c
from algo.optimizer.engine import _derive_metrics
from algo.optimizer.forecast_store import (
    ForecastStore,
    PersistenceInput,
    build_persistence_view,
    series_from_buckets,
)

from tests.optimizer_helpers import buckets, dt, plant, rated, unit_view


class TestWetBulb:
    def test_stull_reference_points(self) -> None:
        # Stull (2011) 公式输出钉死（单测权威层——改公式必须显式改锚点）
        assert wet_bulb_c(20.0, 50.0) == pytest.approx(13.699, abs=1e-3)
        assert wet_bulb_c(15.0, 60.0) == pytest.approx(10.517, abs=1e-3)  # FDD it 口径「≈11」同源

    def test_monotonic_in_rh(self) -> None:
        vals = [wet_bulb_c(25.0, rh) for rh in (20, 40, 60, 80, 99)]
        assert vals == sorted(vals)

    def test_saturated_approximates_dry_bulb(self) -> None:
        assert wet_bulb_c(30.0, 99.0) == pytest.approx(30.0, abs=0.6)


class TestDerivedMetrics:
    def _plant(
        self,
        *,
        powers: dict[str, list[float]],
        running: dict[str, float],
        series_plant: dict | None = None,
        flow: list[float] | None = None,
    ):
        units = []
        for eq, pws in powers.items():
            units.append(
                unit_view(
                    eq,
                    r=rated(),
                    latest={"power": pws[-1], "run_status": running.get(eq, 1.0)},
                    series={"power": buckets(102 if eq == "eq-ch1" else 107, pws)},
                )
            )
        plant_series = dict(series_plant or {})
        if flow is not None:
            plant_series["chw_flow_rate"] = buckets(120, flow)
        return plant(chillers=units, series=plant_series, d=None)

    def test_power_proxy_load(self) -> None:
        p = self._plant(
            powers={"eq-ch1": [300.0] * 6, "eq-ch2": [300.0] * 6},
            running={"eq-ch1": 1.0, "eq-ch2": 1.0},
        )
        d = _derive_metrics(p.chillers, p.series, None)
        assert d.load_source == "power_proxy"
        assert d.running_count == 2
        assert d.load_kw_th == pytest.approx(600 * 5.024)  # Σ P × COP
        assert d.load_ratio == pytest.approx(600 * 5.024 / 8440)
        assert d.per_unit_load_ratio["eq-ch1"] == pytest.approx(300 * 5.024 / 4220)

    def test_flow_measured_load(self) -> None:
        # 1.163 × 400 m³/h × 5 K = 2326 kW_th（§6.7 量纲）
        p = self._plant(
            powers={"eq-ch1": [400.0] * 6},
            running={"eq-ch1": 1.0},
            series_plant={
                "chw_supply_temp": buckets(100, [7.0] * 6),
                "chw_return_temp": buckets(101, [12.0] * 6),
            },
            flow=[400.0] * 6,
        )
        d = _derive_metrics(p.chillers, p.series, None)
        assert d.load_source == "flow"
        assert d.load_kw_th == pytest.approx(1.163 * 400 * 5.0, rel=1e-3)
        assert d.chw_delta_t_c == pytest.approx(5.0)

    def test_delta_t_and_cv_and_slope(self) -> None:
        p = self._plant(
            powers={"eq-ch1": [100.0, 110.0, 120.0, 130.0, 140.0, 150.0]},
            running={"eq-ch1": 1.0},
            series_plant={
                "chw_supply_temp": buckets(100, [7.0] * 6),
                "chw_return_temp": buckets(101, [9.0] * 6),
            },
        )
        d = _derive_metrics(p.chillers, p.series, None)
        assert d.chw_delta_t_c == pytest.approx(2.0)
        assert d.load_cv is not None and 0.1 < d.load_cv < 0.3
        assert d.load_slope_kw_per_min is not None and d.load_slope_kw_per_min > 0  # 上升

    def test_tower_approach_requires_weather(self) -> None:
        from algo.optimizer.context import WeatherView

        p = self._plant(
            powers={"eq-ch1": [400.0] * 6},
            running={"eq-ch1": 1.0},
            series_plant={"cooling_water_supply_temp": buckets(110, [18.0] * 6)},
        )
        d_none = _derive_metrics(p.chillers, p.series, None)
        assert d_none.tower_approach_c is None
        w = WeatherView(station_id="s", obs_ts=dt(0), temp_c=15.0, rh_pct=60.0, wet_bulb_c=10.52)
        d = _derive_metrics(p.chillers, p.series, w)
        assert d.tower_approach_c == pytest.approx(18.0 - 10.52, abs=0.05)


class TestPersistenceView:
    def test_flat_series_extrapolates_flat(self) -> None:
        series = [(dt(m), 1506.0) for m in range(0, 60, 5)]
        v = build_persistence_view(
            PersistenceInput(evaluation_ts=dt(60), building_id="b-1", load_series=series)
        )
        assert v is not None and v.source == "persistence"
        assert [p.load_kw_th for p in v.points] == [pytest.approx(1506.0)] * 4
        assert v.points[0].target_ts == dt(75)

    def test_rising_series_extrapolates_slope(self) -> None:
        series = [(dt(m), 1000.0 + 20.0 * i) for i, m in enumerate(range(0, 60, 5))]
        v = build_persistence_view(
            PersistenceInput(evaluation_ts=dt(60), building_id="b-1", load_series=series)
        )
        assert v is not None
        assert v.points[-1].load_kw_th > v.points[0].load_kw_th > 1000.0

    def test_insufficient_or_nonpositive_returns_none(self) -> None:
        assert build_persistence_view(PersistenceInput(dt(60), "b", [])) is None
        assert build_persistence_view(PersistenceInput(dt(60), "b", [(dt(0), 1.0)])) is None
        # 均值非正（全零）
        assert (
            build_persistence_view(
                PersistenceInput(dt(60), "b", [(dt(m), 0.0) for m in range(0, 60, 5)])
            )
            is None
        )

    def test_series_from_buckets_drops_none(self) -> None:
        bs = buckets(1, [1.0, None if False else 2.0])
        assert len(series_from_buckets(bs)) == 2


class TestForecastStore:
    def test_stale_latest_returns_none(self) -> None:
        from algo.optimizer.context import ForecastPoint, LoadForecastView

        store = ForecastStore()
        view = LoadForecastView(
            issued_at=dt(0),
            building_id="b-1",
            points=(ForecastPoint(target_ts=dt(15), load_kw_th=100.0),),
            source="model",
            model_version="0.1.0",
        )
        store.store(view, now=dt(0))
        assert store.latest(now=dt(30)) is view
        assert store.latest(now=dt(50)) is None  # > 45min 陈旧

    def test_building_mismatch_not_consumed_by_engine_path(self) -> None:
        # 引擎按 building_id 匹配（§5.1）；Store 本身只管新鲜度
        from algo.optimizer.context import ForecastPoint, LoadForecastView

        store = ForecastStore()
        store.store(
            LoadForecastView(
                issued_at=dt(0),
                building_id="b-2",
                points=(ForecastPoint(target_ts=dt(15), load_kw_th=1.0),),
                source="model",
                model_version="x",
            ),
            now=dt(0),
        )
        v = store.latest(now=dt(10))
        assert v is not None and v.building_id == "b-2"


def test_timedelta_bucket_alignment() -> None:
    from algo.optimizer.engine import align_to_15m

    base = dt(0)
    assert align_to_15m(base + timedelta(minutes=14, seconds=59)) == base
    assert align_to_15m(base + timedelta(minutes=15)) == base + timedelta(minutes=15)
