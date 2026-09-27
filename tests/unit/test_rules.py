"""规则求值矩阵（正/负/边界；纯函数零 mock——CODE-TST-02/03 权威层）。"""

from __future__ import annotations

import pytest
from algo.fdd.base import InsufficientData
from algo.fdd.rules.chiller import (
    ChillerDeltaTLow,
    ChillerPowerRatioHigh,
    ChillerSupplyTempHigh,
    ChillerTempSensorReversed,
)
from algo.fdd.rules.common import is_running, wetbulb_c
from algo.fdd.rules.cooling_tower import (
    CoolingTowerApproachHigh,
    CoolingTowerDeltaTHigh,
    CoolingTowerSupplyTempHigh,
)
from algo.fdd.rules.pump import (
    ChwpPowerRatioHighRunning,
    ChwpPowerRatioLowRunning,
    ChwpPowerUnstable,
)

from tests.helpers import bucket, eq_view, make_ctx, pt_view, thr, weather

CH_POINTS = {
    "chw_supply_temp": pt_view(1, "chw_supply_temp"),
    "chw_return_temp": pt_view(2, "chw_return_temp"),
    "power": pt_view(3, "power"),
    "run_status": pt_view(4, "run_status"),
}


def ch_ctx(
    supply: float,
    ret: float,
    power: float = 22.0,
    run: object = "running",
    series: dict | None = None,
) -> object:
    if series is None:
        series = {
            "chw_supply_temp": [bucket(1, m, supply + 0.1 * m) for m in range(0, 30, 5)],
            "chw_return_temp": [bucket(2, m, ret + 0.1 * m) for m in range(0, 30, 5)],
            "power": [bucket(3, m, power) for m in range(0, 30, 5)],
        }
    return make_ctx(points=CH_POINTS, series=series, latest={"run_status": run})


# ── chiller.delta_t_low ──────────────────────────────────────────────────────


def test_delta_t_low_hits() -> None:
    ctx = ch_ctx(supply=20.0, ret=20.5, power=22.0)  # ΔT=0.5 < 1.2，负荷 0.44
    out = ChillerDeltaTLow().evaluate(
        ctx, thr(params={"delta_t_min_c": 1.2, "load_ratio_min": 0.3})
    )
    assert out is not None
    assert out.severity == "warning"
    assert "ΔT=0.5" in out.title
    assert out.evidence["detail"]["delta_t_avg_c"] == pytest.approx(0.5, abs=0.01)
    assert {p["point_id"] for p in out.evidence["points"]} == {1, 2, 3, 4}
    assert out.evidence["window"]["from"].endswith("+00:00")  # RFC3339 带时区
    assert out.suggested_action


def test_delta_t_low_negative_paths() -> None:
    rule = ChillerDeltaTLow()
    t = thr(params={"delta_t_min_c": 1.2, "load_ratio_min": 0.3})
    assert rule.evaluate(ch_ctx(20.0, 22.0), t) is None  # ΔT 正常
    assert rule.evaluate(ch_ctx(20.0, 20.5, power=5.0), t) is None  # 低负荷不评判
    with pytest.raises(InsufficientData):
        rule.evaluate(ch_ctx(20.0, 20.5, run="stopped"), t)  # 停机
    with pytest.raises(InsufficientData):
        rule.evaluate(make_ctx(points=CH_POINTS, latest={"run_status": "running"}), t)  # 无窗数据


def test_delta_t_low_missing_rated_params() -> None:
    ctx = make_ctx(
        equipment=eq_view(rated={}),
        points=CH_POINTS,
        series={
            "chw_supply_temp": [bucket(1, 0, 20.0)],
            "chw_return_temp": [bucket(2, 0, 20.5)],
            "power": [bucket(3, 0, 22.0)],
        },
    )
    with pytest.raises(InsufficientData, match="rated_power_kw"):
        ChillerDeltaTLow().evaluate(ctx, thr())


# ── chiller.supply_temp_high / power_ratio_high / sensor_reversed ───────────


def test_supply_temp_high() -> None:
    series = {"chw_supply_temp": [bucket(1, 0, 9.5)]}
    ctx = make_ctx(points=CH_POINTS, series=series)
    rule = ChillerSupplyTempHigh()
    out = rule.evaluate(ctx, thr(severity=rule.default_severity, params={"supply_max_c": 9.0}))
    assert out is not None and out.severity == "minor"
    assert (
        ChillerSupplyTempHigh().evaluate(
            make_ctx(points=CH_POINTS, series={"chw_supply_temp": [bucket(1, 0, 7.0)]}),
            thr(params={"supply_max_c": 9.0}),
        )
        is None
    )


def test_power_ratio_high() -> None:
    ctx = make_ctx(
        points=CH_POINTS,
        series={"power": [bucket(3, 0, 49.0)]},  # 49/50 = 0.98 > 0.95
    )
    out = ChillerPowerRatioHigh().evaluate(ctx, thr(severity="major"))
    assert out is not None and out.severity == "major"
    assert "98%" in out.title


def test_sensor_reversed() -> None:
    ctx = ch_ctx(supply=20.0, ret=19.0)  # ΔT=-1 < 0
    out = ChillerTempSensorReversed().evaluate(ctx, thr())
    assert out is not None
    assert ch_ctx(20.0, 21.0) is not None
    assert ChillerTempSensorReversed().evaluate(ch_ctx(20.0, 21.5), thr()) is None


# ── pump ─────────────────────────────────────────────────────────────────────

PUMP_POINTS = {"power": pt_view(10, "power"), "run_status": pt_view(11, "run_status")}


def pump_ctx(power_rows: list, run: object = "running", rated: float = 10.0) -> object:
    return make_ctx(
        equipment=eq_view("eq-p", "chwp_pump", rated={"rated_power_kw": rated}),
        points=PUMP_POINTS,
        series={"power": power_rows},
        latest={"run_status": run},
    )


def test_pump_power_ratio_low() -> None:
    rule = ChwpPowerRatioLowRunning()
    t = thr(severity=rule.default_severity)
    out = rule.evaluate(pump_ctx([bucket(10, 0, 1.5)]), t)  # 1.5/10 = 0.15 < 0.3
    assert out is not None and out.severity == "minor"
    assert rule.evaluate(pump_ctx([bucket(10, 0, 5.0)]), t) is None


def test_pump_power_ratio_high() -> None:
    rule = ChwpPowerRatioHighRunning()
    out = rule.evaluate(pump_ctx([bucket(10, 0, 12.0)]), thr(severity=rule.default_severity))
    assert out is not None and out.severity == "major"


def test_pump_power_unstable() -> None:
    rule = ChwpPowerUnstable()
    t = thr(severity=rule.default_severity)
    rows = [bucket(10, m, 10.0 + 8.0 * (m % 2)) for m in range(0, 30, 5)]  # CV ~0.29
    out = rule.evaluate(pump_ctx(rows), t)
    assert out is not None and "CV" in out.title
    stable = [bucket(10, m, 10.0 + 0.1 * m) for m in range(0, 30, 5)]
    assert rule.evaluate(pump_ctx(stable), t) is None
    # 低值段不评判（CV 分母噪声主导）
    assert rule.evaluate(pump_ctx([bucket(10, 0, 0.1)]), t) is None


# ── cooling tower ────────────────────────────────────────────────────────────

CT_POINTS = {
    "cooling_water_supply_temp": pt_view(20, "cooling_water_supply_temp"),
    "cooling_water_return_temp": pt_view(21, "cooling_water_return_temp"),
    "run_status": pt_view(22, "run_status"),
}


def ct_ctx(supply: float, ret: float | None = None, wx=None) -> object:
    series: dict = {"cooling_water_supply_temp": [bucket(20, 0, supply)]}
    if ret is not None:
        series["cooling_water_return_temp"] = [bucket(21, 0, ret)]
    return make_ctx(points=CT_POINTS, series=series, weather=wx)


def test_approach_high_hits_and_weather_gating() -> None:
    t = thr(params={"approach_max_c": 6.0})
    out = CoolingTowerApproachHigh().evaluate(ct_ctx(23.0, wx=weather(temp_c=15.0, rh_pct=60.0)), t)
    assert out is not None  # 湿球 ~11°C，approach ~12 > 6
    assert out.evidence["detail"]["weather_station"] == "ST-1"
    # 无天气观测 → 数据不足（第三态），不硬算
    with pytest.raises(InsufficientData):
        CoolingTowerApproachHigh().evaluate(ct_ctx(23.0), t)
    # 正常逼近 → 不命中
    assert (
        CoolingTowerApproachHigh().evaluate(ct_ctx(16.0, wx=weather(temp_c=15.0, rh_pct=60.0)), t)
        is None
    )


def test_tower_supply_high_and_delta_high() -> None:
    t = thr(params={"supply_max_c": 37.0})
    assert CoolingTowerSupplyTempHigh().evaluate(ct_ctx(38.0), t) is not None
    assert CoolingTowerSupplyTempHigh().evaluate(ct_ctx(30.0), t) is None
    t2 = thr(params={"delta_t_max_c": 8.0})
    out = CoolingTowerDeltaTHigh().evaluate(ct_ctx(30.0, ret=39.0), t2)
    assert out is not None and "ΔT=9.0" in out.title
    assert CoolingTowerDeltaTHigh().evaluate(ct_ctx(30.0, ret=35.0), t2) is None


# ── 公共助手 ────────────────────────────────────────────────────────────────


def test_is_running_variants() -> None:
    assert is_running("running") is True
    assert is_running("RUN") is True
    assert is_running(1) is True
    assert is_running("stopped") is False
    assert is_running(0) is False
    assert is_running("garbage") is None
    assert is_running(None) is None


def test_wetbulb_sanity() -> None:
    """Stull 2011 参考值：25°C/50% ≈ 17.6°C（±0.3°C 域内）。"""
    assert wetbulb_c(weather(25.0, 50.0)) == pytest.approx(17.6, abs=0.5)
    assert wetbulb_c(weather(15.0, 100.0)) == pytest.approx(15.0, abs=0.2)  # 饱和 = 干球
