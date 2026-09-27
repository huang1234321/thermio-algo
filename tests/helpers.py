"""测试工厂（unit/contract/integration 共用；不进包）。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from algo.fdd.base import BucketRow, RuleContext, ThresholdSet, WeatherObs
from algo.semantics.types import EquipmentView, PointView


def dt(minute: int, second: int = 0, hour: int = 12) -> datetime:
    return datetime(2026, 9, 27, hour, minute, second, tzinfo=UTC)


def eq_view(
    equipment_id: str = "eq-1",
    equipment_type: str = "chiller",
    local_id: str | None = "1#冷机",
    rated: dict | None = None,
) -> EquipmentView:
    return EquipmentView(
        equipment_id=equipment_id,
        equipment_type=equipment_type,
        building_id="b-1",
        tenant_id="t-1",
        local_id=local_id,
        rated_params=rated if rated is not None else {"rated_power_kw": 50.0},
    )


def pt_view(
    point_id: int,
    quantity_type: str,
    equipment_id: str = "eq-1",
    unit_std: str = "degC",
) -> PointView:
    return PointView(
        point_id=point_id,
        equipment_id=equipment_id,
        quantity_type=quantity_type,
        unit_std=unit_std,
    )


def bucket(
    point_id: int,
    minute: int,
    avg: float | None,
    *,
    sample_count: int = 12,
    bad_count: int = 0,
    quality_mask: int = 0,
    stddev: float | None = None,
) -> BucketRow:
    return BucketRow(
        point_id=point_id,
        bucket=dt(minute),
        avg=avg,
        min=avg,
        max=avg,
        last=avg,
        stddev=stddev if stddev is not None else (abs(avg) * 0.01 if avg else None),
        sample_count=sample_count,
        bad_count=bad_count,
        quality_mask=quality_mask,
    )


def thr(
    *,
    severity: str = "warning",
    window_minutes: int = 30,
    confirm_windows: int = 1,
    clear_windows: int = 1,
    min_good_ratio: float = 0.8,
    params: dict | None = None,
) -> ThresholdSet:
    return ThresholdSet(
        severity=severity,
        window_minutes=window_minutes,
        confirm_windows=confirm_windows,
        clear_windows=clear_windows,
        min_good_ratio=min_good_ratio,
        params=params or {},
    )


def make_ctx(
    equipment: EquipmentView | None = None,
    points: dict[str, PointView] | None = None,
    series: dict | None = None,
    latest: dict | None = None,
    weather: WeatherObs | None = None,
) -> RuleContext:
    return RuleContext(
        equipment=equipment or eq_view(),
        points=points or {},
        series=series or {},
        latest=latest if latest is not None else {"run_status": "running"},
        weather=weather,
        window_from=dt(0),
        window_to=dt(30),
    )


def weather(temp_c: float = 15.0, rh_pct: float = 60.0) -> WeatherObs:
    return WeatherObs(
        station_id="ST-1",
        obs_ts=datetime(2026, 9, 27, 12, 0, tzinfo=UTC) - timedelta(minutes=10),
        temp_c=temp_c,
        rh_pct=rh_pct,
    )
