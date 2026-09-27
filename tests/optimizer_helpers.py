"""optimizer 单测工厂（unit/contract/integration 共用；不进包）。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from algo.fdd.base import BucketRow
from algo.kafka.topics import TelemetryRow
from algo.optimizer.context import (
    ChillerRated,
    ChillerUnitView,
    EquipmentPointsView,
    PlantDerived,
    PlantView,
)
from algo.semantics.types import PointView

T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)  # 对齐 15min 桶边界


def dt(minute: float) -> datetime:
    return T0 + timedelta(minutes=minute)


def rated(
    capacity: float = 4220.0,
    power: float = 840.0,
    min_unload: float = 0.25,
    cop: float | None = 5.024,
) -> ChillerRated:
    return ChillerRated(
        rated_cooling_capacity_kw=capacity,
        rated_input_power_kw=power,
        min_unload_ratio=min_unload,
        rated_cop=cop,
    )


def pt(
    point_id: int,
    quantity_type: str,
    equipment_id: str | None = None,
    *,
    unit_std: str | None = "degC",
    clamp_min: float | None = None,
    clamp_max: float | None = None,
    control_mode: str = "advisory",
) -> PointView:
    return PointView(
        point_id=point_id,
        equipment_id=equipment_id,
        quantity_type=quantity_type,
        unit_std=unit_std,
        clamp_min=clamp_min,
        clamp_max=clamp_max,
        control_mode=control_mode,
    )


def buckets(
    point_id: int,
    values: list[float],
    *,
    start_min: float = 0,
    step_min: float = 5,
    sample_count: int = 12,
    bad_count: int = 0,
) -> list[BucketRow]:
    return [
        BucketRow(
            point_id=point_id,
            bucket=dt(start_min + i * step_min),
            avg=v,
            min=v,
            max=v,
            last=v,
            stddev=abs(v) * 0.01,
            sample_count=sample_count,
            bad_count=bad_count,
            quality_mask=0,
        )
        for i, v in enumerate(values)
    ]


def unit_view(
    equipment_id: str,
    *,
    local_id: str | None = None,
    r: ChillerRated | None = None,
    points: dict | None = None,
    latest: dict | None = None,
    series: dict | None = None,
) -> ChillerUnitView:
    return ChillerUnitView(
        equipment_id=equipment_id,
        local_id=local_id or equipment_id,
        rated=r or rated(),
        points=points or {},
        latest=latest or {},
        series=series or {},
    )


def derived(**kw) -> PlantDerived:
    defaults = dict(
        chw_delta_t_c=None,
        load_kw_th=None,
        load_source="power_proxy",
        load_ratio=None,
        running_count=0,
        per_unit_power_kw={},
        per_unit_load_ratio={},
        tower_approach_c=None,
        load_cv=None,
        load_slope_kw_per_min=None,
    )
    defaults.update(kw)
    return PlantDerived(**defaults)


def plant(
    *,
    chillers: list[ChillerUnitView] | None = None,
    towers: list[EquipmentPointsView] | None = None,
    points: dict | None = None,
    latest: dict | None = None,
    series: dict | None = None,
    d: PlantDerived | None = None,
    affine: dict | None = None,
    fdd_open: bool = False,
    unit_fdd_open: dict | None = None,
) -> PlantView:
    return PlantView(
        system_id="sys-op",
        building_id="b-1",
        chillers=chillers or [],
        towers=towers or [],
        points=points or {},
        latest=latest or {},
        series=series or {},
        derived=d or derived(),
        affine_models=affine or {},
        fdd_delta_t_low_open=fdd_open,
        unit_fdd_open=unit_fdd_open or {},
    )


def telemetry_row(
    point_id: int,
    value: float | None,
    *,
    ts: datetime | None = None,
    text: str | None = None,
) -> TelemetryRow:
    return TelemetryRow(
        point_id=point_id,
        gateway_id="GW-SIM-001",
        tenant_id="t-1",
        ts=ts or T0,
        value=value,
        value_text=text,
    )


def cold_plant_points(
    eq1: str = "eq-ch1", eq2: str = "eq-ch2", tw: str = "eq-ct1"
) -> list[PointView]:
    """冷源站标准点表（集成夹具 algo-optimizer-seed.sql 的内存镜像）。"""
    return [
        pt(100, "chw_supply_temp", eq1),
        pt(101, "chw_return_temp", eq1),
        pt(102, "power", eq1, unit_std="kW"),
        pt(103, "run_status", eq1, unit_std=None),
        pt(104, "unit_enable", eq1, unit_std="dimensionless", clamp_min=0.0, clamp_max=1.0),
        pt(105, "chw_supply_temp_setpoint", eq1, clamp_min=5.0, clamp_max=9.0),
        pt(106, "chw_supply_temp_setpoint", eq2, clamp_min=5.0, clamp_max=9.0),
        pt(107, "power", eq2, unit_std="kW"),
        pt(108, "run_status", eq2, unit_std=None),
        pt(109, "unit_enable", eq2, unit_std="dimensionless", clamp_min=0.0, clamp_max=1.0),
        pt(110, "cooling_water_supply_temp", tw),
        pt(111, "tower_fan_power", tw, unit_std="kW"),
        pt(112, "cw_supply_temp_setpoint", eq1, clamp_min=18.0, clamp_max=32.0),
    ]
