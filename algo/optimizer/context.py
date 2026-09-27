"""OptimizerContext 及其数据视图（optimizer.md §3——algo.md §11.2 ctx 扩充的定形）。

优化器不做任何 I/O，数据一律由 ctx 给足（与 Rule.evaluate 同款纯函数纪律）。
引擎在进程外装配：latest（algo-optimizer 组最新值缓存视图）+ cagg 窗口序列
（§5.2 质量门控后）+ 天气视图（湿球为应用层推导）+ 负荷预测视图（可能为 None）。
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from algo.fdd.base import BucketRow
from algo.semantics.types import PointView


@dataclass(frozen=True)
class ChillerRated:
    """单机额定参数（rated_params 解析 + YAML 兜底，§6.6-2）。缺键时由装配层兜底，
    仍缺 → 该机组对该策略不适用（WARN + 指标，不硬算）。"""

    rated_cooling_capacity_kw: float  # Q_rated（kW_th）
    rated_input_power_kw: float  # P_rated（kW_e）
    min_unload_ratio: float  # u（最小制冷量百分比）
    design_delta_t_c: float | None = None  # 设计供回温差（信息性）
    rated_cop: float | None = None  # 缺省时按 Q_rated / P_rated 推导

    def cop(self) -> float:
        """额定 COP（proxy 负荷估计用，§6.7）。"""
        return (
            self.rated_cop
            if self.rated_cop
            else self.rated_cooling_capacity_kw / (self.rated_input_power_kw or 1.0)
        )


@dataclass(frozen=True)
class ChillerUnitView:
    """冷源系统内单台冷机（equipment_type=chiller 设备子树投影）。"""

    equipment_id: str
    local_id: str
    rated: ChillerRated
    points: dict[str, PointView] = field(default_factory=dict)  # key=quantity_type
    latest: dict[str, float | None] = field(default_factory=dict)  # run_status 已数值化
    series: dict[str, list[BucketRow]] = field(default_factory=dict)


@dataclass(frozen=True)
class EquipmentPointsView:
    """非冷机设备（冷却塔等）的最小投影（R4 用）。"""

    equipment_id: str
    local_id: str
    rated_params: dict[str, Any] = field(default_factory=dict)  # rated_fan_power_kw 等
    points: dict[str, PointView] = field(default_factory=dict)
    latest: dict[str, float | None] = field(default_factory=dict)
    series: dict[str, list[BucketRow]] = field(default_factory=dict)


@dataclass(frozen=True)
class PlantDerived:
    """引擎统一预计算的派生指标（§3.3——策略共享量只算一次，公式集中单测）。"""

    chw_delta_t_c: float | None  # 回水−供水窗口均值
    load_kw_th: float | None  # 冷负荷（流量实测或 power proxy，§6.7）
    load_source: str  # "flow" | "power_proxy"
    load_ratio: float | None  # load / 在运单机额定制冷量合计
    running_count: int
    per_unit_power_kw: dict[str, float]  # equipment_id → 窗口均值功率
    per_unit_load_ratio: dict[str, float]  # equipment_id → 单机负荷率
    tower_approach_c: float | None  # 冷却水供水温 − 湿球（无湿球/无测点 = None）
    load_cv: float | None  # 30min 窗负荷变异系数 σ/μ（§8.3 f_stab）
    load_slope_kw_per_min: float | None  # 窗口负荷最小二乘斜率（R3/前瞻降级用）


@dataclass(frozen=True)
class PlantView:
    """scope 内一个冷源系统（§3）。points 为系统子树首见投影（key=quantity_type）。

    latest 对目标点（setpoint/unit_enable）已由装配层经 §3.2 三级链补全——
    策略读到的即信封 previous_value 的取值（单一事实，防双读漂移）。
    """

    system_id: str
    building_id: str
    chillers: Sequence[ChillerUnitView]
    towers: Sequence[EquipmentPointsView]
    points: dict[str, PointView] = field(default_factory=dict)
    latest: dict[str, float | None] = field(default_factory=dict)
    series: dict[str, list[BucketRow]] = field(default_factory=dict)
    derived: PlantDerived | None = None  # 引擎装配后回填（frozen → replace）
    affine_models: dict[str, Any] = field(default_factory=dict)  # eq_id → saving.AffineModel
    fdd_delta_t_low_open: bool = False  # §6.7 R1 抑制（进程内 hysteresis 视图）
    unit_fdd_open: dict[str, bool] = field(default_factory=dict)  # eq_id → 任一开放发现


@dataclass(frozen=True)
class WeatherView:
    """天气视图（§3.4）：最近实况 + 应用层湿球（Stull 2011）。"""

    station_id: str
    obs_ts: datetime
    temp_c: float
    rh_pct: float
    wet_bulb_c: float


@dataclass(frozen=True)
class ForecastPoint:
    target_ts: datetime
    load_kw_th: float  # 预测冷负荷（kW_th）


@dataclass(frozen=True)
class LoadForecastView:
    """负荷预测交接视图（§5.1）。source ∈ model|persistence；model 时 model_version 必填。"""

    issued_at: datetime
    building_id: str
    points: tuple[ForecastPoint, ...]  # t+15/30/45/60min 四点
    source: str
    model_version: str | None = None

    def peak_kw_th(self, *, from_min: float, to_min: float, now: datetime) -> float | None:
        """[from_min, to_min] 前瞻窗内的预测峰值（R2 判据）。无覆盖点 = None。"""
        vals = [
            p.load_kw_th
            for p in self.points
            if from_min <= (p.target_ts - now).total_seconds() / 60.0 <= to_min
        ]
        return max(vals) if vals else None


@dataclass(frozen=True)
class OptimizerContext:
    """一轮寻优的只读上下文（§3）。weather/forecast 为 None 时规则必须仍可求值（§4.3 L1 自足）。"""

    evaluation_ts: datetime  # 对齐已完结 15min 桶边界
    plants: Sequence[PlantView]
    weather: WeatherView | None
    forecast: LoadForecastView | None
    cfg: Any  # EffectiveConfig（config.py；Any 防循环导入，键取值面单测钉死）


def wet_bulb_c(t_db_c: float, rh_pct: float) -> float:
    """Stull (2011) 湿球近似（§3.4）。有效域：常压、T∈[−20,50]°C、RH∈[5,99]%
    （本项目运行域内；域外值由调用方保证不进入）。"""
    result: float = (
        t_db_c * math.atan(0.151977 * math.sqrt(rh_pct + 8.313659))
        + math.atan(t_db_c + rh_pct)
        - math.atan(rh_pct - 1.676331)
        + 0.00391838 * rh_pct**1.5 * math.atan(0.023101 * rh_pct)
        - 4.686035
    )
    return result
