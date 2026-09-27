"""规则公共求值助手（纯函数，供 rules/* 复用；不构成对外 API）。"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from algo.fdd.base import BucketRow, InsufficientData, RuleContext, WeatherObs

if TYPE_CHECKING:  # 防环导入（semantics.types 不依赖本包，纯类型面）
    from algo.semantics.types import PointView

# run_status 枚态取值（gw-sim/常见 BA 约定）；新增值 = 发版动作（量类型治理同源）
_RUNNING_TEXT = frozenset({"running", "run", "on", "1"})
_STOPPED_TEXT = frozenset({"stopped", "stop", "off", "0"})


def is_running(latest: Any) -> bool | None:
    """最新 run_status → True/False；取值不可判读 → None（调用方按数据不足处理）。"""
    if latest is None:
        return None
    if isinstance(latest, str):
        if latest.lower() in _RUNNING_TEXT:
            return True
        if latest.lower() in _STOPPED_TEXT:
            return False
        return None
    if isinstance(latest, bool):
        return latest
    if isinstance(latest, (int, float)):
        return bool(latest == 1)
    return None


def require_running(ctx: RuleContext) -> None:
    """run_status 当前态 = 运行（§4.3-1：当前值走最新值缓存）。停机/不可判读 → 跳过。"""
    running = is_running(ctx.latest.get("run_status"))
    if running is None:
        raise InsufficientData("run_status 最新值缺失或不可判读")
    if not running:
        raise InsufficientData("设备停机，本轮不评判")


def series_avg(series: Sequence[BucketRow]) -> float | None:
    """桶均值的窗均值（None 均值桶跳过——枚态量不参与数值规则）。"""
    vals = [b.avg for b in series if b.avg is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def series_cv(series: Sequence[BucketRow]) -> float | None:
    """桶均值变异系数 stddev/mean（不稳定工况判据）。"""
    vals = [b.avg for b in series if b.avg is not None]
    if len(vals) < 2:
        return None
    mean = sum(vals) / len(vals)
    if abs(mean) < 1e-9:
        return None
    var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
    return math.sqrt(var) / abs(mean)


def require_avg(ctx: RuleContext, quantity: str) -> float:
    """取窗均值；无桶/无有效样本 → InsufficientData（第三态，不硬算）。"""
    series = ctx.series.get(quantity)
    if not series:
        raise InsufficientData(f"{quantity} 窗口无有效桶（质量门控后为空）")
    avg = series_avg(series)
    if avg is None:
        raise InsufficientData(f"{quantity} 窗口无有效数值样本（枚态/空值）")
    return avg


def rated_power_kw(ctx: RuleContext) -> float:
    """额定功率（rated_params 键约定 rated_power_kw）。缺 → 数据不足（负荷比不可算）。"""
    v = ctx.equipment.rated_params.get("rated_power_kw")
    if v is None or not isinstance(v, (int, float)) or v <= 0:
        raise InsufficientData("equipment.rated_params.rated_power_kw 缺失或非法")
    return float(v)


def wetbulb_c(obs: WeatherObs) -> float:
    """由 temp_c + rh_pct 推湿球（Stull 2011 近似，常域误差 ~±0.3°C）。

    weather_actual 无湿球列（ddl.md §11.4）——湿球判据的推导口径固定在此，
    供 cooling_tower.approach_high 与 M&V 后续共用。
    """
    if obs.temp_c is None or obs.rh_pct is None:
        raise InsufficientData("weather_actual 缺 temp_c/rh_pct，湿球不可推")
    t, rh = obs.temp_c, obs.rh_pct
    if rh < 0 or rh > 100:
        raise InsufficientData("weather rh_pct 越界")
    result = (
        t * math.atan(0.151977 * math.sqrt(rh + 8.313659))
        + math.atan(t + rh)
        - math.atan(rh - 1.676331)
        + 0.00391838 * rh**1.5 * math.atan(0.023101 * rh)
        - 4.686035
    )
    return float(result)


def equipment_label(ctx: RuleContext) -> str:
    """发现文案里的设备名（local_id 现场编号优先，§6.2）。"""
    return ctx.equipment.local_id or ctx.equipment.name or ctx.equipment.equipment_id[:8]


def participating_points(ctx: RuleContext, quantities: frozenset[str]) -> list[PointView]:
    """规则的参与点位（evidence 骨架用，按 point_id 稳定排序）。"""
    return sorted(
        (p for p in ctx.points.values() if p.quantity_type in quantities),
        key=lambda p: p.point_id,
    )
