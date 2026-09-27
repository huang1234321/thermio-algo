"""负荷预测交接（optimizer.md §5.2/§5.4——进程内 ForecastStore，零新增持久化）。

forecast_15m job（离线验证通过后启用常驻）与 optimize_15m 同进程——交接 = 进程
内存（最新一期 + 写入时间），不落 TSDB/PG/Kafka（预测每 15min 重建，无状态纪律
内自然丢弃）。optimize 不等待同槽位预测，消费「最新一期」。

MVP 降级链（model 未上线时的常态）：Store 空/陈旧 → 引擎就地构造
source="persistence" 视图：近 60min telemetry_5min 冷负荷均值 + 最小二乘斜率
外推四点；冷负荷不可测 → forecast=None。降级是常态路径而非错误：不告警只计数
（OBS-MT-01 Rate 面，algo_optimizer_forecast_source）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from algo.fdd.base import BucketRow
from algo.optimizer.context import ForecastPoint, LoadForecastView

STALE_AFTER = timedelta(minutes=45)  # §5.2：model 期新鲜度（3 槽位）
PERSISTENCE_WINDOW = timedelta(minutes=60)  # §5.2：persistence 基窗


class ForecastStore:
    """进程内最新一期交接面（forecast_15m 写入、optimize_15m 读取）。"""

    def __init__(self) -> None:
        self._latest: LoadForecastView | None = None
        self._stored_at: datetime | None = None

    def store(self, view: LoadForecastView, *, now: datetime | None = None) -> None:
        self._latest = view
        self._stored_at = now or datetime.now().astimezone()

    def latest(self, *, now: datetime) -> LoadForecastView | None:
        """最新一期；陈旧（>45min）视为不可用 → None（走降级链）。"""
        if self._latest is None or self._stored_at is None:
            return None
        if now - self._stored_at > STALE_AFTER:
            return None
        return self._latest


@dataclass(frozen=True)
class _SeriesPoint:
    t_min: float  # 相对窗起点的分钟数
    v: float


def _ols_slope(points: list[_SeriesPoint]) -> float:
    n = len(points)
    if n < 2:
        return 0.0
    mx = sum(p.t_min for p in points) / n
    my = sum(p.v for p in points) / n
    sxx = sum((p.t_min - mx) ** 2 for p in points)
    if sxx <= 1e-12:
        return 0.0
    return sum((p.t_min - mx) * (p.v - my) for p in points) / sxx


@dataclass
class PersistenceInput:
    """persistence 视图的装配输入（引擎从 cagg 窗口序列准备）。"""

    evaluation_ts: datetime
    building_id: str
    load_series: list[tuple[datetime, float]] = field(default_factory=list)  # (bucket, kW_th)


def build_persistence_view(inp: PersistenceInput) -> LoadForecastView | None:
    """近 60min 负荷均值 + 最小二乘斜率外推 t+15/30/45/60 四点（§5.2）。

    质量门控（§5.4）：负荷序列不可得 / 均值非正 / 外推四点全非正 → None。
    """
    cutoff = inp.evaluation_ts - PERSISTENCE_WINDOW
    recent = [(ts, v) for ts, v in inp.load_series if ts >= cutoff and v > 0]
    if len(recent) < 2:
        return None
    base_ts = recent[0][0]
    pts = [_SeriesPoint(t_min=(ts - base_ts).total_seconds() / 60.0, v=v) for ts, v in recent]
    mean_v = sum(p.v for p in pts) / len(pts)
    slope_per_min = _ols_slope(pts)
    four: list[ForecastPoint] = []
    for h in (15, 30, 45, 60):
        # §5.2 口径：窗均值基线 + 斜率 × 前瞻步长（flat 数据 → 平推）
        val = mean_v + slope_per_min * h
        four.append(
            ForecastPoint(target_ts=inp.evaluation_ts + timedelta(minutes=h), load_kw_th=val)
        )
    if all(p.load_kw_th <= 0 for p in four):
        return None
    return LoadForecastView(
        issued_at=inp.evaluation_ts,
        building_id=inp.building_id,
        points=tuple(four),
        source="persistence",
        model_version=None,
    )


def series_from_buckets(
    load_buckets: list[BucketRow], *, value_of: str = "avg"
) -> list[tuple[datetime, float]]:
    """cagg 桶序列 → (bucket, 均值) 序列（None 均值剔除）。"""
    out: list[tuple[datetime, float]] = []
    for b in load_buckets:
        v = getattr(b, value_of)
        if v is not None:
            out.append((b.bucket, v))
    return out
