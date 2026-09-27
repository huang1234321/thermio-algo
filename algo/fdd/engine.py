"""FDD 评估循环（algo.md §7.3，每 5min 一轮）。

    fdd_eval(evaluation_ts 对齐到上一个已完结的 5min 桶边界):
      1 snapshot → 2 设备×规则二维展开 → 3 阈值生效 → 4 取窗+质量门控
      → 5 纯函数求值 → 6 迟滞 → 7 drain → 8 一轮一批量提交

单设备单规则失败（含 InsufficientData）不影响同轮其他求值（逐项 try，CODE-LOG-04）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from algo.fdd.base import (
    BucketRow,
    InsufficientData,
    RuleContext,
    RuleOutcome,
    ThresholdSet,
    WeatherObs,
)
from algo.fdd.hysteresis import ClearedEmission, HitEmission, Hysteresis
from algo.fdd.registry import RuleBinding, RuleRegistry
from algo.fdd.rules import RULES
from algo.fdd.thresholds import ThresholdStore
from algo.kafka.consumer import LatestCache
from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.platform.client import PlatformClient
from algo.platform.contracts import FddFindingsBatch, FindingCleared, FindingHit
from algo.semantics.snapshot import SnapshotService
from algo.semantics.types import AssetSnapshot, EquipmentView, PointView, group_points
from algo.tsdb.client import TsdbClient
from algo.tsdb.windows import fetch_buckets, gated_series
from algo.versioning import fdd_algo_version

log = get_logger(__name__)

BUCKET = timedelta(minutes=5)  # cagg 桶宽（ddl.md §11.2）


def align_to_bucket(now: datetime) -> datetime:
    """对齐到**上一个已完结**的 5min 桶边界（右开端点 = evaluation_ts）。"""
    now = now.astimezone()
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    buckets = (now.astimezone(UTC) - epoch) // BUCKET
    return (epoch + buckets * BUCKET).astimezone(now.tzinfo)


class WeatherReader(Protocol):
    """weather_actual 最近观测的读取面（实现：algo.weather.job.WeatherService）。"""

    async def latest_obs(self) -> WeatherObs | None: ...


@dataclass
class RoundReport:
    """单轮评估摘要（日志/指标/测试断言面）。"""

    evaluation_ts: datetime
    algo_version: str
    equipments: int = 0
    evaluations: int = 0
    new_hits: int = 0
    refresh_hits: int = 0
    cleared: int = 0
    insufficient: int = 0
    errors: int = 0
    submitted: bool = False
    submitted_hits: list[FindingHit] = field(default_factory=list)
    submitted_cleared: list[FindingCleared] = field(default_factory=list)


class FddEngine:
    def __init__(
        self,
        tsdb: TsdbClient,
        snapshot: SnapshotService,
        thresholds: ThresholdStore,
        platform: PlatformClient,
        latest: LatestCache,
        registry: RuleRegistry | None = None,
        hysteresis: Hysteresis | None = None,
        weather_reader: WeatherReader | None = None,
    ) -> None:
        self._tsdb = tsdb
        self._snapshot = snapshot
        self._thresholds = thresholds
        self._platform = platform
        self._latest = latest
        self._registry = registry or RuleRegistry(RULES)
        self._hysteresis = hysteresis or Hysteresis()
        self._weather = weather_reader

    @property
    def registry(self) -> RuleRegistry:
        return self._registry

    @property
    def thresholds(self) -> ThresholdStore:
        return self._thresholds

    async def run_round(self, now: datetime | None = None) -> RoundReport:
        """一轮评估（调度任务 fdd_eval 的任务体；调度层兜「单轮失败不中断」）。"""
        with mt.JOB_DURATION_MS.labels(job="fdd_eval").time():
            return await self._run_round_inner(now)

    async def _run_round_inner(self, now: datetime | None) -> RoundReport:
        self._thresholds.maybe_reload()
        version = fdd_algo_version(self._registry.rules, self._thresholds)
        snap: AssetSnapshot = await self._snapshot.ensure_loaded()
        evaluation_ts = align_to_bucket(now or datetime.now().astimezone())
        report = RoundReport(evaluation_ts=evaluation_ts, algo_version=version)

        points_by_eq = group_points(snap)
        weather_obs = (await self._weather.latest_obs()) if self._weather else None

        for equipment in snap.equipments:
            eq_points = points_by_eq.get(equipment.equipment_id, [])
            if not eq_points:
                continue
            report.equipments += 1
            bindings = self._registry.rules_for(equipment, eq_points)
            if not bindings:
                continue
            # 单设备一轮只拉一次窗（全部点位、最宽窗），逐规则做窗口裁剪+质量门控
            all_points = sorted(eq_points, key=lambda p: p.point_id)
            max_window = max(self._thresholds.effective(b.rule).window_minutes for b in bindings)
            raw_buckets = await fetch_buckets(
                self._tsdb,
                [p.point_id for p in all_points],
                evaluation_ts - timedelta(minutes=max_window),
                evaluation_ts,
            )
            for binding in bindings:
                thr = self._thresholds.effective(binding.rule)
                report.evaluations += 1
                mt.FDD_RULES_EVALUATED_TOTAL.inc()
                try:
                    outcome = self._evaluate_binding(
                        equipment, binding, thr, raw_buckets, weather_obs, evaluation_ts
                    )
                except InsufficientData as exc:
                    # 数据不足 ≠ 错误（§5.2 第三态）：跳过该轮，不产 finding、不清计数
                    report.insufficient += 1
                    log.debug(
                        "fdd_insufficient",
                        rule_key=binding.rule.rule_id,
                        equipment_id=equipment.equipment_id,
                        reason=exc.reason,
                    )
                    continue
                except Exception:
                    report.errors += 1
                    log.exception(
                        "fdd_rule_error",
                        rule_key=binding.rule.rule_id,
                        equipment_id=equipment.equipment_id,
                    )
                    continue
                emission = self._hysteresis.update(
                    equipment.equipment_id, binding.rule.rule_id, outcome, thr, evaluation_ts
                )
                if isinstance(emission, HitEmission):
                    if emission.first_detected_at == emission.last_detected_at:
                        report.new_hits += 1
                    else:
                        report.refresh_hits += 1
                    report.submitted_hits.append(
                        FindingHit(
                            equipment_id=emission.equipment_id,
                            rule_key=emission.rule_key,
                            severity=emission.outcome.severity,  # type: ignore[arg-type]
                            title=emission.outcome.title,
                            evidence=emission.outcome.evidence,
                            suggested_action=emission.outcome.suggested_action,
                            first_detected_at=emission.first_detected_at,
                            last_detected_at=emission.last_detected_at,
                        )
                    )
                    mt.FDD_FINDINGS_TOTAL.labels(action="hit").inc()
                elif isinstance(emission, ClearedEmission):
                    report.cleared += 1
                    report.submitted_cleared.append(
                        FindingCleared(
                            equipment_id=emission.equipment_id,
                            rule_key=emission.rule_key,
                            cleared_at=emission.cleared_at,
                        )
                    )
                    mt.FDD_FINDINGS_TOTAL.labels(action="cleared").inc()

        await self._submit(version, report)
        log.info(
            "fdd_round_done",
            algo_version=version,
            equipments=report.equipments,
            evaluations=report.evaluations,
            new_hits=report.new_hits,
            refresh_hits=report.refresh_hits,
            cleared=report.cleared,
            insufficient=report.insufficient,
            errors=report.errors,
            submitted=report.submitted,
        )
        return report

    def _evaluate_binding(
        self,
        equipment: EquipmentView,
        binding: RuleBinding,
        thr: ThresholdSet,
        raw_buckets: dict[int, list[BucketRow]],
        weather_obs: WeatherObs | None,
        evaluation_ts: datetime,
    ) -> RuleOutcome | None:
        points_by_qty: dict[str, PointView] = {}
        for p in sorted(binding.points, key=lambda x: x.point_id):
            points_by_qty.setdefault(p.quantity_type, p)  # 同量多点取首个（WARN 在 registry）
        window_from = evaluation_ts - timedelta(minutes=thr.window_minutes)
        series: dict[str, Sequence[BucketRow]] = {}
        for qty, p in points_by_qty.items():
            rows = [
                r
                for r in raw_buckets.get(p.point_id, [])
                if window_from <= r.bucket < evaluation_ts
            ]
            series[qty] = gated_series(rows, thr.min_good_ratio)
        latest: dict[str, object | None] = {}
        for qty, p in points_by_qty.items():
            row = self._latest.get(p.point_id)
            if row is None:
                latest[qty] = None
            elif row.value is not None:
                latest[qty] = row.value
            else:
                latest[qty] = row.value_text
        ctx = RuleContext(
            equipment=equipment,
            points=points_by_qty,
            series=series,
            latest=latest,
            weather=weather_obs,
            window_from=window_from,
            window_to=evaluation_ts,
        )
        return binding.rule.evaluate(ctx, thr)

    async def _submit(self, version: str, report: RoundReport) -> None:
        """一轮一批量（§7.3-8 / §8.2 wire）。空批不提交（限速面天然满足）。"""
        if not report.submitted_hits and not report.submitted_cleared:
            return
        batch = FddFindingsBatch(
            algo_version=version,
            hits=report.submitted_hits,
            cleared=report.submitted_cleared,
        )
        await self._platform.submit_fdd_findings(batch)
        report.submitted = True
