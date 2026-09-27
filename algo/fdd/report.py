"""FDD 日/周报告聚合（algo.md §8.3；summary schema 钉死于 modules/M6-fdd.md §4.4）。

数据流（M6-fdd §6）：algo 以 period 为活跃窗口逐楼宇分页拉取
GET /internal/fdd/findings → 本地按 §4.4 分类判据聚合 → POST /internal/fdd/reports。
同期唯一 (tenant, building, period_type, period) ⇒ api 侧 upsert，报告任务天然可重跑。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from algo.fdd.base import Rule
from algo.fdd.thresholds import ThresholdStore
from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.platform.client import PlatformClient
from algo.platform.contracts import (
    FddFindingListItem,
    FddReportSubmission,
    HealthRankEntry,
    PeriodType,
    ReportCounts,
    ReportPeriod,
    ReportSummary,
)
from algo.semantics.snapshot import SnapshotService
from algo.versioning import fdd_algo_version

log = get_logger(__name__)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

# 与告警同源五级（ddl.md §9.2 CHECK；M6-fdd §4.4：五键全给，0 也给）
SEVERITIES: tuple[str, ...] = ("info", "warning", "minor", "major", "critical")
# 健康度权重（M6-fdd §4.1/R2：随 shared-types 常量钉死，先于此处落地）
SEVERITY_WEIGHTS: dict[str, int] = {
    "info": 1,
    "warning": 2,
    "minor": 4,
    "major": 8,
    "critical": 16,
}
HEALTH_RANKING_MAX = 10


def previous_day(now: datetime) -> tuple[date, date]:
    """前一自然日（本地时区 Asia/Shanghai）闭开区间 [start, end)。"""
    d = (now.astimezone(LOCAL_TZ) - timedelta(days=1)).date()
    return d, d + timedelta(days=1)


def previous_week(now: datetime) -> tuple[date, date]:
    """前一自然周 [周一, 次周一)（本地时区）。"""
    today = now.astimezone(LOCAL_TZ).date()
    monday = today - timedelta(days=today.weekday())
    return monday - timedelta(days=7), monday


def classify(findings: Iterable[FddFindingListItem], start: date, end: date) -> ReportSummary:
    """§4.4 分类判据：effective_end = COALESCE(resolved_at, ignored_at)。

    new = first ∈ period；resolved = resolved_at ∈ period；
    persisting = first < end ∧（effective_end 为空 ∨ effective_end ≥ end）。
    health_ranking = 期末未决按 Σ weights[severity] 降序 top10（生成时快照语义）。
    """
    s = datetime.combine(start, time.min, tzinfo=LOCAL_TZ)
    e = datetime.combine(end, time.min, tzinfo=LOCAL_TZ)
    new = resolved = persisting = 0
    new_by_sev = {sev: 0 for sev in SEVERITIES}
    open_by_sev = {sev: 0 for sev in SEVERITIES}
    open_score: dict[str, int] = {}
    open_count: dict[str, int] = {}
    eq_info: dict[str, tuple[str, str]] = {}  # eq_id → (name, type)

    for f in findings:
        first = f.first_detected_at
        eff_end = f.resolved_at or f.ignored_at
        if s <= first < e:
            new += 1
            new_by_sev[f.severity] = new_by_sev.get(f.severity, 0) + 1
        if f.resolved_at is not None and s <= f.resolved_at < e:
            resolved += 1
        is_open_at_end = first < e and (eff_end is None or eff_end >= e)
        if is_open_at_end:
            persisting += 1
            open_by_sev[f.severity] = open_by_sev.get(f.severity, 0) + 1
            open_score[f.equipment.id] = open_score.get(f.equipment.id, 0) + SEVERITY_WEIGHTS.get(
                f.severity, 0
            )
            open_count[f.equipment.id] = open_count.get(f.equipment.id, 0) + 1
            name = f.equipment.name or f.equipment.local_id or f.equipment.id[:8]
            eq_info[f.equipment.id] = (name, f.equipment.equipment_type)

    ranking = sorted(
        (
            HealthRankEntry(
                equipment_id=eq_id,
                equipment_name=eq_info[eq_id][0],
                equipment_type=eq_info[eq_id][1],
                open_count=open_count[eq_id],
                weighted_score=score,
            )
            for eq_id, score in open_score.items()
        ),
        key=lambda h: (-h.weighted_score, h.equipment_id),
    )[:HEALTH_RANKING_MAX]
    return ReportSummary(
        counts=ReportCounts(new=new, resolved=resolved, persisting=persisting),
        new_by_severity=new_by_sev,
        open_by_severity=open_by_sev,
        health_ranking=list(ranking),
    )


class FddReportGenerator:
    def __init__(
        self,
        platform: PlatformClient,
        snapshot: SnapshotService,
        rules: tuple[Rule, ...],
        thresholds: ThresholdStore,
    ) -> None:
        self._platform = platform
        self._snapshot = snapshot
        self._rules = rules
        self._thresholds = thresholds

    async def generate(
        self, period_type: PeriodType, now: datetime | None = None
    ) -> list[FddReportSubmission]:
        """生成前一期的全部楼宇报告并提交；返回提交载荷（同期重生成 = api upsert）。"""
        now = now or datetime.now().astimezone()
        start, end = previous_day(now) if period_type == "day" else previous_week(now)
        algo_version = fdd_algo_version(self._rules, self._thresholds)
        snap = await self._snapshot.ensure_loaded()
        buildings = sorted({e.building_id for e in snap.equipments if e.building_id is not None})
        subs: list[FddReportSubmission] = []
        job = "fdd_report_day" if period_type == "day" else "fdd_report_week"
        with mt.JOB_DURATION_MS.labels(job=job).time():
            for b in buildings:
                findings = await self._collect(b, start, end)
                sub = FddReportSubmission(
                    building_id=b,
                    period_type=period_type,
                    period=ReportPeriod(start=start.isoformat(), end=end.isoformat()),
                    summary=classify(findings, start, end),
                    algo_version=algo_version,
                )
                await self._platform.submit_fdd_report(sub)
                subs.append(sub)
        log.info(
            "fdd_report_generated",
            period_type=period_type,
            period=f"{start}..{end}",
            buildings=len(subs),
            algo_version=algo_version,
        )
        return subs

    async def _collect(self, building_id: str, start: date, end: date) -> list[FddFindingListItem]:
        """活跃窗口分页拉取（§5.2 谓词在 api 侧；limit 上限 200，platform.md §12）。"""
        from_ = datetime.combine(start, time.min, tzinfo=LOCAL_TZ)
        to = datetime.combine(end, time.min, tzinfo=LOCAL_TZ)
        items: list[FddFindingListItem] = []
        cursor: str | None = None
        while True:
            page = await self._platform.list_fdd_findings(building_id, from_, to, cursor=cursor)
            items.extend(page.items)
            cursor = page.next_cursor
            if not cursor:
                return items
