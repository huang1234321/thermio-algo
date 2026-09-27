"""信封 / FDD 发现 / 报告 wire 模型（algo.md §8.2/§8.3；modules/M6-fdd.md §4）。

取值零自造：
- severity 五级 = ddl.md §9.2 CHECK（= platform.md §6.3 ALARM_SEVERITIES）；
- FDD 发现状态 open/resolved/ignored、报告期型 day/week = ddl.md §9.2
  （= platform.md §6.3 FDD_FINDING_STATUSES / FDD_REPORT_PERIOD_TYPES）。
- id/tenant_id/status 等一律 api 侧维护：algo 报文出现即被 api 拒（M6-fdd §3.2），
  本侧模型按 algo intent 建模、不含这些列。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Severity = Literal["info", "warning", "minor", "major", "critical"]
FindingStatus = Literal["open", "resolved", "ignored"]
PeriodType = Literal["day", "week"]


# ── FDD 发现提交（POST /internal/fdd/findings，批量 upsert）──────────────────


class FindingHit(BaseModel):
    """单条命中（活跃 upsert 键 = (tenant, equipment_id, rule_key)，api 侧解析租户）。"""

    equipment_id: str
    rule_key: str
    severity: Severity
    title: str
    evidence: dict[str, Any]
    suggested_action: str | None = None
    first_detected_at: datetime
    last_detected_at: datetime


class FindingCleared(BaseModel):
    """条件消除（api 侧置 resolved + resolved_at；无活跃行则忽略，幂等）。"""

    equipment_id: str
    rule_key: str
    cleared_at: datetime


class FddFindingsBatch(BaseModel):
    algo_version: str
    hits: list[FindingHit] = []
    cleared: list[FindingCleared] = []


# ── FDD 报告提交（POST /internal/fdd/reports；summary 钉死于 M6-fdd §4.4）────


class ReportPeriod(BaseModel):
    """闭开区间 [start, end)，YYYY-MM-DD（api 侧构造 daterange）。"""

    start: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")


class ReportCounts(BaseModel):
    new: int
    resolved: int
    persisting: int


class HealthRankEntry(BaseModel):
    equipment_id: str
    equipment_name: str
    equipment_type: str
    open_count: int
    weighted_score: int


class ReportSummary(BaseModel):
    counts: ReportCounts
    new_by_severity: dict[str, int]  # 五键全给（0 也给，前端免兜底）
    open_by_severity: dict[str, int]
    health_ranking: list[HealthRankEntry] = Field(max_length=10)


class FddReportSubmission(BaseModel):
    building_id: str
    period_type: PeriodType
    period: ReportPeriod
    summary: ReportSummary
    algo_version: str


# ── FDD 发现历史读（GET /internal/fdd/findings；形状 = M6-fdd §4.2 ListItem）──


class FindingEquipmentRef(BaseModel):
    """列表项内嵌设备摘要（快照投影）。"""

    model_config = ConfigDict(extra="ignore")

    id: str
    name: str
    local_id: str | None = None
    equipment_type: str


class FindingReviewRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    result: str
    reviewed_at: datetime


class FddFindingListItem(BaseModel):
    """消费侧读模型（extra=ignore：algo 不消费的字段原样容忍，M6-fdd §6）。"""

    model_config = ConfigDict(extra="ignore")

    id: str
    building_id: str
    equipment: FindingEquipmentRef
    rule_key: str
    severity: Severity
    status: FindingStatus
    title: str
    suggested_action: str | None = None
    algo_version: str
    first_detected_at: datetime
    last_detected_at: datetime
    resolved_at: datetime | None = None
    ignored_at: datetime | None = None
    review: FindingReviewRef | None = None
    created_at: datetime


class FddFindingList(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: list[FddFindingListItem]
    next_cursor: str | None = None
