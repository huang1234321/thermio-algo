"""Rule / RuleContext / RuleOutcome（algo.md §7.1 全文骨架——规则库唯一必须实现的 API）。

evaluate 为纯函数式求值：无 I/O、无时钟读取（窗口与最新值都在 ctx 里）——
单测零 mock（CODE-TST-02/03 的结构保证）。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from algo.semantics.types import EquipmentView, PointView


class InsufficientData(Exception):
    """数据不足（§5.2 第三态）：引擎记 insufficient 统计并跳过该轮（不产 finding、不清计数）。"""

    def __init__(self, reason: str, *, point_id: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.point_id = point_id


@dataclass(frozen=True)
class BucketRow:
    """telemetry_5min 一行的投影（ddl.md §11.2 列名一致）。"""

    point_id: int
    bucket: datetime
    avg: float | None
    min: float | None
    max: float | None
    last: float | None
    stddev: float | None
    sample_count: int
    bad_count: int
    quality_mask: int


@dataclass(frozen=True)
class ThresholdSet:
    """rules.<rule_key> 下 YAML 段解析后的生效视图（阈值语义由规则自定义）。"""

    severity: str
    window_minutes: int
    min_good_ratio: float
    confirm_windows: int
    clear_windows: int
    params: dict[str, float | int | str] = field(default_factory=dict)

    def p(self, key: str, default: float) -> float:
        """浮点参数取值（带类型默认）。"""
        v = self.params.get(key, default)
        if isinstance(v, str):
            return float(v)
        return float(v)


@dataclass(frozen=True)
class WeatherObs:
    """weather_actual 最近观测投影（§7.7 approach_high 湿球判据的数据面）。"""

    station_id: str
    obs_ts: datetime
    temp_c: float | None = None
    rh_pct: float | None = None


@dataclass(frozen=True)
class RuleContext:
    """单设备单轮求值上下文（引擎装配，规则只读）。"""

    equipment: EquipmentView
    points: dict[str, PointView]  # key = quantity_type（一设备同量多点时取首个并 WARN）
    series: dict[str, Sequence[BucketRow]]  # key = quantity_type，已过质量门控与窗口裁剪
    latest: dict[str, Any] = field(default_factory=dict)  # key = quantity_type（§4.3）
    weather: WeatherObs | None = None  # 站点最近观测（缺站点配置/无观测 = None）
    window_from: datetime | None = None  # 评估窗 [from, to)（evidence 骨架用）
    window_to: datetime | None = None


@dataclass(frozen=True)
class RuleOutcome:
    severity: str  # 五级：info/warning/minor/major/critical（ddl.md §9.2 CHECK 同源）
    title: str  # 一句话发现（fdd_finding.title，列表主文案）
    evidence: dict[str, Any]  # §7.5 结构
    suggested_action: str | None = None


def build_evidence(
    points: Sequence[PointView],
    window_from: datetime,
    window_to: datetime,
    detail: dict[str, Any],
) -> dict[str, Any]:
    """§7.5 证据骨架：points + window 固定（前端拉曲线最小充分集），detail 规则自定义。"""
    return {
        "points": [{"point_id": p.point_id, "quantity_type": p.quantity_type} for p in points],
        "window": {"from": window_from.isoformat(), "to": window_to.isoformat()},
        "detail": detail,
    }


class Rule(ABC):
    """FDD 规则基类（ADR-008：knowhow 按「设备类型 × 量类型」组织为 Python 规则类）。

    命名：rule_id = "<equipment_type>.<name>"——fdd_finding.rule_key 的取值即此，
    活跃去重键 (tenant_id, equipment_id, rule_key) 的一部分（ddl.md §9.2）。
    """

    rule_id: ClassVar[str]
    equipment_type: ClassVar[str]  # EQUIPMENT_TYPES（shared-types enums.ts 同源）
    required_quantities: ClassVar[frozenset[str]]  # QUANTITY_TYPES（同源；扩充=发版动作）
    default_severity: ClassVar[str]  # YAML 未覆盖时的兜底

    @abstractmethod
    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        """命中 → RuleOutcome；未命中 → None；数据不足 → raise InsufficientData。"""
