"""「设备类型 × 量类型」注册表与适用性判定（algo.md §7.2，ADR-006 二维标签路由）。

第一维：rule.equipment_type == equipment.equipment_type；
第二维：rule.required_quantities ⊆ 该设备点的 quantity_type 集——
不满足 = 该设备不适用此规则（**静默跳过，非错误**）。
快照出现未知设备类型 → WARN + 跳过该设备（UNKNOWN 兜底纪律，不崩不静默吞）。
"""

from __future__ import annotations

from dataclasses import dataclass

from algo.fdd.base import Rule
from algo.obs.logging import get_logger
from algo.semantics.types import EQUIPMENT_TYPES, EquipmentView, PointView

log = get_logger(__name__)


@dataclass(frozen=True)
class RuleBinding:
    rule: Rule
    points: frozenset[PointView]


class RuleRegistry:
    def __init__(self, rules: tuple[Rule, ...]) -> None:
        seen: set[str] = set()
        for r in rules:
            if r.rule_id in seen:
                msg = f"重复 rule_id: {r.rule_id}"
                raise ValueError(msg)
            if not r.rule_id.startswith(f"{r.equipment_type}."):
                msg = f"rule_id 命名违约（应为 <equipment_type>.<name>）: {r.rule_id}"
                raise ValueError(msg)
            seen.add(r.rule_id)
        self._rules = rules

    @property
    def rules(self) -> tuple[Rule, ...]:
        return self._rules

    def rules_for(self, equipment: EquipmentView, points: list[PointView]) -> list[RuleBinding]:
        """二维标签路由：返回 (规则, 参与点位集)。"""
        if equipment.equipment_type not in EQUIPMENT_TYPES:
            log.warning(
                "unknown_equipment_type_skip",
                equipment_id=equipment.equipment_id,
                equipment_type=equipment.equipment_type,
            )
            return []
        by_quantity: dict[str, PointView] = {}
        for p in points:  # 一设备同量多点：取首个并 WARN（§7.1 注）
            if p.quantity_type in by_quantity:
                log.warning(
                    "duplicate_quantity_type_take_first",
                    equipment_id=equipment.equipment_id,
                    quantity_type=p.quantity_type,
                    point_id=p.point_id,
                    kept=by_quantity[p.quantity_type].point_id,
                )
                continue
            by_quantity[p.quantity_type] = p
        bindings: list[RuleBinding] = []
        for rule in self._rules:
            if rule.equipment_type != equipment.equipment_type:
                continue  # 第一维不过（非错误）
            missing = rule.required_quantities - by_quantity.keys()
            if missing:
                continue  # 第二维覆盖不足 = 不适用（静默跳过）
            chosen = frozenset(by_quantity[q] for q in rule.required_quantities)
            bindings.append(RuleBinding(rule=rule, points=chosen))
        return bindings
