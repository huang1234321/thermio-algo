"""confirm/clear 迟滞计数（algo.md §7.6）——与 DB 层 fdd_finding_active_uidx 同一
防风暴意图的两层实现：algo 层管节流与迟滞，DB 层管唯一性兜底。

状态机（每 (equipment_id, rule_key) 一格，进程内存，重启清零 §1.4）：

    无发现 --命中×confirm_windows--> 提交 hit（fdd_finding upsert，status=open）
    有发现(open) --未命中×clear_windows--> 提交 cleared（api 侧置 resolved）
    有发现(open) --命中--> 提交 hit（刷新 last_detected_at，upsert 幂等）
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from algo.fdd.base import RuleOutcome, ThresholdSet


@dataclass
class _Cell:
    active: bool = False
    hit_streak: int = 0
    clear_streak: int = 0
    first_detected_at: datetime | None = None


@dataclass(frozen=True)
class HitEmission:
    equipment_id: str
    rule_key: str
    outcome: RuleOutcome
    first_detected_at: datetime
    last_detected_at: datetime


@dataclass(frozen=True)
class ClearedEmission:
    equipment_id: str
    rule_key: str
    cleared_at: datetime


class Hysteresis:
    """迟滞状态机；update() 单轮单格推进，返回本轮排放（None = 无动作）。"""

    def __init__(self) -> None:
        self._cells: dict[tuple[str, str], _Cell] = {}

    def update(
        self,
        equipment_id: str,
        rule_key: str,
        outcome: RuleOutcome | None,
        thr: ThresholdSet,
        now: datetime,
    ) -> HitEmission | ClearedEmission | None:
        cell = self._cells.setdefault((equipment_id, rule_key), _Cell())
        if outcome is not None:
            cell.hit_streak += 1
            cell.clear_streak = 0
            if not cell.active and cell.hit_streak >= thr.confirm_windows:
                cell.active = True
                cell.first_detected_at = now
                return HitEmission(
                    equipment_id=equipment_id,
                    rule_key=rule_key,
                    outcome=outcome,
                    first_detected_at=now,
                    last_detected_at=now,
                )
            if cell.active:
                # 持续命中：刷新 last_detected_at（不产生新行，ddl.md §9.2 upsert 语义）
                assert cell.first_detected_at is not None
                return HitEmission(
                    equipment_id=equipment_id,
                    rule_key=rule_key,
                    outcome=outcome,
                    first_detected_at=cell.first_detected_at,
                    last_detected_at=now,
                )
            return None  # 未满 confirm_windows：蓄数中
        # 未命中
        cell.hit_streak = 0
        if not cell.active:
            cell.clear_streak = 0
            return None
        cell.clear_streak += 1
        if cell.clear_streak >= thr.clear_windows:
            cell.active = False
            cell.clear_streak = 0
            cell.first_detected_at = None
            return ClearedEmission(equipment_id=equipment_id, rule_key=rule_key, cleared_at=now)
        return None  # 蓄 clear 数中（防振荡；clear > confirm 是有意不对称）

    def is_active(self, equipment_id: str, rule_key: str) -> bool:
        return self._cells.get((equipment_id, rule_key), _Cell()).active

    def active_rules(self, equipment_id: str) -> set[str]:
        """该设备当前全部活跃规则键（optimizer.md §6.7 standby 无开放故障判据）。"""
        return {
            rule_key
            for (eq, rule_key), cell in self._cells.items()
            if eq == equipment_id and cell.active
        }

    def reset(self) -> None:
        """重启语义（§1.4）：清零（confirm 侧最多延迟一个确认窗，clear 侧重新计数）。"""
        self._cells.clear()
