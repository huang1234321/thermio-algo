"""候选仲裁与提交节流（optimizer.md §7——算法侧自律，进程内存，重启清零）。

闸门 3（6 次/h）与闸门 4（排队）在中台侧；algo 侧不依赖「读 pending proposal」
（internal 面无此端点，DM §2 禁 PG 直连）——用本地提交记忆做前置节流，重启清零
的代价 = 至多多一张卡（§1.4 无状态纪律内的可接受降级）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.optimizer.strategies.base import AdvisoryDraft

log = get_logger(__name__)


@dataclass
class MemoryCell:
    last_value: float
    last_submitted_at: datetime
    expires_at: datetime


class SubmissionMemory:
    """{point_id: {last_value, last_submitted_at, expires_at}}（§7.3）。

    提交成功（201）即写入；重启清零。不消费 thermio.control.executed 做状态回写
    （契约未定稿不预实现；定稿后以其 point_id + value_effective 刷新，§14-1）。
    """

    def __init__(self) -> None:
        self._cells: dict[int, MemoryCell] = {}

    def record(
        self, point_id: int, value: float, submitted_at: datetime, expires_at: datetime
    ) -> None:
        self._cells[point_id] = MemoryCell(value, submitted_at, expires_at)

    def cell(self, point_id: int) -> MemoryCell | None:
        return self._cells.get(point_id)

    def in_cooldown(self, point_id: int, *, now: datetime) -> bool:
        c = self._cells.get(point_id)
        return c is not None and now < c.expires_at

    def __len__(self) -> int:
        return len(self._cells)

    def reset(self) -> None:
        """清空（测试/场景切换用；运行期语义 = 进程重启清零，§7.3）。"""
        self._cells.clear()


@dataclass
class Arbitrated:
    """一轮仲裁结果（候选 + 各去向计数——指标与测试断言面）。"""

    candidates: list[AdvisoryDraft]
    emitted: int = 0
    deduped: int = 0
    throttled: int = 0
    floored: int = 0


def arbitrate(
    drafts: Sequence[AdvisoryDraft],
    *,
    memory: SubmissionMemory,
    now: datetime,
    defaults_min_saving_kw: float,
    deadband_c: float,
) -> Arbitrated:
    """单轮仲裁：噪声下限 → 单点单条 → 冷却窗/值死区。

    - min_expected_saving_kw：低于下限不出提案（R3 豁免——exempt_min_saving）；
    - 单点单条：同 point 多候选按 expected_saving_kw 降序取首条（§7.1；
      跨点不互斥——R1 抬温 + R2 停机作用于不同点允许同轮并存）；
    - 冷却窗：point 出提案后至 expires_at 前不重发（旧卡过期前不重发）；
    - 值死区：新目标值与上次提交值差 < deadband（温度 0.5°C / enable 0）→ 跳过。
    """
    result = Arbitrated(candidates=[])
    by_point: dict[int, list[AdvisoryDraft]] = {}
    for d in drafts:
        exempt = bool(d.exempt_min_saving)
        if not exempt and d.expected_saving_kw < defaults_min_saving_kw:
            result.floored += 1
            mt.OPTIMIZER_PROPOSALS_TOTAL.labels(strategy=d.strategy_id, result="throttled").inc()
            continue
        by_point.setdefault(d.point_id, []).append(d)
    for point_id, group in by_point.items():
        pick = max(group, key=lambda d: d.expected_saving_kw)
        dropped = len(group) - 1
        result.deduped += dropped
        for _ in range(dropped):
            mt.OPTIMIZER_PROPOSALS_TOTAL.labels(strategy=pick.strategy_id, result="deduped").inc()
        if memory.in_cooldown(point_id, now=now):
            result.throttled += 1
            mt.OPTIMIZER_PROPOSALS_TOTAL.labels(strategy=pick.strategy_id, result="throttled").inc()
            continue
        cell = memory.cell(point_id)
        if cell is not None and abs(pick.value - cell.last_value) < deadband_for(pick, deadband_c):
            result.throttled += 1
            mt.OPTIMIZER_PROPOSALS_TOTAL.labels(strategy=pick.strategy_id, result="throttled").inc()
            continue
        result.candidates.append(pick)
        result.emitted += 1
    return result


def deadband_for(draft: AdvisoryDraft, deadband_c: float) -> float:
    """值死区取值：温度类用配置死区；0/1 命令量死区为 0（任何变化都显著）。"""
    return 0.0 if draft.target_quantity == "unit_enable" else deadband_c


def record_submitted(
    memory: SubmissionMemory,
    drafts: Sequence[AdvisoryDraft],
    *,
    now: datetime,
    ttl_min: int,
) -> None:
    """提交成功（201）后回写记忆（冷却窗 = expires_at，§7.2）。"""
    ttl = timedelta(minutes=ttl_min)
    for d in drafts:
        memory.record(d.point_id, d.value, now, now + ttl)
