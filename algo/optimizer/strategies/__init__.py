"""策略包清单（optimizer.md §2：新增策略 = 发版动作，同 fdd/rules 纪律）。

首版策略集 4 条（§6）：冷源 3（R1/R2/R3）+ 冷却侧 1（R4）；热源模板 R5 为
P1 槽位（§6.5，供暖季接入后按同构模板扩充）。
"""

from __future__ import annotations

from algo.optimizer.strategies.base import AdvisoryDraft, Strategy
from algo.optimizer.strategies.chiller_stage import ChillerStageDown, ChillerStageUp
from algo.optimizer.strategies.chw_temp_reset import ChwTempReset
from algo.optimizer.strategies.cw_temp_reset import CwTempReset

STRATEGIES: tuple[Strategy, ...] = (
    ChwTempReset(),
    ChillerStageDown(),
    ChillerStageUp(),
    CwTempReset(),
)

__all__ = [
    "STRATEGIES",
    "AdvisoryDraft",
    "ChillerStageDown",
    "ChillerStageUp",
    "ChwTempReset",
    "CwTempReset",
    "Strategy",
]
