"""规则包清单（algo.md §7.7）：新增规则 = 发版动作，不开放运行时注册。

新规则三件套：rules/*.py 一个类 + 本清单一行 + config/thresholds.yaml 一段
（快照测试随契约同步；量类型扩充另走 shared-types 发版，§15）。
"""

from __future__ import annotations

from algo.fdd.base import Rule
from algo.fdd.rules.chiller import (
    ChillerDeltaTLow,
    ChillerPowerRatioHigh,
    ChillerSupplyTempHigh,
    ChillerTempSensorReversed,
)
from algo.fdd.rules.cooling_tower import (
    CoolingTowerApproachHigh,
    CoolingTowerDeltaTHigh,
    CoolingTowerSupplyTempHigh,
)
from algo.fdd.rules.pump import (
    ChwpPowerRatioHighRunning,
    ChwpPowerRatioLowRunning,
    ChwpPowerUnstable,
    CwpPowerRatioHighRunning,
    CwpPowerRatioLowRunning,
)

RULES: tuple[Rule, ...] = (
    # 冷机（4）
    ChillerDeltaTLow(),
    ChillerSupplyTempHigh(),
    ChillerPowerRatioHigh(),
    ChillerTempSensorReversed(),
    # 水泵（5：chwp/cwp 功率双侧 + chwp 波动率）
    ChwpPowerRatioLowRunning(),
    ChwpPowerRatioHighRunning(),
    ChwpPowerUnstable(),
    CwpPowerRatioLowRunning(),
    CwpPowerRatioHighRunning(),
    # 冷却塔（3：逼近/高温差/出温）
    CoolingTowerApproachHigh(),
    CoolingTowerSupplyTempHigh(),
    CoolingTowerDeltaTHigh(),
)
