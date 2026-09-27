"""FDD 开放发现抑制（optimizer.md §6.7——R1 守卫的进程内状态视图）。

R1 守卫：系统内任一冷机存在开放 `chiller.delta_t_low` 发现 → 禁止再抬温度
（温差进一步收窄会掩盖故障征兆）。视图直接读同进程 FDD Hysteresis 的活跃面
（algo.md §1.4 单服务多任务的天然便利）；FDD 引擎与优化器共享同一实例
（main.py 装配保证）。快照侧无 FDD 状态可读（internal 面无此端点，DM §2
禁 PG 直连）——重启清零的迟滞面是 MVP 口径，FDD 确认窗内优化器可能先行一步，
代价由 advisory 人工确认 + 闸门两层防抖吸收（§6 约定）。
"""

from __future__ import annotations

from algo.fdd.hysteresis import Hysteresis

SUPPRESSED_RULE = "chiller.delta_t_low"  # §6.1 R1 守卫点名的规则


class FddSuppressView:
    """hysteresis 活跃面的只读投影。"""

    def __init__(self, hysteresis: Hysteresis | None = None) -> None:
        self._hysteresis = hysteresis

    def bind(self, hysteresis: Hysteresis) -> None:
        """装配期绑定（main.py / 测试装配）。"""
        self._hysteresis = hysteresis

    def has_open_finding(self, equipment_ids: list[str], rule_key: str = SUPPRESSED_RULE) -> bool:
        h = self._hysteresis
        if h is None:
            return False  # FDD 未装配（纯优化器部署形态）——无抑制面
        return any(h.is_active(eq, rule_key) for eq in equipment_ids)

    def has_any_open(self, equipment_id: str) -> bool:
        """该设备任一规则存在活跃发现（R3 standby 可加判据，§6.3）。"""
        h = self._hysteresis
        if h is None:
            return False
        return len(h.active_rules(equipment_id)) > 0
