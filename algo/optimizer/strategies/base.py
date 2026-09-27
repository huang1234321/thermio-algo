"""Strategy 协议与 AdvisoryDraft（optimizer.md §4.1——与 fdd/base.py 同构的纯函数纪律）。

evaluate 无 I/O、无时钟读取（窗口与最新值都在 ctx 里）——单测零 mock。
AdvisoryDraft 字段与 algo.md §11.1 信封一一对应，不新增信封字段；引擎补全
（previous_value / expires_at / proposal_id）后成为 ProposalEnvelope。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar

from algo.optimizer.config import EffectiveConfig
from algo.optimizer.context import OptimizerContext, PlantView


@dataclass(frozen=True)
class AdvisoryDraft:
    """策略候选（引擎补全后成为信封；字段与 algo.md §11.1 一一对应，不新增）。"""

    strategy_id: str
    algo_path: str  # 信封 algo 字段（optimizer.md §2：optimizer/<族名>）
    equipment_id: str  # 目标设备（R1=主冷机；R2/R3=停/启目标机组；R4=系统代表设备）
    point_id: int  # 目标点位（信封自检与仲裁键）
    target_quantity: str  # 提案目标点的 quantity_type（信封 target.point）
    value: float  # 动作值（op 固定 "set"，P2-3 数值量）
    unit: str  # 目标点位 unit_std（信封 action.unit）
    rationale: str  # 中文一句话，含量化依据（M5 §6.2-Z2 全文展示）
    expected_saving_kw: float  # §8.2 公式产物（R3 允许为负：保护性动作）
    confidence: float  # §8.3 公式产物
    previous_value: float | None = None  # §3.2 链解析值（策略判据与信封同源）
    evidence: dict[str, Any] = field(default_factory=dict)  # §8.4 对账就绪骨架
    ttl_min: int = 45  # → expires_at = 生成时刻 + ttl
    exempt_min_saving: bool = False  # R3 保护性豁免噪声下限（§7.2）


class Strategy(ABC):
    """寻优策略基类（规则+模型混合分层 §4.3 的 L1 规则层实现面）。"""

    strategy_id: ClassVar[str]  # "<域>.<名>"，如 "chw.temp_reset"（evidence 与指标 label）
    algo_path: ClassVar[str]  # 信封 algo 字段，如 "optimizer/chw-temp-reset"
    system_types: ClassVar[frozenset[str]]  # 适用 hvac_system.system_type
    required_quantities: ClassVar[frozenset[str]]  # 系统级必需量（缺一 = 不适用，静默跳过）
    target_quantity: ClassVar[str]  # 提案目标点的 quantity_type
    forecast_consumer: ClassVar[bool] = False  # 预测消费型策略（f_fcst 因子适用面，§5.3）

    @abstractmethod
    def evaluate(
        self, ctx: OptimizerContext, plant: PlantView, cfg: EffectiveConfig
    ) -> list[AdvisoryDraft]:
        """纯函数求值：命中 → 候选列表；不命中 → 空列表（数据不足也返回空，
        与 FDD 的 InsufficientData 三态不同——advisory 缺数据 = 本轮不建议，非错误）。"""
