"""节能量公式库（optimizer.md §8.2——formula_id 注册制，CODE-ST-03 不散落）。

统一口径：电功率节省（kW_e）、当前工况点、稳态估计（过渡过程损失由保守系数
吸收）。公式变更 = minor 发版（§2）；新增公式走 PR 注册。
E2/E3 的单机仿射模型 P(Q)=P0+m·Q：近 7 天 telemetry_1h 在运窗最小二乘拟合
（引擎装配，流批分离的「批」侧）；样本 < 24h 或 R² < 0.7 → 额定两点线性化
（怠机功率比锚点，f_param 降档由调用方处理）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from algo.optimizer.config import EffectiveConfig


@dataclass(frozen=True)
class AffineModel:
    """P(Q) = p0_kw + m_kw_per_kwth（单机仿射；fallback=True 表示额定线性化）。"""

    p0_kw: float
    m_kw_per_kwth: float
    fallback: bool = False

    def at(self, q_kw_th: float) -> float:
        return self.p0_kw + self.m_kw_per_kwth * q_kw_th


def fit_affine(
    samples: list[tuple[float, float]], *, min_samples: int = 24, min_r2: float = 0.7
) -> AffineModel | None:
    """(Q, P) 最小二乘拟合。样本不足 / Q 零方差 / R² 低 → None（调用方走额定线性化）。"""
    if len(samples) < min_samples:
        return None
    n = len(samples)
    sx = sum(q for q, _ in samples)
    sy = sum(p for _, p in samples)
    mx, my = sx / n, sy / n
    sxx = sum((q - mx) ** 2 for q, _ in samples)
    if sxx <= 1e-12:  # Q 恒定（零方差）：斜率不可辨识
        return None
    sxy = sum((q - mx) * (p - my) for q, p in samples)
    m = sxy / sxx
    p0 = my - m * mx
    ss_res = sum((p - (p0 + m * q)) ** 2 for q, p in samples)
    ss_tot = sum((p - my) ** 2 for _, p in samples)
    if ss_tot <= 1e-12:
        return None  # P 恒定：模型无信息量
    r2 = 1.0 - ss_res / ss_tot
    if r2 < min_r2:
        return None
    if p0 < 0:  # 物理防御：负截距外推出「停机省负电」，弃用拟合
        return None
    return AffineModel(p0_kw=p0, m_kw_per_kwth=m)


def rated_linearize(
    rated_cooling_capacity_kw: float,
    rated_input_power_kw: float,
    min_unload_ratio: float,
    idle_power_ratio: float,
) -> AffineModel:
    """额定两点线性化（§8.2 E2 回落路径）：两点 (u·Q_rated, idle_ratio·P_rated)
    与 (Q_rated, P_rated) 连线——怠机功率比锚点是保守选择（文献 0.25–0.4 下段）。"""
    q1 = min_unload_ratio * rated_cooling_capacity_kw
    p1 = idle_power_ratio * rated_input_power_kw
    m = (rated_input_power_kw - p1) / max(1e-9, rated_cooling_capacity_kw - q1)
    p0 = p1 - m * q1
    return AffineModel(p0_kw=max(0.0, p0), m_kw_per_kwth=m, fallback=True)


# ── 公式注册表（formula_id → 实现；签名统一，参数经 cfg 取默认可覆盖）────────


def e1_temp_reset(step_c: float, p_chiller_now_kw: float, sensitivity_pct_per_c: float) -> float:
    """E1：sensitivity × ΔT_set × P_now（R1；R4 冷凝侧复用，灵敏度参数独立）。"""
    return sensitivity_pct_per_c * step_c * p_chiller_now_kw


def e2_stage_down(model_stop: AffineModel, model_remain: AffineModel, q_stop_kw_th: float) -> float:
    """E2：P0_s + (m_s − m_r) × Q_s（停掉高空载功率机组，正常为正）。"""
    return model_stop.p0_kw + (model_stop.m_kw_per_kwth - model_remain.m_kw_per_kwth) * q_stop_kw_th


def e3_stage_split(p_now_kw: float, models_after: list[tuple[AffineModel, float]]) -> float:
    """E3：分载前后模型差（R3，允许为负——保护成本诚实上报）。
    models_after = 分载后各在运单机 (模型, Q_i′) 清单。"""
    p_after = sum(model.at(q) for model, q in models_after)
    return p_now_kw - p_after


def e4_condenser(
    step_c: float,
    p_chiller_now_kw: float,
    cw_sensitivity_pct_per_c: float,
    fan_power_now_kw: float,
    fan_penalty_pct_per_c: float,
) -> float:
    """E4：冷凝侧收益 − 风机惩罚（净额为正才出提案，§6.4）。"""
    fan_penalty = fan_power_now_kw * min(1.0, step_c * fan_penalty_pct_per_c)
    return cw_sensitivity_pct_per_c * step_c * p_chiller_now_kw - fan_penalty


class _Registry:
    """formula_id 注册面（§8.4 evidence 记公式标识；PR 注册制）。"""

    def __init__(self) -> None:
        self._formulas: dict[str, Callable[..., float]] = {}

    def register(self, formula_id: str, fn: Callable[..., float]) -> None:
        if formula_id in self._formulas:
            msg = f"formula_id 重复注册: {formula_id}"
            raise ValueError(msg)
        self._formulas[formula_id] = fn

    def require(self, formula_id: str) -> Callable[..., float]:
        if formula_id not in self._formulas:
            msg = f"未注册的 formula_id: {formula_id}"
            raise KeyError(msg)
        return self._formulas[formula_id]

    def ids(self) -> frozenset[str]:
        return frozenset(self._formulas)


REGISTRY = _Registry()
REGISTRY.register("E1_temp_reset", e1_temp_reset)
REGISTRY.register("E2_stage_down", e2_stage_down)
REGISTRY.register("E3_stage_split", e3_stage_split)
REGISTRY.register("E4_condenser", e4_condenser)


def strategy_cap(cfg: EffectiveConfig) -> float:
    """策略置信度上限（§6 各表；配置可覆盖）。"""
    return cfg.p("confidence_cap", 0.8)
