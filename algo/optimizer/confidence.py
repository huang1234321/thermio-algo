"""置信度公式（optimizer.md §8.3——确定性可复算，单测钉死）。

confidence = round(cap(strategy) × Π f_i)，两位小数。连乘组合系统性偏低是
有意设计：advisory 置信度是运维确认卡片的决策参考，低估比高估安全。
v1 数值只保证单调性（输入越差值越低）与有界性（∈[0,1]），不保证概率语义
（校准为 IMPL-19 离线回测的验收产物，§14-2）。
"""

from __future__ import annotations

from dataclasses import dataclass

# f_fcst 档位表（§8.3）：仅预测消费型策略乘此项（R2/R3；R1/R4 不乘）
F_FCST_MODEL = 1.0
F_FCST_PERSISTENCE = 0.8
F_FCST_NONE = 0.6

F_DATA_MEASURED = 1.0
F_DATA_PROXY = 0.7  # proxy 负荷 / 关键量质量门控剔除

F_PARAM_TUNED = 1.0  # 试点楼覆盖段存在
F_PARAM_DEFAULT = 0.8

CV_FULL_CREDIT = 0.20  # f_stab：CV(load, 30min) ≥ 0.20 时稳定性因子归零


def f_stab(cv: float | None) -> float:
    """负荷越稳越可信：clamp(1 − CV/0.20, 0, 1)。CV 不可得（None）按中性 1.0。"""
    if cv is None:
        return 1.0
    return min(1.0, max(0.0, 1.0 - cv / CV_FULL_CREDIT))


@dataclass(frozen=True)
class ConfidenceInput:
    cap: float  # 策略级上限（§6 各表）
    f_data: float = F_DATA_MEASURED
    cv: float | None = None  # 30min 负荷变异系数
    f_param: float = F_PARAM_DEFAULT
    f_fcst: float | None = None  # None = 非预测消费型策略（不乘）


def compute(inp: ConfidenceInput) -> float:
    factors = [inp.f_data, f_stab(inp.cv), inp.f_param]
    if inp.f_fcst is not None:
        factors.append(inp.f_fcst)
    value = inp.cap
    for f in factors:
        value *= min(1.0, max(0.0, f))
    return round(min(1.0, max(0.0, value)), 2)
