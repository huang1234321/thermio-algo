"""R1 `chw.temp_reset` —— 冷冻水出水温度上调（节能主路径，optimizer.md §6.1）。"""

from __future__ import annotations

from datetime import timedelta

from algo.optimizer.confidence import (
    F_DATA_MEASURED,
    F_DATA_PROXY,
    F_PARAM_DEFAULT,
    F_PARAM_TUNED,
    ConfidenceInput,
    compute,
)
from algo.optimizer.config import EffectiveConfig, cfg_fp8
from algo.optimizer.context import OptimizerContext, PlantView
from algo.optimizer.saving import e1_temp_reset
from algo.optimizer.strategies.base import AdvisoryDraft, Strategy
from algo.versioning import optimizer_algo_version

TARGET_QTY = "chw_supply_temp_setpoint"
SUSTAIN_RATIO = 0.8  # §6 约定：「持续」= 窗口内逐桶满足占比 ≥ 0.8（防单桶毛刺）


class ChwTempReset(Strategy):
    strategy_id = "chw.temp_reset"
    algo_path = "optimizer/chw-temp-reset"
    system_types = frozenset({"chilled_water"})
    required_quantities = frozenset(
        {"chw_supply_temp", "chw_return_temp", "power", "run_status", TARGET_QTY}
    )
    target_quantity = TARGET_QTY

    def evaluate(
        self, ctx: OptimizerContext, plant: PlantView, cfg: EffectiveConfig
    ) -> list[AdvisoryDraft]:
        d = plant.derived
        target = plant.points.get(TARGET_QTY)
        if d is None or target is None:
            return []
        prev = plant.latest.get(TARGET_QTY)
        if prev is None:  # §3.2：previous_value 不可得 → 不产 proposal
            return []
        if d.running_count < 1 or d.chw_delta_t_c is None or d.load_ratio is None:
            return []
        if plant.fdd_delta_t_low_open:  # §6.7 FDD 抑制（抬温会掩盖温差故障征兆）
            return []
        delta_t_low = cfg.p("delta_t_low_c", 2.0)
        load_max = cfg.p("load_ratio_max", 0.55)
        sustain_min = int(cfg.p("sustain_min", 30))
        if not d.chw_delta_t_c < delta_t_low or not d.load_ratio < load_max:
            return []
        if not _sustained_low_delta(plant, sustain_min, delta_t_low):
            return []
        step = cfg.p("step_c", 0.5)
        max_reset = cfg.p("max_total_reset_c", 1.5)
        new_value = round(prev + step, 3)
        clamp_max = target.clamp_max if target.clamp_max is not None else new_value
        if new_value > min(clamp_max, prev + max_reset):  # 步进后仍在域内（§6.1）
            return []
        p_running = sum(d.per_unit_power_kw.values())
        sensitivity = cfg.p("sensitivity_pct_per_c", 0.02)
        saving = e1_temp_reset(step, p_running, sensitivity)
        f_param = F_PARAM_TUNED if cfg.building_id is not None else F_PARAM_DEFAULT
        f_data = F_DATA_MEASURED if d.load_source == "flow" else F_DATA_PROXY
        confidence = compute(
            ConfidenceInput(
                cap=cfg.p("confidence_cap", 0.85), f_data=f_data, cv=d.load_cv, f_param=f_param
            )
        )
        win_from = ctx.evaluation_ts - timedelta(minutes=sustain_min)
        evidence = {
            "strategy": self.strategy_id,
            "formula_id": "E1_temp_reset",
            "cfg_fp8": cfg_fp8(cfg),
            "inputs": {
                "point_ids": [
                    plant.points["chw_supply_temp"].point_id,
                    plant.points["chw_return_temp"].point_id,
                    target.point_id,
                ],
                "load_ratio": round(d.load_ratio, 3),
                "chw_delta_t_c": round(d.chw_delta_t_c, 2),
                "p_chiller_now_kw": round(p_running, 1),
                "sensitivity_pct_per_c": sensitivity,
                "step_c": step,
            },
            "window": {"from": win_from.isoformat(), "to": ctx.evaluation_ts.isoformat()},
            "forecast": forecast_evidence(plant, ctx),
            "previous_value_source": "point_latest",
            "baseline": {"mean_p_kw": round(p_running, 1), "window": f"{sustain_min}min"},
            "algo_version": optimizer_algo_version(),
        }
        rationale = (
            f"负荷 {d.load_ratio:.0%}、供回温差 {d.chw_delta_t_c:.1f}°C 持续偏低，"
            f"建议出水温度 {prev:g}→{new_value:g}°C；预计降机功率 {saving:.1f} kW"
        )
        return [
            AdvisoryDraft(
                strategy_id=self.strategy_id,
                algo_path=self.algo_path,
                equipment_id=target.equipment_id or plant.chillers[0].equipment_id,
                point_id=target.point_id,
                target_quantity=TARGET_QTY,
                value=new_value,
                previous_value=prev,
                unit=target.unit_std or "degC",
                rationale=rationale,
                expected_saving_kw=round(saving, 2),
                confidence=confidence,
                evidence=evidence,
            )
        ]


def _sustained_low_delta(plant: PlantView, sustain_min: int, delta_t_low: float) -> bool:
    """sustain 窗（最近 sustain_min 分钟 = sustain_min//5 个 5min 桶）逐桶占比 ≥ 0.8。"""
    supply = _bucket_values(plant, "chw_supply_temp")
    ret = _bucket_values(plant, "chw_return_temp")
    pairs = _pair_by_bucket(supply, ret)
    if not pairs:
        return False
    recent = pairs[-max(1, sustain_min // 5) :]
    hits = sum(1 for _, s, r in recent if (r - s) < delta_t_low)
    return hits / len(recent) >= SUSTAIN_RATIO


def _bucket_values(plant: PlantView, qty: str) -> dict[float, float]:
    out: dict[float, float] = {}
    for b in plant.series.get(qty, []):
        if b.avg is not None:
            out[b.bucket.timestamp()] = b.avg
    return out


def _pair_by_bucket(
    supply: dict[float, float], ret: dict[float, float]
) -> list[tuple[float, float, float]]:
    keys = sorted(set(supply) & set(ret))
    return [(k, supply[k], ret[k]) for k in keys]


def forecast_evidence(plant: PlantView, ctx: OptimizerContext) -> dict[str, object]:
    """§8.4 evidence.forecast 段（R1 不消费预测——透明起见仍记当前源）。"""
    if ctx.forecast is None:
        return {"source": "none"}
    peak = ctx.forecast.peak_kw_th(from_min=0, to_min=60, now=ctx.evaluation_ts)
    return {
        "source": ctx.forecast.source,
        "next_60min_peak_kw_th": round(peak, 1) if peak is not None else None,
    }
