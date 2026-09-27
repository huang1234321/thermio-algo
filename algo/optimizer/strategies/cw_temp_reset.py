"""R4 `cw.temp_reset` —— 冷却水供水温度下调（湿球低时回收冷凝侧收益，§6.4）。

唯一天气强依赖策略：weather=None 时直接不适用。风机余量判据要求塔风机功率
测点（required_quantities 强制）+ 塔额定风机功率（rated_params/YAML）。
净额（E4 − fan_penalty）为正才出提案。
"""

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
from algo.optimizer.saving import e4_condenser
from algo.optimizer.strategies.base import AdvisoryDraft, Strategy
from algo.versioning import optimizer_algo_version

TARGET_QTY = "cw_supply_temp_setpoint"
FAN_QTY = "tower_fan_power"
CWS_QTY = "cooling_water_supply_temp"


class CwTempReset(Strategy):
    strategy_id = "cw.temp_reset"
    algo_path = "optimizer/cw-temp-reset"
    system_types = frozenset({"chilled_water"})
    required_quantities = frozenset({CWS_QTY, FAN_QTY, TARGET_QTY, "power", "run_status"})
    target_quantity = TARGET_QTY

    def evaluate(
        self, ctx: OptimizerContext, plant: PlantView, cfg: EffectiveConfig
    ) -> list[AdvisoryDraft]:
        d = plant.derived
        target = plant.points.get(TARGET_QTY)
        if d is None or target is None:
            return []
        if ctx.weather is None:  # §6.4：唯一天气强依赖策略
            return []
        prev = plant.latest.get(TARGET_QTY)
        if prev is None:
            return []
        if d.running_count < 1 or d.load_ratio is None or d.tower_approach_c is None:
            return []
        approach_max = cfg.p("approach_max_c", 5.0)
        if d.tower_approach_c <= approach_max:
            return []
        if d.load_ratio <= 0.5:  # 高负荷才有冷凝侧收益（§6.4）
            return []
        fan_now = plant.latest.get(FAN_QTY)
        fan_rated = None
        for t in plant.towers:
            fan_rated = t.rated_params.get("rated_fan_power_kw")
            if fan_rated:
                break
        if fan_now is None or not fan_rated:
            return []
        headroom = cfg.p("fan_headroom_pct", 0.80)
        if fan_now / fan_rated >= headroom:  # 冷却塔无余量
            return []
        step = cfg.p("step_c", 1.0)
        clamp_min = target.clamp_min if target.clamp_min is not None else prev - step
        new_value = round(prev - step, 3)
        if new_value < clamp_min:  # previous_value − step ≥ clamp_min（§6.4）
            return []
        p_running = sum(d.per_unit_power_kw.values())
        cw_sens = cfg.p("cw_sensitivity_pct_per_c", 0.01)
        fan_penalty_pct = cfg.p("fan_penalty_pct_per_c", 0.50)
        saving = e4_condenser(step, p_running, cw_sens, fan_now, fan_penalty_pct)
        if saving <= 0:  # 净额为正才出提案（§6.4）
            return []
        f_param = F_PARAM_TUNED if cfg.building_id is not None else F_PARAM_DEFAULT
        f_data = F_DATA_MEASURED if d.load_source == "flow" else F_DATA_PROXY
        confidence = compute(
            ConfidenceInput(
                cap=cfg.p("confidence_cap", 0.80), f_data=f_data, cv=d.load_cv, f_param=f_param
            )
        )
        sustain_min = int(cfg.p("sustain_min", 30))
        win_from = ctx.evaluation_ts - timedelta(minutes=sustain_min)
        evidence = {
            "strategy": self.strategy_id,
            "formula_id": "E4_condenser",
            "cfg_fp8": cfg_fp8(cfg),
            "inputs": {
                "point_ids": [
                    plant.points[CWS_QTY].point_id,
                    plant.points[FAN_QTY].point_id,
                    target.point_id,
                ],
                "tower_approach_c": round(d.tower_approach_c, 2),
                "wet_bulb_c": round(ctx.weather.wet_bulb_c, 2),
                "fan_power_now_kw": round(fan_now, 1),
                "fan_rated_kw": round(float(fan_rated), 1),
                "p_chiller_now_kw": round(p_running, 1),
                "step_c": step,
            },
            "window": {"from": win_from.isoformat(), "to": ctx.evaluation_ts.isoformat()},
            "forecast": {"source": "none"},  # R4 非预测消费型（§5.3 表）
            "previous_value_source": "point_latest",
            "baseline": {"mean_p_kw": round(p_running, 1), "window": f"{sustain_min}min"},
            "algo_version": optimizer_algo_version(),
        }
        rationale = (
            f"湿球 {ctx.weather.wet_bulb_c:.1f}°C 偏低、冷却塔逼近温度 "
            f"{d.tower_approach_c:.1f}°C 超限且风机有余量，建议冷却水供水温度 "
            f"{prev:g}→{new_value:g}°C；预计净降功率 {saving:.1f} kW"
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
