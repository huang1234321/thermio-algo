"""冷机规则（首批规则集，algo.md §7.7——判据骨架的落地与扩充）。"""

from __future__ import annotations

from algo.fdd.base import (
    Rule,
    RuleContext,
    RuleOutcome,
    ThresholdSet,
    build_evidence,
)
from algo.fdd.rules.common import (
    equipment_label,
    participating_points,
    rated_power_kw,
    require_avg,
    require_running,
)

KW_CHW_SUPPLY = "chw_supply_temp"
KW_CHW_RETURN = "chw_return_temp"
KW_POWER = "power"
KW_RUN = "run_status"


class ChillerDeltaTLow(Rule):
    """供回水温差持续偏低（蒸发器/水系统故障征兆；algo.md §7.7 机制样例一）。"""

    rule_id = "chiller.delta_t_low"
    equipment_type = "chiller"
    required_quantities = frozenset({KW_CHW_SUPPLY, KW_CHW_RETURN, KW_POWER, KW_RUN})
    default_severity = "warning"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CHW_SUPPLY)
        ret = require_avg(ctx, KW_CHW_RETURN)
        power = require_avg(ctx, KW_POWER)
        rated = rated_power_kw(ctx)
        delta_t = ret - supply
        load_ratio = power / rated
        delta_t_min = thr.p("delta_t_min_c", 1.2)
        load_min = thr.p("load_ratio_min", 0.3)
        if load_ratio < load_min or delta_t >= delta_t_min:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}供回水温差持续低于 {delta_t_min:g}°C"
                f"（ΔT={delta_t:.1f}°C，负荷 {load_ratio:.0%}）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "delta_t_avg_c": round(delta_t, 2),
                    "chw_supply_temp_avg_c": round(supply, 2),
                    "chw_return_temp_avg_c": round(ret, 2),
                    "load_ratio": round(load_ratio, 3),
                    "power_avg_kw": round(power, 2),
                },
            ),
            suggested_action="检查蒸发器结垢、旁通阀内漏与负荷侧阀门开度；核对水泵出力",
        )


class ChillerSupplyTempHigh(Rule):
    """冷冻水供水温度持续偏高（机组出力不足/负荷超限征兆）。"""

    rule_id = "chiller.supply_temp_high"
    equipment_type = "chiller"
    required_quantities = frozenset({KW_CHW_SUPPLY, KW_RUN})
    default_severity = "minor"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CHW_SUPPLY)
        supply_max = thr.p("supply_max_c", 9.0)
        if supply < supply_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=f"{equipment_label(ctx)}供水温度持续高于 {supply_max:g}°C（均值 {supply:.1f}°C）",
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {"chw_supply_temp_avg_c": round(supply, 2), "supply_max_c": supply_max},
            ),
            suggested_action="核对机组负荷与冷凝器散热；检查设定值被改动和水系统旁通",
        )


class ChillerPowerRatioHigh(Rule):
    """运行功率占额定比持续偏高（过载/COP 劣化征兆）。"""

    rule_id = "chiller.power_ratio_high"
    equipment_type = "chiller"
    required_quantities = frozenset({KW_POWER, KW_RUN})
    default_severity = "major"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        power = require_avg(ctx, KW_POWER)
        rated = rated_power_kw(ctx)
        ratio = power / rated
        ratio_max = thr.p("ratio_max", 0.95)
        if ratio <= ratio_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}运行功率持续达额定 {ratio:.0%}"
                f"（{power:.1f}kW / {rated:.0f}kW）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "power_avg_kw": round(power, 2),
                    "rated_power_kw": rated,
                    "ratio": round(ratio, 3),
                },
            ),
            suggested_action="检查冷凝压力/冷却水进水温度；评估压缩机磨损与制冷剂充注",
        )


class ChillerTempSensorReversed(Rule):
    """回水温度不高于供水（传感器反接/漂移征兆——ΔT 负值在物理上不成立）。"""

    rule_id = "chiller.temp_sensor_reversed"
    equipment_type = "chiller"
    required_quantities = frozenset({KW_CHW_SUPPLY, KW_CHW_RETURN, KW_RUN})
    default_severity = "warning"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CHW_SUPPLY)
        ret = require_avg(ctx, KW_CHW_RETURN)
        margin = thr.p("margin_c", 0.0)
        delta_t = ret - supply
        if delta_t > margin:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}回水温度未高于供水（ΔT={delta_t:.1f}°C），"
                "疑供回水温度传感器反接或漂移"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "delta_t_avg_c": round(delta_t, 2),
                    "chw_supply_temp_avg_c": round(supply, 2),
                    "chw_return_temp_avg_c": round(ret, 2),
                },
            ),
            suggested_action="现场核对供/回水测点安装位置与接线；比对相邻机组同名测点",
        )
