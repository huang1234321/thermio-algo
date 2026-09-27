"""冷却塔规则（含 weather_actual 湿球联动判据，algo.md §7.7 机制样例二）。"""

from __future__ import annotations

from algo.fdd.base import (
    InsufficientData,
    Rule,
    RuleContext,
    RuleOutcome,
    ThresholdSet,
    build_evidence,
)
from algo.fdd.rules.common import (
    equipment_label,
    participating_points,
    require_avg,
    require_running,
    wetbulb_c,
)

KW_CWS = "cooling_water_supply_temp"
KW_CWR = "cooling_water_return_temp"
KW_RUN = "run_status"


class CoolingTowerApproachHigh(Rule):
    """逼近温度偏高（出水 − 湿球实况；填料/风机/布水问题，algo.md §7.7 样例二）。"""

    rule_id = "cooling_tower.approach_high"
    equipment_type = "cooling_tower"
    required_quantities = frozenset({KW_CWS, KW_RUN})
    default_severity = "minor"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CWS)
        if ctx.weather is None:
            # 湿球判据数据面缺失 → 第三态（不硬算：缺天气不应误报/漏报）
            raise InsufficientData("无 weather_actual 最近观测，逼近温度不可算")
        wetbulb = wetbulb_c(ctx.weather)
        approach = supply - wetbulb
        approach_max = thr.p("approach_max_c", 6.0)
        if approach <= approach_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}逼近温度持续高于 {approach_max:g}°C"
                f"（出水 {supply:.1f}°C − 湿球 {wetbulb:.1f}°C = {approach:.1f}°C）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "cws_supply_avg_c": round(supply, 2),
                    "wetbulb_c": round(wetbulb, 2),
                    "approach_c": round(approach, 2),
                    "weather_station": ctx.weather.station_id,
                    "weather_obs_ts": ctx.weather.obs_ts.isoformat(),
                },
            ),
            suggested_action="检查填料结垢/破损、布水器堵塞与风机运行；核对循环水量",
        )


class CoolingTowerSupplyTempHigh(Rule):
    """出水温度持续偏高（散热能力不足征兆）。"""

    rule_id = "cooling_tower.supply_temp_high"
    equipment_type = "cooling_tower"
    required_quantities = frozenset({KW_CWS, KW_RUN})
    default_severity = "major"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CWS)
        supply_max = thr.p("supply_max_c", 37.0)
        if supply < supply_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=f"{equipment_label(ctx)}出水温度持续高于 {supply_max:g}°C（均值 {supply:.1f}°C）",
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {"cws_supply_avg_c": round(supply, 2), "supply_max_c": supply_max},
            ),
            suggested_action="检查风机全速运行/通风障碍；核对冷却水量与喷淋系统",
        )


class CoolingTowerDeltaTHigh(Rule):
    """冷却水供回温差持续偏大（流量不足/旁通泄漏征兆）。"""

    rule_id = "cooling_tower.delta_t_high"
    equipment_type = "cooling_tower"
    required_quantities = frozenset({KW_CWS, KW_CWR, KW_RUN})
    default_severity = "warning"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        supply = require_avg(ctx, KW_CWS)
        ret = require_avg(ctx, KW_CWR)
        delta_t = ret - supply
        delta_t_max = thr.p("delta_t_max_c", 8.0)
        if delta_t <= delta_t_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}冷却水供回温差持续大于 {delta_t_max:g}°C"
                f"（ΔT={delta_t:.1f}°C）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "delta_t_avg_c": round(delta_t, 2),
                    "cws_supply_avg_c": round(supply, 2),
                    "cwr_return_avg_c": round(ret, 2),
                },
            ),
            suggested_action="核对冷却水泵出力与阀门开度；检查旁通阀内漏",
        )
