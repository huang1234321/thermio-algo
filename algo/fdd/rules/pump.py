"""水泵规则（chwp/cwp 双侧；同一判据经子类落到两个设备类型，rule_id 各自独立）。"""

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
    rated_power_kw,
    require_avg,
    require_running,
    series_cv,
)

KW_POWER = "power"
KW_RUN = "run_status"


class _PumpPowerRule(Rule):
    """水泵功率判据中间基类（不进清单；子类钉 rule_id / equipment_type）。"""

    rule_id = "_pump.power_ratio"
    equipment_type = "_pump"

    def _ratio_outcome(
        self,
        ctx: RuleContext,
        thr: ThresholdSet,
        *,
        power: float,
        rated: float,
        ratio: float,
        bound: float,
        breach: bool,
        direction: str,
        action: str,
    ) -> RuleOutcome | None:
        if not breach:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}运行功率持续{direction}（{ratio:.0%} 额定，"
                f"阈值 {bound:.0%}）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {
                    "power_avg_kw": round(power, 2),
                    "rated_power_kw": rated,
                    "ratio": round(ratio, 3),
                    "bound": bound,
                },
            ),
            suggested_action=action,
        )


class _PowerRatioLowRunning(_PumpPowerRule):
    """运行中功率占额定比过低（空转/传动失效/电气缺相征兆）。"""

    rule_id = "_pump.power_ratio_low_running"
    equipment_type = "_pump"
    required_quantities = frozenset({KW_POWER, KW_RUN})
    default_severity = "minor"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        power = require_avg(ctx, KW_POWER)
        rated = rated_power_kw(ctx)
        ratio = power / rated
        ratio_min = thr.p("ratio_min", 0.3)
        return self._ratio_outcome(
            ctx,
            thr,
            power=power,
            rated=rated,
            ratio=ratio,
            bound=ratio_min,
            breach=ratio < ratio_min,
            direction="偏低",
            action="检查泵体是否空转/气蚀、联轴器与传动、电机缺相；核对阀门开度",
        )


class _PowerRatioHighRunning(_PumpPowerRule):
    """运行中功率占额定比过高（过载/管路堵塞/选型失配征兆）。"""

    rule_id = "_pump.power_ratio_high_running"
    equipment_type = "_pump"
    required_quantities = frozenset({KW_POWER, KW_RUN})
    default_severity = "major"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        power = require_avg(ctx, KW_POWER)
        rated = rated_power_kw(ctx)
        ratio = power / rated
        ratio_max = thr.p("ratio_max", 1.10)
        return self._ratio_outcome(
            ctx,
            thr,
            power=power,
            rated=rated,
            ratio=ratio,
            bound=ratio_max,
            breach=ratio > ratio_max,
            direction="偏高",
            action="检查管路过滤器堵塞与阀门状态；核对变频器参数与泵选型曲线",
        )


class ChwpPowerRatioLowRunning(_PowerRatioLowRunning):
    rule_id = "chwp_pump.power_ratio_low_running"
    equipment_type = "chwp_pump"


class ChwpPowerRatioHighRunning(_PowerRatioHighRunning):
    rule_id = "chwp_pump.power_ratio_high_running"
    equipment_type = "chwp_pump"


class CwpPowerRatioLowRunning(_PowerRatioLowRunning):
    rule_id = "cwp_pump.power_ratio_low_running"
    equipment_type = "cwp_pump"


class CwpPowerRatioHighRunning(_PowerRatioHighRunning):
    rule_id = "cwp_pump.power_ratio_high_running"
    equipment_type = "cwp_pump"


class ChwpPowerUnstable(Rule):
    """运行中功率波动率超阈（气蚀/不稳定工况征兆；波动度由 cagg 桶均值序列聚合）。"""

    rule_id = "chwp_pump.power_unstable"
    equipment_type = "chwp_pump"
    required_quantities = frozenset({KW_POWER, KW_RUN})
    default_severity = "warning"

    def evaluate(self, ctx: RuleContext, thr: ThresholdSet) -> RuleOutcome | None:
        require_running(ctx)
        power = require_avg(ctx, KW_POWER)
        power_floor = thr.p("power_min_kw", 0.5)
        if power < power_floor:
            return None  # 低值段 CV 分母无意义（噪声主导），不评判
        cv = series_cv(ctx.series.get(KW_POWER, ()))
        if cv is None:
            raise InsufficientData("power 有效桶不足两桶，CV 不可算")
        cv_max = thr.p("cv_max", 0.25)
        if cv <= cv_max:
            return None
        assert ctx.window_from is not None and ctx.window_to is not None
        return RuleOutcome(
            severity=thr.severity,
            title=(
                f"{equipment_label(ctx)}运行功率波动率持续超阈（CV={cv:.0%}，阈值 {cv_max:.0%}）"
            ),
            evidence=build_evidence(
                participating_points(ctx, self.required_quantities),
                ctx.window_from,
                ctx.window_to,
                {"power_cv": round(cv, 3), "cv_max": cv_max, "power_avg_kw": round(power, 2)},
            ),
            suggested_action="检查吸入侧气蚀（NPSH）/管路积气与变频器参数整定",
        )
