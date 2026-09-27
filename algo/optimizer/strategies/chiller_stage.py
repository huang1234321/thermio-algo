"""R2/R3 `chiller.stage_down` / `chiller.stage_up` —— 冷机加减机（optimizer.md §6.2/§6.3）。

同文件共享机型仿射模型（§2 目录约定）：P(Q)=P0+m·Q，近 7 天 telemetry_1h 在运窗
拟合（引擎装配进 plant.affine_models），失败回落额定两点线性化（f_param 降档）。
L2 预测增强红线（§4.3）：预测只允许收紧/放宽判据，不允许独立触发动作——
任何提案都必须能只用实测数据解释。
"""

from __future__ import annotations

import math

from algo.optimizer.confidence import (
    F_DATA_MEASURED,
    F_DATA_PROXY,
    F_FCST_MODEL,
    F_FCST_NONE,
    F_FCST_PERSISTENCE,
    F_PARAM_DEFAULT,
    F_PARAM_TUNED,
    ConfidenceInput,
    compute,
)
from algo.optimizer.config import EffectiveConfig, cfg_fp8
from algo.optimizer.context import ChillerUnitView, OptimizerContext, PlantView
from algo.optimizer.saving import AffineModel, e2_stage_down, e3_stage_split
from algo.optimizer.strategies.base import AdvisoryDraft, Strategy
from algo.versioning import optimizer_algo_version

TARGET_QTY = "unit_enable"
SUSTAIN_RATIO = 0.8


def _model_for(plant: PlantView, unit_id: str) -> AffineModel:
    """该机组仿射模型——引擎装配保证完备（拟合成功或额定两点线性化兜底，§8.2）。"""
    model = plant.affine_models.get(unit_id)
    assert model is not None, "引擎装配必须为每台冷机注入仿射模型"
    assert isinstance(model, AffineModel)
    return model


def _f_param(cfg: EffectiveConfig) -> float:
    return F_PARAM_TUNED if cfg.building_id is not None else F_PARAM_DEFAULT


def _f_fcst(ctx: OptimizerContext) -> float:
    if ctx.forecast is None:
        return F_FCST_NONE
    return F_FCST_MODEL if ctx.forecast.source == "model" else F_FCST_PERSISTENCE


def _f_data(plant: PlantView) -> float:
    d = plant.derived
    return F_DATA_MEASURED if (d is not None and d.load_source == "flow") else F_DATA_PROXY


def _lookahead_peak(plant: PlantView, ctx: OptimizerContext) -> float | None:
    """前瞻窗峰值（§5.3）：预测优先；无预测 → 窗口趋势外推（30–60min，保守取 60min）。"""
    if ctx.forecast is not None:
        peak = ctx.forecast.peak_kw_th(from_min=30, to_min=60, now=ctx.evaluation_ts)
        if peak is not None:
            return peak
    d = plant.derived
    if d is None or d.load_kw_th is None or d.load_slope_kw_per_min is None:
        return None
    return max(0.0, d.load_kw_th + d.load_slope_kw_per_min * 60.0)


class ChillerStageDown(Strategy):
    """R2 减机（N≥2 → N−1）：「均低载」而非「一台低载」。"""

    strategy_id = "chiller.stage_down"
    algo_path = "optimizer/chiller-sequencer"
    system_types = frozenset({"chilled_water"})
    required_quantities = frozenset({"power", "run_status", TARGET_QTY})
    target_quantity = TARGET_QTY
    forecast_consumer = True  # §5.3：前瞻窗收紧判据（防振荡）

    def evaluate(
        self, ctx: OptimizerContext, plant: PlantView, cfg: EffectiveConfig
    ) -> list[AdvisoryDraft]:
        d = plant.derived
        if d is None or d.running_count < 2:
            return []
        stage_down_ratio = cfg.p("stage_down_ratio", 0.45)
        headroom = cfg.p("stage_headroom", 0.85)
        sustain_min = int(cfg.p("sustain_min", 30))
        running = [c for c in plant.chillers if (c.latest.get("run_status") or 0.0) > 0.5]
        if len(running) != d.running_count or not running:
            return []
        # 均低载 + 持续（逐桶占比 ≥ 0.8）
        if not all(
            d.per_unit_load_ratio.get(c.equipment_id, 1.0) < stage_down_ratio for c in running
        ):
            return []
        if not _sustained_low_load(plant, running, sustain_min, stage_down_ratio):
            return []
        # 前瞻：未来 30–60min 峰值满足 N−1 台承载（§5.3 收紧方向）
        peak = _lookahead_peak(plant, ctx)
        if peak is None:
            return []
        remain_capacity = _n_minus_1_capacity(running)
        if peak >= remain_capacity * headroom:
            return []
        # 停运目标：窗口功率/负荷比最差者（无流量计时 power/rated 排序，§6.2）
        target_unit = _worst_unit(plant, running)
        stop_share = target_unit.rated.rated_cooling_capacity_kw / remain_capacity
        # 吸收约束：剩余机组承接被停负荷后仍 < 1.0（§6.2 min_unload 约束的承载面）
        shifted = d.load_kw_th * stop_share if d.load_kw_th is not None else 0.0
        for c in running:
            if c.equipment_id == target_unit.equipment_id:
                continue
            after = d.per_unit_load_ratio.get(c.equipment_id, 0.0) + shifted / max(
                1e-9, c.rated.rated_cooling_capacity_kw
            )
            if after >= 1.0:
                return []
        prev = target_unit.latest.get(TARGET_QTY)
        if prev is None or prev < 0.5:  # 命令点当前值不可得/已在停位 → 不产
            return []
        q_stop = d.per_unit_load_ratio.get(target_unit.equipment_id, 0.0) * (
            target_unit.rated.rated_cooling_capacity_kw
        )
        model_stop = _model_for(plant, target_unit.equipment_id)
        model_remain = _model_for(
            plant,
            next(c.equipment_id for c in running if c.equipment_id != target_unit.equipment_id),
        )
        saving = e2_stage_down(model_stop, model_remain, q_stop)
        confidence = compute(
            ConfidenceInput(
                cap=cfg.p("confidence_cap", 0.80),
                f_data=_f_data(plant),
                cv=d.load_cv,
                f_param=_f_param(cfg),
                f_fcst=_f_fcst(ctx),
            )
        )
        n = len(running)
        rationale = (
            f"{n} 台均在 {stage_down_ratio:.0%} 以下低载运行 {sustain_min}min，"
            f"前瞻 60min 负荷 {peak:.0f} kW 可由 {n - 1} 台承载；"
            f"建议停 {target_unit.local_id}，预计降功率 {saving:.1f} kW"
        )
        evidence = {
            "strategy": self.strategy_id,
            "formula_id": "E2_stage_down",
            "cfg_fp8": cfg_fp8(cfg),
            "inputs": {
                "point_ids": sorted(p.point_id for c in running for p in c.points.values()),
                "stop_unit": target_unit.equipment_id,
                "running_count": n,
                "per_unit_load_ratio": {
                    c.local_id: round(d.per_unit_load_ratio.get(c.equipment_id, 0.0), 3)
                    for c in running
                },
                "lookahead_peak_kw_th": round(peak, 1),
                "n_minus_1_capacity_kw_th": round(remain_capacity, 1),
                "model_stop_fallback": model_stop.fallback,
            },
            "window": _window(ctx, sustain_min),
            "forecast": {
                "source": ctx.forecast.source if ctx.forecast else "trend_extrapolation",
                "next_60min_peak_kw_th": round(peak, 1),
            },
            "previous_value_source": "point_latest",
            "baseline": {
                "mean_p_kw": round(sum(d.per_unit_power_kw.values()), 1),
                "window": f"{sustain_min}min",
            },
            "algo_version": optimizer_algo_version(),
        }
        point = target_unit.points.get(TARGET_QTY)
        assert point is not None  # required_quantities 已保证
        return [
            AdvisoryDraft(
                strategy_id=self.strategy_id,
                algo_path=self.algo_path,
                equipment_id=target_unit.equipment_id,
                point_id=point.point_id,
                target_quantity=TARGET_QTY,
                value=0.0,
                previous_value=prev,
                unit=point.unit_std or "dimensionless",
                rationale=rationale,
                expected_saving_kw=round(saving, 2),
                confidence=confidence,
                evidence=evidence,
            )
        ]


class ChillerStageUp(Strategy):
    """R3 加机（保护性优先）：允许负 saving，豁免噪声下限，置信度 cap 最低。"""

    strategy_id = "chiller.stage_up"
    algo_path = "optimizer/chiller-sequencer"
    system_types = frozenset({"chilled_water"})
    required_quantities = frozenset({"power", "run_status", TARGET_QTY})
    target_quantity = TARGET_QTY
    forecast_consumer = True  # §5.3：预测持续回落 → 延后一步观察（放宽方向）

    def evaluate(
        self, ctx: OptimizerContext, plant: PlantView, cfg: EffectiveConfig
    ) -> list[AdvisoryDraft]:
        d = plant.derived
        if d is None or d.running_count < 1:
            return []
        stage_up_ratio = cfg.p("stage_up_ratio", 0.92)
        sustain_min = int(cfg.p("sustain_min", 20))
        running = [c for c in plant.chillers if (c.latest.get("run_status") or 0.0) > 0.5]
        standby = [
            c
            for c in plant.chillers
            if (c.latest.get("run_status") or 0.0) <= 0.5
            and (enable := c.latest.get(TARGET_QTY)) is not None
            and enable < 0.5
            and not plant.unit_fdd_open.get(c.equipment_id, False)
        ]
        if not running or not standby:
            return []
        max_ratio = max(d.per_unit_load_ratio.get(c.equipment_id, 0.0) for c in running)
        if max_ratio <= stage_up_ratio:
            return []
        if not _sustained_high_load(plant, running, sustain_min, stage_up_ratio):
            return []
        # 趋势：窗口负荷仍上升（斜率>0）或前瞻 30min 不回落（§6.3）
        slope = d.load_slope_kw_per_min
        fcst_30 = (
            ctx.forecast.peak_kw_th(from_min=15, to_min=45, now=ctx.evaluation_ts)
            if ctx.forecast
            else None
        )
        rising = (slope is not None and slope > 1e-9) or (
            fcst_30 is not None and d.load_kw_th is not None and fcst_30 >= d.load_kw_th - 1e-9
        )
        if not rising:
            return []
        target_unit = standby[0]  # 待机候选按装配序（快照 point_id 排序确定）
        prev = target_unit.latest.get(TARGET_QTY)
        if prev is None or prev >= 0.5:
            return []
        # E3：分载前后模型差（按额定容量比例分载；允许为负——保护成本诚实上报）
        all_units = [*running, target_unit]
        total_rated = sum(c.rated.rated_cooling_capacity_kw for c in all_units)
        total_load = (
            d.load_kw_th
            if d.load_kw_th is not None
            else sum(
                d.per_unit_load_ratio.get(c.equipment_id, 0.0) * c.rated.rated_cooling_capacity_kw
                for c in running
            )
        )
        models_after = [
            (
                _model_for(plant, c.equipment_id),
                total_load * c.rated.rated_cooling_capacity_kw / total_rated,
            )
            for c in all_units
        ]
        p_now = sum(d.per_unit_power_kw.values())
        saving = e3_stage_split(p_now, models_after)
        confidence = compute(
            ConfidenceInput(
                cap=cfg.p("confidence_cap", 0.75),
                f_data=_f_data(plant),
                cv=d.load_cv,
                f_param=_f_param(cfg),
                f_fcst=_f_fcst(ctx),
            )
        )
        worst = max(running, key=lambda c: d.per_unit_load_ratio.get(c.equipment_id, 0.0))
        worst_ratio = d.per_unit_load_ratio.get(worst.equipment_id, 0.0)
        protective = saving < 0
        delta_txt = f"{saving:+.1f}"
        rationale = (
            f"{worst.local_id} 负荷率 {worst_ratio:.0%} 持续超限且趋势上行，"
            f"存在供水温度失守风险；建议启动 {target_unit.local_id}"
            + (
                f"（保护性，预计功率变化 {delta_txt} kW）"
                if protective
                else f"（预计降功率 {saving:.1f} kW）"
            )
        )
        evidence = {
            "strategy": self.strategy_id,
            "formula_id": "E3_stage_split",
            "cfg_fp8": cfg_fp8(cfg),
            "inputs": {
                "point_ids": sorted(p.point_id for c in all_units for p in c.points.values()),
                "start_unit": target_unit.equipment_id,
                "max_per_unit_load_ratio": round(max_ratio, 3),
                "load_slope_kw_per_min": round(slope, 3) if slope is not None else None,
                "protective": protective,
            },
            "window": _window(ctx, sustain_min),
            "forecast": {
                "source": ctx.forecast.source if ctx.forecast else "none",
                "t_plus_30_45_peak_kw_th": round(fcst_30, 1) if fcst_30 is not None else None,
            },
            "previous_value_source": "point_latest",
            "baseline": {"mean_p_kw": round(p_now, 1), "window": f"{sustain_min}min"},
            "algo_version": optimizer_algo_version(),
        }
        point = target_unit.points.get(TARGET_QTY)
        assert point is not None
        return [
            AdvisoryDraft(
                strategy_id=self.strategy_id,
                algo_path=self.algo_path,
                equipment_id=target_unit.equipment_id,
                point_id=point.point_id,
                target_quantity=TARGET_QTY,
                value=1.0,
                previous_value=prev,
                unit=point.unit_std or "dimensionless",
                rationale=rationale,
                expected_saving_kw=round(saving, 2),
                confidence=confidence,
                evidence=evidence,
                exempt_min_saving=bool(cfg.params.get("exempt_min_saving", True)),
            )
        ]


def _n_minus_1_capacity(running: list[ChillerUnitView]) -> float:
    """N−1 台承载能力：停掉一台后剩余总额定（停运目标未定前取「最差者除外」保守序）。"""
    return sum(c.rated.rated_cooling_capacity_kw for c in running) - min(
        c.rated.rated_cooling_capacity_kw for c in running
    )


def _worst_unit(plant: PlantView, running: list[ChillerUnitView]) -> ChillerUnitView:
    """停运目标：窗口功率/负荷比最差者（同分按 equipment_id 确定性破平）。"""

    def key(c: ChillerUnitView) -> tuple[float, str]:
        d = plant.derived
        assert d is not None
        p = d.per_unit_power_kw.get(c.equipment_id, 0.0)
        q = d.per_unit_load_ratio.get(c.equipment_id, 0.0) * c.rated.rated_cooling_capacity_kw
        pq = p / q if q > 1e-9 else math.inf  # 单位 kW / kW_th
        return (pq, c.equipment_id)

    return max(running, key=key)


def _unit_ratio_series(plant: PlantView, unit: ChillerUnitView) -> list[tuple[float, float]]:
    """该机组 (bucket_ts, per-bucket 负荷率) 序列（power 桶 / 额定）。"""
    out: list[tuple[float, float]] = []
    for b in unit.series.get("power", []):
        if b.avg is not None:
            out.append(
                (
                    b.bucket.timestamp(),
                    b.avg * unit.rated.cop() / unit.rated.rated_cooling_capacity_kw,
                )
            )
    out.sort()
    return out


def _sustained_low_load(
    plant: PlantView, running: list[ChillerUnitView], sustain_min: int, ratio: float
) -> bool:
    need = max(1, sustain_min // 5)
    for c in running:
        series = _unit_ratio_series(plant, c)[-need:]
        if not series:
            return False
        hits = sum(1 for _, r in series if r < ratio)
        if hits / len(series) < SUSTAIN_RATIO:
            return False
    return True


def _sustained_high_load(
    plant: PlantView, running: list[ChillerUnitView], sustain_min: int, ratio: float
) -> bool:
    need = max(1, sustain_min // 5)
    best = max(
        (_unit_ratio_series(plant, c)[-need:] for c in running),
        key=lambda s: s[-1][1] if s else 0.0,
    )
    if not best:
        return False
    hits = sum(1 for _, r in best if r > ratio)
    return hits / len(best) >= SUSTAIN_RATIO


def _window(ctx: OptimizerContext, sustain_min: int) -> dict[str, str]:
    from datetime import timedelta

    return {
        "from": (ctx.evaluation_ts - timedelta(minutes=sustain_min)).isoformat(),
        "to": ctx.evaluation_ts.isoformat(),
    }
