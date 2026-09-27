"""寻优引擎（optimizer.md §1——optimize_15m 槽位的填充）。

一轮（evaluation_ts 对齐到上一个已完结的 15min 桶边界，调度纪律沿 algo.md §3）：
装配 ctx（latest + cagg 窗口 + 天气 + 预测）→ 逐系统逐策略纯函数求值 →
arbiter 仲裁 → 信封自检补全 → ProposalSubmitter 提交（201 即止）。

失败语义沿 algo.md §8.1：单系统/单策略异常 = 本轮跳过该单元（ERROR + 指标，
不中断其他）；整轮提交失败按有界重试后丢弃（宁缺毋滥——15min 后重评估自愈）。

scope 口径（MVP）：快照 wire（algo.md §6.2）不含 hvac_system.system_type——
冷源系统 = 含 equipment_type='chiller' 的 system_id 子树（cooling_water 系统
无冷机自然排除，语义等价 §4.2 的 system_type 过滤）；snapshot wire 增列
systems 为开放项（增列后切回字面过滤，零结构变更）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from statistics import fmean, pstdev
from typing import Any, Protocol

from algo.fdd.base import BucketRow
from algo.kafka.consumer import LatestCache
from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.optimizer.arbiter import Arbitrated, SubmissionMemory, arbitrate, record_submitted
from algo.optimizer.config import OptimizerConfigStore
from algo.optimizer.context import (
    ChillerRated,
    ChillerUnitView,
    EquipmentPointsView,
    LoadForecastView,
    OptimizerContext,
    PlantDerived,
    PlantView,
    WeatherView,
    wet_bulb_c,
)
from algo.optimizer.forecast_store import (
    ForecastStore,
    PersistenceInput,
    build_persistence_view,
)
from algo.optimizer.saving import fit_affine, rated_linearize
from algo.optimizer.strategies import STRATEGIES
from algo.optimizer.strategies.base import AdvisoryDraft, Strategy
from algo.optimizer.suppress import FddSuppressView
from algo.proposal.envelope import ProposalAction, ProposalEnvelope, ProposalTarget
from algo.proposal.producer import ProposalSubmitter, new_proposal_id
from algo.semantics.snapshot import SnapshotService
from algo.semantics.types import AssetSnapshot, PointView
from algo.tsdb.client import TsdbClient
from algo.tsdb.windows import QUALITY_BIT_UNIT_UNCONVERTED, fetch_buckets
from algo.versioning import optimizer_algo_version

log = get_logger(__name__)

BUCKET_15M = timedelta(minutes=15)
WEATHER_MAX_AGE = timedelta(hours=3)  # §3.4：obs 距 evaluation_ts 超 3h → weather=None
AFFINE_FIT_DAYS = 7  # §8.2 E2：近 7 天 telemetry_1h 在运窗拟合
AFFINE_MIN_SAMPLES = 24  # < 24h 样本 → 额定两点线性化（f_param 降档在置信度面）

RUN_STATUS_TEXT: dict[str, float] = {
    "running": 1.0,
    "on": 1.0,
    "1": 1.0,
    "start": 1.0,
    "stopped": 0.0,
    "off": 0.0,
    "0": 0.0,
    "stop": 0.0,
}

TARGET_QUANTITIES = frozenset(
    {"chw_supply_temp_setpoint", "cw_supply_temp_setpoint", "unit_enable"}
)


def align_to_15m(now: datetime) -> datetime:
    """对齐到**上一个已完结**的 15min 桶边界（右开端点 = evaluation_ts）。"""
    now = now.astimezone()
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    buckets = (now.astimezone(UTC) - epoch) // BUCKET_15M
    return (epoch + buckets * BUCKET_15M).astimezone(now.tzinfo)


class WeatherObsReader(Protocol):
    """weather_actual 最近观测的读取面（实现：algo.weather.job.WeatherService）。"""

    async def latest_obs(self) -> Any: ...


class WeatherViewReader:
    """WeatherObs → WeatherView 适配（湿球应用层推导 + 3h 新鲜度，§3.4）。"""

    def __init__(self, reader: WeatherObsReader) -> None:
        self._reader = reader

    async def latest_view(self, evaluation_ts: datetime) -> WeatherView | None:
        obs = await self._reader.latest_obs()
        if obs is None or obs.temp_c is None or obs.rh_pct is None:
            return None
        if evaluation_ts - obs.obs_ts > WEATHER_MAX_AGE:
            return None
        return WeatherView(
            station_id=obs.station_id,
            obs_ts=obs.obs_ts,
            temp_c=obs.temp_c,
            rh_pct=obs.rh_pct,
            wet_bulb_c=wet_bulb_c(obs.temp_c, obs.rh_pct),
        )


@dataclass
class RoundReport15m:
    """单轮寻优摘要（日志/指标/测试断言面）。"""

    evaluation_ts: datetime
    algo_version: str
    plants: int = 0
    drafts: list[AdvisoryDraft] = field(default_factory=list)
    arbitrated: Arbitrated | None = None
    envelopes: list[ProposalEnvelope] = field(default_factory=list)
    submitted: list[ProposalEnvelope] = field(default_factory=list)
    selfcheck_failed: int = 0
    errors: int = 0
    forecast_source: str = "none"


class OptimizerEngine:
    def __init__(
        self,
        tsdb: TsdbClient,
        snapshot: SnapshotService,
        config: OptimizerConfigStore,
        submitter: ProposalSubmitter,
        latest: LatestCache,
        forecast_store: ForecastStore,
        *,
        weather_reader: WeatherViewReader | None = None,
        memory: SubmissionMemory | None = None,
        strategies: tuple[Strategy, ...] = STRATEGIES,
        suppress: FddSuppressView | None = None,
    ) -> None:
        self._tsdb = tsdb
        self._snapshot = snapshot
        self._config = config
        self._submitter = submitter
        self._latest = latest
        self._forecast = forecast_store
        self._weather = weather_reader
        self._memory = memory or SubmissionMemory()
        self._strategies = strategies
        self._suppress = suppress
        self._round_weather: WeatherView | None = None  # 单轮缓存（§3.4 单查）

    @property
    def memory(self) -> SubmissionMemory:
        return self._memory

    async def run_round(self, now: datetime | None = None) -> RoundReport15m:
        with mt.JOB_DURATION_MS.labels(job="optimize_15m").time():
            return await self._run_round_inner(now or datetime.now().astimezone())

    async def _run_round_inner(self, now: datetime) -> RoundReport15m:
        self._config.maybe_reload()
        evaluation_ts = align_to_15m(now)
        report = RoundReport15m(evaluation_ts=evaluation_ts, algo_version=optimizer_algo_version())
        snap = await self._snapshot.ensure_loaded()
        groups = _group_by_system(snap)
        point_lookup = {p.point_id: p for p in snap.points}
        window = timedelta(minutes=self._config.defaults.window_min)
        raw_buckets = await fetch_buckets(
            self._tsdb,
            [p.point_id for pts in groups.values() for p in pts],
            evaluation_ts - window,
            evaluation_ts,
        )
        weather = await self._weather_view(evaluation_ts)  # 单轮单查（§3.4）
        for system_id, points in groups.items():
            try:
                plant = await self._assemble_plant(
                    snap, system_id, points, raw_buckets, evaluation_ts
                )
            except Exception:
                report.errors += 1
                log.exception("optimizer_plant_assembly_error", system_id=system_id)
                continue
            if plant is None:
                continue
            report.plants += 1
            ctx = OptimizerContext(
                evaluation_ts=evaluation_ts,
                plants=[plant],
                weather=weather,
                forecast=self._forecast_view(plant, evaluation_ts, report),
                cfg=self._config,
            )
            for strategy in self._strategies:
                cfg = self._config.effective(strategy.strategy_id, plant.building_id or None)
                applicable = self._applicable(strategy, plant)
                mt.OPTIMIZER_STRATEGY_APPLICABLE.labels(
                    strategy=strategy.strategy_id, system_id=system_id
                ).set(1.0 if applicable else 0.0)
                if not applicable or not cfg.enabled:
                    continue
                try:
                    drafts = strategy.evaluate(ctx, plant, cfg)
                except Exception:
                    report.errors += 1
                    log.exception(
                        "optimizer_strategy_error",
                        strategy_id=strategy.strategy_id,
                        system_id=system_id,
                    )
                    continue
                for d in drafts:
                    mt.OPTIMIZER_SAVING_ESTIMATED_KW.labels(strategy=d.strategy_id).observe(
                        max(0.0, d.expected_saving_kw)
                    )
                report.drafts.extend(drafts)
        report.arbitrated = arbitrate(
            report.drafts,
            memory=self._memory,
            now=evaluation_ts,
            defaults_min_saving_kw=self._config.defaults.min_expected_saving_kw,
            deadband_c=self._config.defaults.deadband_c,
        )
        draft_by_proposal: dict[str, AdvisoryDraft] = {}
        for d in report.arbitrated.candidates:
            env = self._into_envelope(d, evaluation_ts, point_lookup, report)
            if env is not None:
                report.envelopes.append(env)
                draft_by_proposal[env.proposal_id] = d
        if report.envelopes:
            accepted = await self._submitter.submit(report.envelopes, now=evaluation_ts)
            report.submitted = accepted
            record_submitted(
                self._memory,
                [draft_by_proposal[e.proposal_id] for e in accepted],
                now=evaluation_ts,
                ttl_min=self._config.defaults.proposal_ttl_min,
            )
            for env in accepted:
                mt.OPTIMIZER_PROPOSALS_TOTAL.labels(
                    strategy=draft_by_proposal[env.proposal_id].strategy_id, result="emitted"
                ).inc()
        log.info(
            "optimizer_round_done",
            algo_version=report.algo_version,
            plants=report.plants,
            drafts=len(report.drafts),
            emitted=report.arbitrated.emitted if report.arbitrated else 0,
            submitted=len(report.submitted),
            selfcheck_failed=report.selfcheck_failed,
            errors=report.errors,
            forecast_source=report.forecast_source,
        )
        return report

    # ── 装配 ────────────────────────────────────────────────────────────────

    async def _assemble_plant(
        self,
        snap: AssetSnapshot,
        system_id: str,
        points: list[PointView],
        raw_buckets: dict[int, list[BucketRow]],
        evaluation_ts: datetime,
    ) -> PlantView | None:
        equip_by_id = {e.equipment_id: e for e in snap.equipments}
        system_eq = [e for e in equip_by_id.values() if e.system_id == system_id]
        chillers = [e for e in system_eq if e.equipment_type == "chiller"]
        if not chillers:  # 冷源系统 = 含冷机的系统（scope 口径，模块 docstring）
            return None
        towers = [e for e in system_eq if e.equipment_type == "cooling_tower"]
        building_id = next((e.building_id for e in chillers if e.building_id), "")
        min_good = self._config.defaults.min_good_ratio
        window = timedelta(minutes=self._config.defaults.window_min)

        def series_for(pts: list[PointView]) -> dict[str, list[BucketRow]]:
            by_qty: dict[str, list[BucketRow]] = {}
            for qty, p in _first_by_qty(pts).items():
                rows = [
                    r for r in raw_buckets.get(p.point_id, []) if r.bucket >= evaluation_ts - window
                ]
                by_qty[qty] = _gated_series(rows, min_good)
            return by_qty

        def latest_numeric(p: PointView) -> float | None:
            row = self._latest.get(p.point_id)
            if row is None:
                return None
            if row.value is not None:
                return row.value
            if p.quantity_type == "run_status" and row.value_text is not None:
                return RUN_STATUS_TEXT.get(row.value_text.strip().lower())
            return None

        async def resolve_previous(p: PointView, run_status_value: float | None) -> float | None:
            """§3.2 三级链：latest 缓存 → TSDB 点查 → run_status 回退。"""
            row = self._latest.get(p.point_id)
            if row is not None and row.value is not None:
                return row.value
            hit = await self._tsdb.latest_telemetry(p.point_id)
            if hit is not None and hit[1] is not None:
                return hit[1]
            if p.quantity_type == "unit_enable" and run_status_value is not None:
                return run_status_value
            return None

        plant_first = _first_by_qty(points)
        plant_latest: dict[str, float | None] = {}
        for qty, p in plant_first.items():
            if qty in TARGET_QUANTITIES:
                plant_latest[qty] = await resolve_previous(p, None)
            else:
                plant_latest[qty] = latest_numeric(p)

        unit_views: list[ChillerUnitView] = []
        for c in chillers:
            c_points = [p for p in points if p.equipment_id == c.equipment_id]
            rated = self._rated_for(
                c.rated_params,
                self._config.chiller_rated_fallback(building_id, c.local_id or c.equipment_id),
            )
            if rated is None:
                log.warning(
                    "optimizer_chiller_rated_missing_skip_unit",
                    equipment_id=c.equipment_id,
                    local_id=c.local_id,
                )
                continue
            u_first = _first_by_qty(c_points)
            run_status_v = (
                latest_numeric(u_first["run_status"]) if "run_status" in u_first else None
            )
            u_latest: dict[str, float | None] = {}
            for qty, p in u_first.items():
                if qty == "unit_enable":
                    u_latest[qty] = await resolve_previous(p, run_status_v)
                else:
                    u_latest[qty] = latest_numeric(p)
            unit_views.append(
                ChillerUnitView(
                    equipment_id=c.equipment_id,
                    local_id=c.local_id or c.equipment_id,
                    rated=rated,
                    points=u_first,
                    latest=u_latest,
                    series=series_for(c_points),
                )
            )
        if not unit_views:
            return None
        tower_views = [
            EquipmentPointsView(
                equipment_id=t.equipment_id,
                local_id=t.local_id or t.equipment_id,
                rated_params=dict(t.rated_params),
                points=(
                    first := _first_by_qty([p for p in points if p.equipment_id == t.equipment_id])
                ),
                latest={qty: latest_numeric(p) for qty, p in first.items()},
                series=series_for([p for p in points if p.equipment_id == t.equipment_id]),
            )
            for t in towers
        ]
        weather = self._round_weather  # 装配期复用轮级天气视图
        derived = _derive_metrics(unit_views, series_for(points), weather)
        affine = await self._affine_models(unit_views, evaluation_ts)
        return PlantView(
            system_id=system_id,
            building_id=building_id,
            chillers=unit_views,
            towers=tower_views,
            points=plant_first,
            latest=plant_latest,
            series=series_for(points),
            derived=derived,
            affine_models=affine,
            fdd_delta_t_low_open=(
                self._suppress.has_open_finding([c.equipment_id for c in chillers])
                if self._suppress
                else False
            ),
            unit_fdd_open=(
                {c.equipment_id: self._suppress.has_any_open(c.equipment_id) for c in chillers}
                if self._suppress
                else {}
            ),
        )

    def _rated_for(
        self, rated_params: dict[str, Any], fallback: dict[str, Any]
    ) -> ChillerRated | None:
        """rated_params 解析 + YAML 兜底（§6.6-2）；关键字段仍缺 → None（不硬算）。"""
        merged = {**fallback, **rated_params}

        def num(key: str) -> float | None:
            v = merged.get(key)
            return float(v) if isinstance(v, (int, float)) and v else None

        q = num("rated_cooling_capacity_kw")
        p = num("rated_input_power_kw")
        u = num("min_unload_ratio")
        if q is None or p is None or u is None:
            return None
        return ChillerRated(
            rated_cooling_capacity_kw=q,
            rated_input_power_kw=p,
            min_unload_ratio=u,
            design_delta_t_c=num("design_delta_t_c"),
            rated_cop=num("rated_cop"),
        )

    async def _weather_view(self, evaluation_ts: datetime) -> WeatherView | None:
        self._round_weather = None
        if self._weather is None:
            return None
        self._round_weather = await self._weather.latest_view(evaluation_ts)
        return self._round_weather

    def _forecast_view(
        self, plant: PlantView, evaluation_ts: datetime, report: RoundReport15m
    ) -> LoadForecastView | None:
        """§5.2 交接：Store 最新一期 → persistence 降级 → None（降级是常态路径）。"""
        view = self._forecast.latest(now=evaluation_ts)
        if view is not None and view.building_id == plant.building_id:
            report.forecast_source = view.source
            _set_forecast_gauge(view.source)
            return view
        pview = build_persistence_view(
            PersistenceInput(
                evaluation_ts=evaluation_ts,
                building_id=plant.building_id,
                load_series=plant_load_series(plant),
            )
        )
        if pview is not None:
            report.forecast_source = "persistence"
            _set_forecast_gauge("persistence")
            return pview
        report.forecast_source = "none"
        _set_forecast_gauge("none")
        return None

    async def _affine_models(
        self, units: Sequence[ChillerUnitView], evaluation_ts: datetime
    ) -> dict[str, object]:
        """E2/E3 单机仿射模型（§8.2）：7 天 telemetry_1h 拟合；失败额定线性化。

        单楼 MVP 量级（≤ 数台冷机 × 7×24 桶）单系统一次查询可承受（§5.1 批侧）。
        """
        power_ids = [u.points["power"].point_id for u in units if "power" in u.points]
        rows_1h = await fetch_1h_avgs(
            self._tsdb,
            power_ids,
            evaluation_ts - timedelta(days=AFFINE_FIT_DAYS),
            evaluation_ts,
        )
        idle = self._config.defaults.idle_power_ratio
        cop_default = self._config.defaults.cop_default
        models: dict[str, object] = {}
        for u in units:
            fallback = rated_linearize(
                u.rated.rated_cooling_capacity_kw,
                u.rated.rated_input_power_kw,
                u.rated.min_unload_ratio,
                idle,
            )
            p = u.points.get("power")
            if p is None:
                models[u.equipment_id] = fallback
                continue
            cop = u.rated.rated_cop or cop_default
            samples = [(avg * cop, avg) for _, avg in rows_1h.get(p.point_id, [])]
            models[u.equipment_id] = fit_affine(samples, min_samples=AFFINE_MIN_SAMPLES) or fallback
        return models

    def _applicable(self, strategy: Strategy, plant: PlantView) -> bool:
        """§4.2 适用性：量类型覆盖 + advisory 前置闸（§0 铁律）。"""
        qty_set = set(plant.points)
        for u in plant.chillers:
            qty_set |= set(u.points)
        if not strategy.required_quantities <= qty_set:
            return False
        target_points = [
            p
            for p in [
                *plant.points.values(),
                *(p for u in plant.chillers for p in u.points.values()),
            ]
            if p.quantity_type == strategy.target_quantity
        ]
        return any(p.control_mode == "advisory" for p in target_points)

    def _into_envelope(
        self,
        d: AdvisoryDraft,
        evaluation_ts: datetime,
        point_lookup: dict[int, PointView],
        report: RoundReport15m,
    ) -> ProposalEnvelope | None:
        """信封自检（§1 步骤 7）：目标点 advisory / previous_value 可得 / clamp 域内。"""
        if d.previous_value is None:
            report.selfcheck_failed += 1
            _selfcheck_fail(d.strategy_id)
            return None
        target = point_lookup.get(d.point_id)
        if target is None or target.control_mode != "advisory":
            report.selfcheck_failed += 1
            _selfcheck_fail(d.strategy_id)
            return None
        if (target.clamp_min is not None and d.value < target.clamp_min) or (
            target.clamp_max is not None and d.value > target.clamp_max
        ):
            report.selfcheck_failed += 1
            _selfcheck_fail(d.strategy_id)
            return None
        ttl = timedelta(minutes=self._config.defaults.proposal_ttl_min)
        return ProposalEnvelope(
            proposal_id=new_proposal_id(),
            algo=d.algo_path,
            algo_version=optimizer_algo_version(),
            target=ProposalTarget(equipment_id=d.equipment_id, point=d.target_quantity),
            action=ProposalAction(op="set", value=d.value, unit=d.unit),
            previous_value=d.previous_value,
            rationale=d.rationale,
            expected_saving_kw=d.expected_saving_kw,
            confidence=d.confidence,
            evidence=d.evidence,
            expires_at=evaluation_ts + ttl,
        )


async def fetch_1h_avgs(
    tsdb: TsdbClient, point_ids: list[int], window_from: datetime, window_to: datetime
) -> dict[int, list[tuple[datetime, float]]]:
    """telemetry_1h 单点位 (bucket, avg) 序列（E2 拟合的「批」侧读，§5.1）。"""
    if not point_ids:
        return {}
    rows = await tsdb.fetch(
        "SELECT point_id, bucket, avg FROM telemetry_1h "
        "WHERE point_id = ANY($1::bigint[]) AND bucket >= $2 AND bucket < $3 ORDER BY bucket",
        point_ids,
        window_from,
        window_to,
    )
    out: dict[int, list[tuple[datetime, float]]] = {pid: [] for pid in point_ids}
    for r in rows:
        if r["avg"] is not None:
            out[r["point_id"]].append((r["bucket"], r["avg"]))
    return out


# 信息位掩码（ingest.md §4 v1 冻结表）：bit8 Backfill 为信息位（补传标记，非坏样本）。
# cagg bad_count = count(quality <> 0) 把信息位也计入——与位表「信息位非坏」分类冲突
# （跨仓发现，记交付说明）；algo 侧以 quality_mask 判别：mask 无坏位（bits 0–7）的桶
# 视为全好，补传回放数据因此可进寻优窗口（§5.2 门控的本意）。
_QUALITY_BAD_BITS = 0x00FF  # bits 0–7（6/7 预留未置位，等价坏位面）


def _gated_series(rows: list[BucketRow], min_good_ratio: float) -> list[BucketRow]:
    """优化器窗口门控（§5.2 + 信息位判别）。"""
    out: list[BucketRow] = []
    for b in rows:
        if b.sample_count <= 0:
            continue
        if b.quality_mask & _QUALITY_BAD_BITS == 0:
            out.append(b)  # 仅信息位置位（补传）——样本全好
            continue
        good_ratio = 1.0 - (b.bad_count / b.sample_count)
        if good_ratio < min_good_ratio:
            continue
        if b.quality_mask & QUALITY_BIT_UNIT_UNCONVERTED:
            continue
        out.append(b)
    return out


def _first_by_qty(points: list[PointView]) -> dict[str, PointView]:
    out: dict[str, PointView] = {}
    for p in sorted(points, key=lambda x: x.point_id):
        out.setdefault(p.quantity_type, p)
    return out


def _group_by_system(snap: AssetSnapshot) -> dict[str, list[PointView]]:
    """system_id → 挂接点位（无系统/设备归属的点不参与，§6.1 同精神）。"""
    eq_system = {e.equipment_id: e.system_id for e in snap.equipments if e.system_id}
    grouped: dict[str, list[PointView]] = {}
    for p in snap.points:
        if p.equipment_id is None:
            continue
        sys_id = eq_system.get(p.equipment_id)
        if sys_id:
            grouped.setdefault(sys_id, []).append(p)
    return grouped


def _window_mean(series: Sequence[BucketRow]) -> float | None:
    vals = [b.avg for b in series if b.avg is not None]
    return fmean(vals) if vals else None


def plant_load_series(plant: PlantView) -> list[tuple[datetime, float]]:
    """逐桶冷负荷序列（persistence 输入；proxy 口径 = Σ 单机功率桶 × cop）。"""
    by_ts: dict[datetime, float] = {}
    for u in plant.chillers:
        cop = u.rated.cop()
        for b in u.series.get("power", []):
            if b.avg is not None:
                by_ts[b.bucket] = by_ts.get(b.bucket, 0.0) + b.avg * cop
    return sorted(by_ts.items())


def _derive_metrics(
    units: Sequence[ChillerUnitView],
    plant_series: dict[str, list[BucketRow]],
    weather: WeatherView | None,
) -> PlantDerived:
    """§3.3 派生指标（纯函数集中单测的权威层）。"""
    supply = _window_mean(plant_series.get("chw_supply_temp", []))
    ret = _window_mean(plant_series.get("chw_return_temp", []))
    delta_t = (ret - supply) if (supply is not None and ret is not None) else None
    running = [u for u in units if (u.latest.get("run_status") or 0.0) > 0.5]
    per_unit_power: dict[str, float] = {}
    for u in units:
        m = _window_mean(u.series.get("power", []))
        if m is not None:
            per_unit_power[u.equipment_id] = m
    flow = _window_mean(plant_series.get("chw_flow_rate", []))
    load_source = "power_proxy"
    load: float | None = None
    if flow is not None and delta_t is not None and flow > 0:
        load = 1.163 * flow * delta_t  # §6.7：4.187 kJ/(kg·K) 换算系数
        load_source = "flow"
    else:
        load = sum(per_unit_power.get(u.equipment_id, 0.0) * u.rated.cop() for u in running)
    rated_sum = sum(u.rated.rated_cooling_capacity_kw for u in running)
    load_ratio = (load / rated_sum) if (load is not None and rated_sum > 0) else None
    per_unit_ratio = {
        u.equipment_id: (per_unit_power.get(u.equipment_id, 0.0) * u.rated.cop())
        / u.rated.rated_cooling_capacity_kw
        for u in units
    }
    cws = _window_mean(plant_series.get("cooling_water_supply_temp", []))
    approach = (cws - weather.wet_bulb_c) if (cws is not None and weather is not None) else None
    by_ts: dict[datetime, float] = {}
    for u in units:
        cop = u.rated.cop()
        for b in u.series.get("power", []):
            if b.avg is not None:
                by_ts[b.bucket] = by_ts.get(b.bucket, 0.0) + b.avg * cop
    series_vals = [v for _, v in sorted(by_ts.items())]
    cv: float | None = None
    if len(series_vals) >= 2 and (mu := fmean(series_vals)) > 1e-9:
        cv = pstdev(series_vals) / mu
    slope: float | None = None
    ts_sorted = sorted(by_ts)
    if len(ts_sorted) >= 2:
        xs = [(t - ts_sorted[0]).total_seconds() / 60.0 for t in ts_sorted]
        ys = [by_ts[t] for t in ts_sorted]
        mx, my = fmean(xs), fmean(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        slope = (
            sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / sxx
            if sxx > 1e-12
            else 0.0
        )
    return PlantDerived(
        chw_delta_t_c=delta_t,
        load_kw_th=load,
        load_source=load_source,
        load_ratio=load_ratio,
        running_count=len(running),
        per_unit_power_kw=per_unit_power,
        per_unit_load_ratio=per_unit_ratio,
        tower_approach_c=approach,
        load_cv=cv,
        load_slope_kw_per_min=slope,
    )


def _set_forecast_gauge(source: str) -> None:
    for s in ("model", "persistence", "none"):
        mt.OPTIMIZER_FORECAST_SOURCE.labels(source=s).set(1.0 if s == source else 0.0)


def _selfcheck_fail(strategy_id: str) -> None:
    mt.OPTIMIZER_PROPOSALS_TOTAL.labels(strategy=strategy_id, result="selfcheck_failed").inc()
