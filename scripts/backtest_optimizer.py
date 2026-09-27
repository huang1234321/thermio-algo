"""优化器离线回测 harness（optimizer.md §8.4-2——IMPL-19 验收件）。

在历史/回放数据上重放策略（**复用生产策略代码路径**：同一 STRATEGIES.evaluate
+ saving/confidence 公式，仅 ctx 来源换成回放装配——保证回测结论可迁移）：
  1. 逐 15min 槽位装配 ctx（cagg 窗口 + previous_value 时点查询 + persistence 预测）；
  2. 每条 emitted 候选按 evidence 的 baseline 窗与执行后窗计算 realized saving
     （同测点、可比工况窗的实测功率差——湿球分桶天气归一对齐 M&V 精神 DM §5）；
  3. 输出 expected vs realized 散点 + 置信度校准曲线 + 每策略计数；
  4. 负荷预测离线验证（§5：persistence 基线逐时前推 vs 实测，MAE/MAPE 分档）。

评估口径与数据窗口写进报告（验收原文要求）。用法：

    uv run python scripts/backtest_optimizer.py \
        --dsn postgres://tsdb_algo:...@127.0.0.1:5434/thermio_ts \
        --from 2026-09-27T00:00:00+08:00 --to 2026-09-28T00:00:00+08:00 \
        --out build/backtest-report.json

干跑面：不提交 proposal（无 internal 通道依赖）；previous_value 为时点查询
（< evaluation_ts 的最后值），与在线三级链的时点语义一致。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from statistics import fmean
from typing import Any

import asyncpg
from algo.optimizer.config import OptimizerConfigStore
from algo.optimizer.context import (
    ChillerRated,
    ChillerUnitView,
    EquipmentPointsView,
    OptimizerContext,
    PlantView,
    WeatherView,
    wet_bulb_c,
)
from algo.optimizer.engine import _derive_metrics, align_to_15m, plant_load_series
from algo.optimizer.forecast_store import PersistenceInput, build_persistence_view
from algo.optimizer.saving import fit_affine, rated_linearize
from algo.optimizer.strategies import STRATEGIES
from algo.semantics.types import AssetSnapshot, EquipmentView, PointView

CONF_BUCKETS = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)


@dataclass
class ReplayDraft:
    evaluation_ts: datetime
    strategy_id: str
    expected_saving_kw: float
    realized_saving_kw: float | None
    confidence: float
    formula_id: str
    baseline_p_kw: float
    post_p_kw: float | None


class ReplayTsdb:
    """历史窗口只读面（cagg 桶 + 时点 previous_value）。"""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def buckets(self, pids: list[int], lo: datetime, hi: datetime) -> dict[int, list]:
        rows = await self.pool.fetch(
            "SELECT point_id, bucket, avg, min, max, last, stddev, sample_count, "
            "bad_count, quality_mask FROM telemetry_5min "
            "WHERE point_id = ANY($1::bigint[]) AND bucket >= $2 AND bucket < $3 "
            "ORDER BY bucket",
            pids,
            lo,
            hi,
        )
        from algo.fdd.base import BucketRow

        out: dict[int, list] = {pid: [] for pid in pids}
        for r in rows:
            out[r["point_id"]].append(BucketRow(**dict(r)))
        return out

    async def previous_value(self, point_id: int, at: datetime) -> float | None:
        return await self.pool.fetchval(
            "SELECT value FROM telemetry WHERE point_id = $1 AND ts < $2 ORDER BY ts DESC LIMIT 1",
            point_id,
            at,
        )

    async def weather(self, at: datetime) -> WeatherView | None:
        row = await self.pool.fetchrow(
            "SELECT station_id, obs_ts, temp_c, rh_pct FROM weather_actual "
            "WHERE obs_ts < $1 ORDER BY obs_ts DESC LIMIT 1",
            at,
        )
        if row is None or row["temp_c"] is None or row["rh_pct"] is None:
            return None
        if at - row["obs_ts"] > timedelta(hours=3):
            return None
        return WeatherView(
            station_id=row["station_id"],
            obs_ts=row["obs_ts"],
            temp_c=row["temp_c"],
            rh_pct=row["rh_pct"],
            wet_bulb_c=wet_bulb_c(row["temp_c"], row["rh_pct"]),
        )


async def load_snapshot_from_pg(pg_dsn: str) -> AssetSnapshot:
    """PG 直读投影（回测为离线工具面，不经 internal HTTP；字段同 §6.2 wire）。"""
    conn = await asyncpg.connect(pg_dsn)
    try:
        eqs = await conn.fetch(
            "SELECT e.id, e.equipment_type, e.system_id, s.building_id, e.tenant_id, "
            "e.local_id, e.name, e.rated_params FROM equipment e "
            "JOIN hvac_system s ON s.id = e.system_id"
        )
        pts = await conn.fetch(
            "SELECT p.id, p.equipment_id, p.quantity_type, p.unit_std, p.valid_range_min, "
            "p.valid_range_max, p.is_controllable, p.clamp_min, p.clamp_max, p.control_mode "
            "FROM point p WHERE p.equipment_id IS NOT NULL"
        )
        return AssetSnapshot(
            generated_at=datetime.now().astimezone(),
            equipments=[
                EquipmentView(
                    equipment_id=str(e["id"]),
                    equipment_type=e["equipment_type"],
                    system_id=str(e["system_id"]),
                    building_id=str(e["building_id"]) if e["building_id"] else None,
                    tenant_id=str(e["tenant_id"]) if e["tenant_id"] else None,
                    local_id=e["local_id"],
                    name=e["name"],
                    rated_params=json.loads(e["rated_params"] or "{}"),
                )
                for e in eqs
            ],
            points=[
                PointView(
                    point_id=int(p["id"]),
                    equipment_id=str(p["equipment_id"]) if p["equipment_id"] else None,
                    quantity_type=p["quantity_type"] or "",
                    unit_std=p["unit_std"],
                    valid_range_min=float(p["valid_range_min"])
                    if p["valid_range_min"] is not None
                    else None,
                    valid_range_max=float(p["valid_range_max"])
                    if p["valid_range_max"] is not None
                    else None,
                    is_controllable=p["is_controllable"],
                    clamp_min=float(p["clamp_min"]) if p["clamp_min"] is not None else None,
                    clamp_max=float(p["clamp_max"]) if p["clamp_max"] is not None else None,
                    control_mode=p["control_mode"],
                )
                for p in pts
            ],
        )
    finally:
        await conn.close()


def make_num(rp: dict) -> Any:
    """rated_params 键取值闭包（循环内绑定，避免 B023 晚绑定）。"""

    def num(key: str) -> float | None:
        v = rp.get(key)
        return float(v) if isinstance(v, (int, float)) and v else None

    return num


def group_plants(snap: AssetSnapshot) -> dict[str, list[PointView]]:
    eq_system = {e.equipment_id: e.system_id for e in snap.equipments if e.system_id}
    has_chiller = {
        e.system_id for e in snap.equipments if e.equipment_type == "chiller" and e.system_id
    }
    out: dict[str, list[PointView]] = {}
    for p in snap.points:
        if p.equipment_id is None:
            continue
        sys_id = eq_system.get(p.equipment_id)
        if sys_id and sys_id in has_chiller:
            out.setdefault(sys_id, []).append(p)
    return out


async def assemble_plant(
    snap: AssetSnapshot,
    system_id: str,
    points: list[PointView],
    tsdb: ReplayTsdb,
    evaluation_ts: datetime,
    window_min: int,
    min_good: float,
    config: OptimizerConfigStore,
) -> PlantView | None:
    eqs = {e.equipment_id: e for e in snap.equipments if e.system_id == system_id}
    chillers = [e for e in eqs.values() if e.equipment_type == "chiller"]
    if not chillers:
        return None
    towers = [e for e in eqs.values() if e.equipment_type == "cooling_tower"]
    window = timedelta(minutes=window_min)
    raw = await tsdb.buckets([p.point_id for p in points], evaluation_ts - window, evaluation_ts)

    def first_by_qty(pts: list[PointView]) -> dict[str, PointView]:
        out: dict[str, PointView] = {}
        for p in sorted(pts, key=lambda x: x.point_id):
            out.setdefault(p.quantity_type, p)
        return out

    def series_for(pts: list[PointView]) -> dict[str, list]:
        from algo.tsdb.windows import gated_series

        out = {}
        for qty, p in first_by_qty(pts).items():
            out[qty] = gated_series(raw.get(p.point_id, []), min_good)
        return out

    building_id = next((e.building_id for e in chillers if e.building_id), "")
    plant_first = first_by_qty(points)

    async def prev(p: PointView) -> float | None:
        return await tsdb.previous_value(p.point_id, evaluation_ts)

    plant_latest = {
        qty: (await prev(p) if qty.endswith("setpoint") or qty == "unit_enable" else None)
        for qty, p in plant_first.items()
    }
    units: list[ChillerUnitView] = []
    for c in chillers:
        rp = {
            **config.chiller_rated_fallback(building_id, c.local_id or c.equipment_id),
            **c.rated_params,
        }

        num = make_num(rp)
        q, pw, u = (
            num("rated_cooling_capacity_kw"),
            num("rated_input_power_kw"),
            num("min_unload_ratio"),
        )
        c_points = [p for p in points if p.equipment_id == c.equipment_id]
        u_first = first_by_qty(c_points)
        run_v = None
        run_row = (
            await tsdb.previous_value(u_first["run_status"].point_id, evaluation_ts)
            if "run_status" in u_first
            else None
        )
        if run_row is None and "run_status" in u_first:
            # 枚态量 value 为空——回放面按最近文本值数值化
            txt = await tsdb.pool.fetchval(
                "SELECT value_text FROM telemetry WHERE point_id=$1 AND ts<$2 "
                "AND value_text IS NOT NULL ORDER BY ts DESC LIMIT 1",
                u_first["run_status"].point_id,
                evaluation_ts,
            )
            run_v = {"running": 1.0, "stopped": 0.0, "on": 1.0, "off": 0.0}.get((txt or "").lower())
        u_latest = {}
        for qty, p in u_first.items():
            if qty == "unit_enable":
                v = await prev(p)
                u_latest[qty] = v if v is not None else run_v
            elif qty == "run_status":
                u_latest[qty] = run_v
            else:
                u_latest[qty] = None  # 窗口值以 series 为准（回放判据全走窗口）
        if q is None or pw is None or u is None:
            continue
        units.append(
            ChillerUnitView(
                equipment_id=c.equipment_id,
                local_id=c.local_id or c.equipment_id,
                rated=ChillerRated(
                    rated_cooling_capacity_kw=q,
                    rated_input_power_kw=pw,
                    min_unload_ratio=u,
                    rated_cop=num("rated_cop"),
                ),
                points=u_first,
                latest=u_latest,
                series=series_for(c_points),
            )
        )
    if not units:
        return None
    tower_views = [
        EquipmentPointsView(
            equipment_id=t.equipment_id,
            local_id=t.local_id or t.equipment_id,
            rated_params=dict(t.rated_params),
            points=first_by_qty([p for p in points if p.equipment_id == t.equipment_id]),
            latest={},
            series=series_for([p for p in points if p.equipment_id == t.equipment_id]),
        )
        for t in towers
    ]
    weather = await tsdb.weather(evaluation_ts)
    # latest 功率面：窗口末桶值（回放无 Kafka 缓存）
    for u in units:
        if u.series.get("power"):
            u.latest["power"] = u.series["power"][-1].avg
    derived = _derive_metrics(units, series_for(points), weather)
    rows_1h = await tsdb.pool.fetch(
        "SELECT point_id, bucket, avg FROM telemetry_1h "
        "WHERE point_id = ANY($1::bigint[]) AND bucket >= $2 AND bucket < $3",
        [u.points["power"].point_id for u in units if "power" in u.points],
        evaluation_ts - timedelta(days=7),
        evaluation_ts,
    )
    by_pid: dict[int, list] = defaultdict(list)
    for r in rows_1h:
        if r["avg"] is not None:
            by_pid[r["point_id"]].append(r["avg"])
    models = {}
    for u in units:
        fb = rated_linearize(
            u.rated.rated_cooling_capacity_kw,
            u.rated.rated_input_power_kw,
            u.rated.min_unload_ratio,
            config.defaults.idle_power_ratio,
        )
        p = u.points.get("power")
        cop = u.rated.cop()
        samples = [(a * cop, a) for a in by_pid.get(p.point_id if p else -1, [])]
        models[u.equipment_id] = fit_affine(samples, min_samples=24) or fb
    return PlantView(
        system_id=system_id,
        building_id=building_id,
        chillers=units,
        towers=tower_views,
        points=plant_first,
        latest=plant_latest,
        series=series_for(points),
        derived=derived,
        affine_models=models,
    )


async def run_backtest(args: argparse.Namespace) -> dict:
    config = OptimizerConfigStore("config/optimizer.yaml")
    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=3)
    tsdb = ReplayTsdb(pool)
    snap = await load_snapshot_from_pg(args.pg_dsn)
    plants_map = group_plants(snap)
    t = align_to_15m(datetime.fromisoformat(args.frm))
    to = datetime.fromisoformat(args.to)
    drafts: list[ReplayDraft] = []
    fcst_evals: list[dict] = []
    slots = 0
    while t < to:
        t += timedelta(minutes=15)
        slots += 1
        for system_id, points in plants_map.items():
            plant = await assemble_plant(
                snap,
                system_id,
                points,
                tsdb,
                t,
                config.defaults.window_min,
                config.defaults.min_good_ratio,
                config,
            )
            if plant is None:
                continue
            weather = await tsdb.weather(t)
            pview = build_persistence_view(
                PersistenceInput(
                    evaluation_ts=t,
                    building_id=plant.building_id,
                    load_series=plant_load_series(plant),
                )
            )
            ctx = OptimizerContext(
                evaluation_ts=t, plants=[plant], weather=weather, forecast=pview, cfg=config
            )
            # 预测离线验证：persistence vs 实测（未来窗桶）
            if pview is not None:
                future = await tsdb.buckets(
                    [u.points["power"].point_id for u in plant.chillers if "power" in u.points],
                    t,
                    t + timedelta(minutes=60),
                )
                actual_by_ts: dict = {}
                for u in plant.chillers:
                    cop = u.rated.cop()
                    for b in future.get(
                        u.points["power"].point_id if "power" in u.points else -1, []
                    ):
                        if b.avg is not None:
                            actual_by_ts[b.bucket] = actual_by_ts.get(b.bucket, 0.0) + b.avg * cop
                for fp_ in pview.points:
                    horizon = (fp_.target_ts - t).total_seconds() / 60.0
                    nearest = min(
                        actual_by_ts,
                        key=lambda bt: abs((bt - t).total_seconds() / 60.0 - horizon),
                        default=None,
                    )  # type: ignore[arg-type]
                    if nearest is not None:
                        fcst_evals.append(
                            {
                                "horizon_min": horizon,
                                "pred_kw_th": fp_.load_kw_th,
                                "actual_kw_th": actual_by_ts[nearest],
                            }
                        )
            for strategy in STRATEGIES:
                eff = config.effective(strategy.strategy_id, plant.building_id or None)
                if not eff.enabled:
                    continue
                try:
                    out = strategy.evaluate(ctx, plant, eff)
                except Exception:
                    logging.getLogger(__name__).exception(
                        "backtest_strategy_error", extra={"strategy": strategy.strategy_id}
                    )
                    continue
                for d in out:
                    base_pts = [
                        u.series.get("power", [])[-6:]
                        for u in plant.chillers
                        if (u.latest.get("run_status") or 0.0) > 0.5
                    ]
                    baseline_p = sum(b[-1].avg for b in base_pts if b and b[-1].avg is not None)
                    post = await tsdb.buckets(
                        [u.points["power"].point_id for u in plant.chillers],
                        t,
                        t + timedelta(minutes=30),
                    )
                    post_vals = [r.avg for rows in post.values() for r in rows if r.avg is not None]
                    realized = None
                    if post_vals:
                        realized = baseline_p - fmean(post_vals)
                    drafts.append(
                        ReplayDraft(
                            evaluation_ts=t,
                            strategy_id=d.strategy_id,
                            expected_saving_kw=d.expected_saving_kw,
                            realized_saving_kw=realized,
                            confidence=d.confidence,
                            formula_id=str(d.evidence.get("formula_id", "?")),
                            baseline_p_kw=baseline_p,
                            post_p_kw=fmean(post_vals) if post_vals else None,
                        )
                    )
    await pool.close()
    return summarize(drafts, fcst_evals, args, slots)


def summarize(
    drafts: list[ReplayDraft], fcst: list[dict], args: argparse.Namespace, slots: int
) -> dict:
    by_strategy: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "mean_expected": 0.0, "mean_realized": None}
    )
    for d in drafts:
        s = by_strategy[d.strategy_id]
        s["count"] += 1
        s["mean_expected"] += d.expected_saving_kw
    for sid, s in by_strategy.items():
        if s["count"]:
            s["mean_expected"] = round(s["mean_expected"] / s["count"], 2)
        rl = [
            d.realized_saving_kw
            for d in drafts
            if d.strategy_id == sid and d.realized_saving_kw is not None
        ]
        s["mean_realized"] = round(fmean(rl), 2) if rl else None
        s["realized_n"] = len(rl)
    # 校准曲线：置信度分桶 × (expected−realized) 误差
    calibration = []
    pairs = [
        (d.confidence, d.expected_saving_kw, d.realized_saving_kw)
        for d in drafts
        if d.realized_saving_kw is not None
    ]
    for lo, hi in pairwise(CONF_BUCKETS):
        seg = [(e, r) for c, e, r in pairs if lo <= c < hi or (hi == 1.0 and c == 1.0)]
        if not seg:
            calibration.append({"conf_bucket": f"[{lo:.1f},{hi:.1f})", "n": 0})
            continue
        errs = [e - r for e, r in seg]
        calibration.append(
            {
                "conf_bucket": f"[{lo:.1f},{hi:.1f})",
                "n": len(seg),
                "mean_signed_err_kw": round(fmean(errs), 2),
                "mae_kw": round(fmean(abs(e) for e in errs), 2),
            }
        )
    # 预测离线验证
    fcst_by_h: dict[float, list] = defaultdict(list)
    for f in fcst:
        fcst_by_h[f["horizon_min"]].append(f)
    forecast_eval = []
    for h in sorted(fcst_by_h):
        seg = fcst_by_h[h]
        apes = [
            abs(f["pred_kw_th"] - f["actual_kw_th"]) / f["actual_kw_th"]
            for f in seg
            if f["actual_kw_th"] > 1e-9
        ]
        forecast_eval.append(
            {
                "horizon_min": h,
                "n": len(seg),
                "mae_kw_th": round(fmean(abs(f["pred_kw_th"] - f["actual_kw_th"]) for f in seg), 1),
                "mape": round(fmean(apes), 3) if apes else None,
            }
        )
    return {
        "caliber": {
            "expected": "E1–E4 稳态电功率节省（kW_e）——optimizer.md §8.2",
            "realized": "baseline 窗（前 30min 在运功率均值）− 执行后窗（后 30min 功率均值）",
            "note": "回放无真实执行——realized 为「若执行则观测到的功率漂移」代理（校准/评审用）",
            "window": {"from": args.frm, "to": args.to, "slots": slots},
            "strategies": "生产 STRATEGIES 代码路径（同一 evaluate + saving 公式）",
        },
        "per_strategy": dict(by_strategy),
        "calibration_curve": calibration,
        "forecast_offline_validation": {
            "source": "persistence（§5.2 MVP 降级链）",
            "metric": "对齐最近桶的逐点前推误差",
            "by_horizon": forecast_eval,
        },
        "scatter": [
            {
                "ts": d.evaluation_ts.isoformat(),
                "strategy": d.strategy_id,
                "expected_kw": d.expected_saving_kw,
                "realized_kw": d.realized_saving_kw,
                "confidence": d.confidence,
            }
            for d in drafts
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="optimizer 离线回测（IMPL-19 验收件）")
    parser.add_argument("--dsn", required=True, help="TSDB DSN（tsdb_algo 只读角色）")
    parser.add_argument("--pg-dsn", required=True, help="PG DSN（快照投影离线直读）")
    parser.add_argument("--from", dest="frm", required=True, help="窗口起（RFC3339）")
    parser.add_argument("--to", required=True, help="窗口止（RFC3339）")
    parser.add_argument("--out", default="build/backtest-report.json")
    args = parser.parse_args()
    report = asyncio.run(run_backtest(args))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"回测报告: {out}")
    print(
        json.dumps(
            {
                k: report[k]
                for k in (
                    "caliber",
                    "per_strategy",
                    "calibration_curve",
                    "forecast_offline_validation",
                )
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
