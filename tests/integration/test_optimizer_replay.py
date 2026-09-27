"""优化器回放链路（optimizer.md §11 integration 层；IMPL-19 / DAT-165）：

gw-sim 冷源场景（backfill 历史回放 + 点名级 overrides）→ EMQX → ingestd →
Kafka/TSDB → optimize_15m 一轮 → mock internal 断言 proposal 信封载荷
（含 evidence 骨架、previous_value 来源）与节流；场景 E 验证 FDD 开放发现
对 R1 的抑制（§6.7——FDD 与优化器同进程共享 Hysteresis）。

前置由 scripts/it-fdd.sh 完成：thermio- 栈、迁移/种子（含 algo-optimizer-seed.sql）、
ingestd 在跑；gw-sim 为 DAT-165 增量构建（points.offset + overrides 旋钮）。
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from pathlib import Path

import asyncpg
import pytest
from algo.fdd.engine import FddEngine
from algo.fdd.hysteresis import Hysteresis
from algo.fdd.thresholds import ThresholdStore
from algo.kafka.consumer import LatestCache, TelemetryLatestConsumer
from algo.kafka.topics import GROUP_ALGO_OPTIMIZER
from algo.optimizer.config import OptimizerConfigStore
from algo.optimizer.engine import OptimizerEngine, WeatherViewReader
from algo.optimizer.forecast_store import ForecastStore
from algo.optimizer.suppress import FddSuppressView
from algo.platform.client import PlatformClient
from algo.proposal.producer import ProposalSubmitter
from algo.semantics.snapshot import SnapshotService
from algo.tsdb.client import TsdbClient
from algo.weather.job import WeatherService

from tests.integration.mock_internal import InternalState, make_server
from tests.integration.test_fdd_replay import FakeWeatherProvider, build_snapshot

FIXTURES = Path(__file__).parent / "fixtures"
BUILDING = "22222222-2222-2222-2222-222222222222"
EQ_CH1 = "55555555-0000-0000-0000-000000000011"
EQ_CH2 = "55555555-0000-0000-0000-000000000012"


class _WetBulbWeather(FakeWeatherProvider):
    """15°C / 60% → 湿球 ≈ 10.52（R4 逼近温度判据锚点）。"""

    name = "fake-it-optimizer"


OP_POINT_IDS = list(range(101, 114))  # SIM_0100..0112（e2e-seed 顺序分配 id）


async def _refresh_caggs(admin_dsn: str) -> None:
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute("CALL refresh_continuous_aggregate('telemetry_5min', NULL, NULL)")
        await admin.execute("CALL refresh_continuous_aggregate('telemetry_1h', NULL, NULL)")
    finally:
        await admin.close()


async def _wait_for_scenario_data(it_env, latest: LatestCache) -> None:  # type: ignore[no-untyped-def]
    """等场景数据过链再刷新评估（时序确定性）。

    gw-sim 退出只代表发布完成——ingestd 批量写 TSDB 与 Kafka 消费仍在途；
    全量套件（前置 6min FDD 回放负载）下立即 refresh 曾读到空桶
    （drafts=0/forecast=none 的间歇性失败根因）。就位判据：
    ① 原始表近 15min 出现本场景 live 行（FIFO 保证更早的回放行已落）；
    ② 优化器 consumer 缓存覆盖全部场景点位（previous_value 走缓存路径）。
    """
    import time as _time

    deadline = _time.monotonic() + 90
    while _time.monotonic() < deadline:
        conn = await asyncpg.connect(it_env.tsdb_admin_dsn)
        try:
            n = await conn.fetchval(
                "SELECT count(*) FROM telemetry WHERE point_id = ANY($1::bigint[]) "
                "AND ts > now() - interval '15 minutes'",
                OP_POINT_IDS,
            )
        finally:
            await conn.close()
        cached = sum(1 for pid in OP_POINT_IDS if latest.get(pid) is not None)
        if (n or 0) >= len(OP_POINT_IDS) and cached >= len(OP_POINT_IDS):
            return
        await asyncio.sleep(1)
    pytest.fail("场景数据 90s 内未完成过链（EMQX→ingestd→TSDB/Kafka）——检查 ingestd 日志")


async def _purge_scenario_data(admin_dsn: str) -> None:
    """逐场景清理优化器点位的原始+聚合数据（场景确定性：不同回放的 ts 网格错位，
    同桶混值会污染窗口均值；删除后 refresh 使 cagg 失效区间重算）。"""
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(
            "DELETE FROM telemetry WHERE point_id = ANY($1::bigint[])", OP_POINT_IDS
        )
        await admin.execute("CALL refresh_continuous_aggregate('telemetry_5min', NULL, NULL)")
        await admin.execute("CALL refresh_continuous_aggregate('telemetry_1h', NULL, NULL)")
    finally:
        await admin.close()


async def _run_gwsim(it_env, profile: str) -> None:  # type: ignore[no-untyped-def]
    proc = await asyncio.create_subprocess_exec(
        it_env.gwsim_bin,
        "-profile",
        str(FIXTURES / profile),
        "-summary-file",
        f"build/it-gwsim-{profile}",
        env={
            **os.environ,
            "GWSIM_BROKER_URL": it_env.mqtt_broker_url,
            "GWSIM_MQTT_PASSWORD": it_env.mqtt_password,
        },
    )
    rc = await proc.wait()
    assert rc == 0, f"gw-sim {profile} 回放异常退出"


async def _setup(it_env) -> tuple:  # type: ignore[no-untyped-def]
    snapshot_wire = await build_snapshot(it_env.pg_dsn)
    state = InternalState(snapshot_wire, it_env.svc_token)
    server, base_url = make_server(state)
    tsdb = TsdbClient(it_env.tsdb_dsn)
    await tsdb.start()
    platform = PlatformClient(base_url, it_env.svc_token)
    latest = LatestCache()
    consumer = TelemetryLatestConsumer(
        it_env.kafka_brokers.split(","),
        latest,
        lag_check_interval_s=10.0,
        group=GROUP_ALGO_OPTIMIZER,
    )
    snap = SnapshotService(platform)
    weather = WeatherService(tsdb, _WetBulbWeather(), ["it-station"])
    suppress = FddSuppressView()
    engine = OptimizerEngine(
        tsdb=tsdb,
        snapshot=snap,
        config=OptimizerConfigStore("/nonexistent-optimizer.yaml"),
        submitter=ProposalSubmitter(platform),
        latest=latest,
        forecast_store=ForecastStore(),
        weather_reader=WeatherViewReader(weather),
        suppress=suppress,
    )
    return state, server, tsdb, platform, latest, consumer, snap, weather, suppress, engine


def _posts(state: InternalState) -> list[dict]:  # type: ignore[no-untyped-def]
    return state.proposal_posts


@pytest.mark.integration
async def test_optimizer_scenario_matrix(it_env) -> None:  # type: ignore[no-untyped-def]
    """五场景矩阵：A→R2 / B→R3 / C→R1+节流 / D→R4 / E→FDD 抑制。"""
    (
        state,
        server,
        tsdb,
        platform,
        latest,
        consumer,
        snap,
        weather,
        suppress,
        engine,
    ) = await _setup(it_env)
    await weather.fetch_actual_job()  # 湿球判据数据面（15°C/60%）
    await consumer.start()  # 先就位（latest 语义 seek end——后续消息全量入缓存）
    try:
        # ── 场景 A：低载双机 → R2 ──────────────────────────────────────────
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-a-lowload-dual.json")
        await _wait_for_scenario_data(it_env, latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        ra = await engine.run_round()
        assert ra.errors == 0 and ra.plants >= 1
        posts = _posts(state)
        assert len(posts) == 1, f"A 场景应恰出 1 条 R2，实得 {len(posts)}"
        env = posts[0]
        assert env["algo"] == "optimizer/chiller-sequencer"
        assert env["target"]["point"] == "unit_enable"
        assert env["action"] == {"op": "set", "value": 0.0, "unit": "dimensionless"}
        assert env["previous_value"] == 1.0
        assert env["expected_saving_kw"] == pytest.approx(56.0, abs=1.5)  # E2 额定线性化
        assert env["evidence"]["formula_id"] == "E2_stage_down"
        assert env["evidence"]["forecast"]["source"] in ("persistence", "trend_extrapolation")

        # ── 场景 B：高载单机+待机 → R3（保护性负 saving）────────────────────
        state.proposal_posts.clear()
        engine.memory.reset()  # 换场景清提交记忆（每场景独立评审）
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-b-highload-single.json")
        await _wait_for_scenario_data(it_env, latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        rb = await engine.run_round()
        assert rb.errors == 0
        posts = _posts(state)
        assert len(posts) == 1, f"B 场景应恰出 1 条 R3，实得 {len(posts)}"
        env = posts[0]
        assert env["target"]["point"] == "unit_enable"
        assert env["target"]["equipment_id"] == EQ_CH2  # 待机机组
        assert env["action"]["value"] == 1.0 and env["previous_value"] == 0.0
        assert env["expected_saving_kw"] < 0  # 保护成本诚实上报
        assert "保护性" in env["rationale"]
        assert env["evidence"]["formula_id"] == "E3_stage_split"

        # ── 场景 C：低温差 → R1 + 三轮节流（仅首轮出卡）───────────────────
        state.proposal_posts.clear()
        engine.memory.__init__()
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-c-lowdeltat.json")
        await _wait_for_scenario_data(it_env, latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        rc1 = await engine.run_round()
        assert rc1.errors == 0
        posts = _posts(state)
        assert len(posts) == 1, f"C 场景首轮应出 1 条 R1，实得 {len(posts)}"
        env = posts[0]
        assert env["algo"] == "optimizer/chw-temp-reset"
        assert env["target"] == {"equipment_id": EQ_CH1, "point": "chw_supply_temp_setpoint"}
        assert env["action"] == {"op": "set", "value": 7.5, "unit": "degC"}
        assert env["previous_value"] == 7.0
        assert env["expected_saving_kw"] == pytest.approx(8.0, abs=0.5)  # 0.02×0.5×800
        assert env["evidence"]["formula_id"] == "E1_temp_reset"
        assert env["evidence"]["inputs"]["chw_delta_t_c"] == pytest.approx(1.6, abs=0.2)
        assert env["evidence"]["previous_value_source"] == "point_latest"
        assert env["evidence"]["baseline"]["window"] == "30min"
        assert env["algo_version"] == "0.1.0"  # 裸 semver（热调参数走 cfg_fp8）
        assert len(env["evidence"]["cfg_fp8"]) == 8
        assert datetime.fromisoformat(env["expires_at"]).tzinfo is not None
        # 节流：连续三轮仅首轮出卡（§7.2 冷却窗 = expires_at）
        rc2 = await engine.run_round()
        rc3 = await engine.run_round()
        assert _posts(state) and len(_posts(state)) == 1
        assert rc2.arbitrated is not None and rc2.arbitrated.throttled == 1
        assert rc3.arbitrated is not None and rc3.arbitrated.throttled == 1

        # ── 场景 D：冷凝侧 → R4 ────────────────────────────────────────────
        state.proposal_posts.clear()
        engine.memory.__init__()
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-d-condenser.json")
        await _wait_for_scenario_data(it_env, latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        rd = await engine.run_round()
        assert rd.errors == 0
        posts = _posts(state)
        assert len(posts) == 1, f"D 场景应恰出 1 条 R4，实得 {len(posts)}"
        env = posts[0]
        assert env["algo"] == "optimizer/cw-temp-reset"
        assert env["target"]["point"] == "cw_supply_temp_setpoint"
        assert env["action"]["value"] == 23.0 and env["previous_value"] == 24.0
        assert env["expected_saving_kw"] == pytest.approx(7.6, abs=0.5)
        assert env["evidence"]["formula_id"] == "E4_condenser"
        assert env["evidence"]["inputs"]["wet_bulb_c"] == pytest.approx(10.5, abs=0.3)

        # ── 场景 E：FDD 开放发现抑制 R1（§6.7）────────────────────────────
        state.proposal_posts.clear()
        engine.memory.__init__()
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-e-fdd-suppress.json")
        await _wait_for_scenario_data(it_env, latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        thresholds = ThresholdStore(FIXTURES / "thresholds-it.yaml")  # confirm=1
        hyst = Hysteresis()
        suppress.bind(hyst)  # FDD 与优化器共享迟滞面（§6.7 进程内抑制视图）
        fdd_engine = FddEngine(
            tsdb=tsdb,
            snapshot=snap,
            thresholds=thresholds,
            platform=platform,
            latest=latest,
            weather_reader=weather,
            hysteresis=hyst,
        )
        rf = await fdd_engine.run_round()
        hit_rules = {h.rule_key for h in rf.submitted_hits}
        assert "chiller.delta_t_low" in hit_rules  # CH-OP-1 ΔT=1.0 < 1.2
        assert suppress.has_open_finding([EQ_CH1])
        re_ = await engine.run_round()
        assert re_.errors == 0
        assert _posts(state) == [], "R1 判据满足但必须被开放发现抑制（零提案）"
    finally:
        await consumer.stop()
        await tsdb.stop()
        await platform.stop()
        server.shutdown()


@pytest.mark.integration
async def test_optimizer_advisory_only_gate(it_env) -> None:  # type: ignore[no-untyped-def]
    """§0 铁律：control_mode 非 advisory 的目标点零提案。

    先 PG 直改 0105/0106 为 supervised（夹具操作面），再装配 mock（快照投影
    读到新值）；场景 C 数据下 R1 应被前置闸拦下。
    """
    conn = await asyncpg.connect(it_env.pg_dsn)
    await conn.execute(
        "UPDATE point SET control_mode = 'supervised' WHERE raw_name IN ('SIM_0105','SIM_0106')"
    )
    await conn.close()
    (
        state,
        server,
        tsdb,
        platform,
        _latest,
        consumer,
        _snap,
        _weather,
        _suppress,
        engine,
    ) = await _setup(it_env)  # 夹具口径：快照投影在 PG 直改之后装配
    await consumer.start()
    try:
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _purge_scenario_data(it_env.tsdb_admin_dsn)
        await _run_gwsim(it_env, "optimizer-c-lowdeltat.json")
        await _wait_for_scenario_data(it_env, _latest)
        await _refresh_caggs(it_env.tsdb_admin_dsn)
        r = await engine.run_round()
        assert r.errors == 0
        assert r.drafts == [] and _posts(state) == []
    finally:
        conn = await asyncpg.connect(it_env.pg_dsn)
        await conn.execute(
            "UPDATE point SET control_mode = 'advisory' WHERE raw_name IN ('SIM_0105','SIM_0106')"
        )
        await conn.close()
        await consumer.stop()
        await tsdb.stop()
        await platform.stop()
        server.shutdown()
