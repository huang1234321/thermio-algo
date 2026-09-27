"""FDD 回放链路（algo.md §14 integration 层）：

gw-sim → EMQX → ingestd → Kafka/TSDB → algo（真 Kafka 消费 + 真 cagg 窗口 + 真规则求值
+ 真 internal 提交）→ mock 中台断言 findings upsert 载荷；随后 YAML 热调 → cleared。

前置由 scripts/it-fdd.sh 完成：thermio- 栈起、迁移/种子就位、ingestd 在跑。
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest
from algo.fdd.engine import FddEngine
from algo.fdd.thresholds import ThresholdStore
from algo.kafka.consumer import LatestCache, TelemetryLatestConsumer
from algo.platform.client import PlatformClient
from algo.semantics.snapshot import SnapshotService
from algo.tsdb.client import TsdbClient
from algo.weather.job import WeatherService
from algo.weather.providers import ActualObs, ForecastIssue, ForecastPoint

from tests.integration.mock_internal import InternalState, make_server

FIXTURES = Path(__file__).parent / "fixtures"
TENANT = "11111111-1111-1111-1111-111111111111"
BUILDING = "22222222-2222-2222-2222-222222222222"


class FakeWeatherProvider:
    """确定性天气供应商（15°C / 60% → 湿球 ≈11°C）。"""

    name = "fake-it"

    async def fetch_actual(self, station_id: str) -> ActualObs:
        return ActualObs(
            station_id=station_id,
            obs_ts=datetime.now(UTC),
            temp_c=15.0,
            rh_pct=60.0,
            source=self.name,
        )

    async def fetch_forecast(self, station_id: str) -> ForecastIssue:
        now = datetime.now(UTC)
        return ForecastIssue(
            station_id=station_id,
            issued_at=now,
            targets=(ForecastPoint(target_ts=now, temp_c=15.5, rh_pct=55.0),),
            source=self.name,
        )


async def build_snapshot(pg_dsn: str) -> dict:
    """mock 的 api 侧投影（= 真实 GET /internal/algo/asset-snapshot 将来做的事：
    从 PG equipment/point 列投影，algo.md §6.2 wire）。"""
    conn = await asyncpg.connect(pg_dsn)
    try:
        eqs = await conn.fetch(
            """
            SELECT e.id, e.equipment_type, e.system_id, s.building_id, e.tenant_id,
                   e.local_id, e.name, e.rated_params
            FROM equipment e JOIN hvac_system s ON s.id = e.system_id
            """
        )
        pts = await conn.fetch(
            """
            SELECT p.id, p.equipment_id, p.quantity_type, p.unit_std,
                   p.valid_range_min, p.valid_range_max, p.is_controllable,
                   p.clamp_min, p.clamp_max, p.control_mode
            FROM point p WHERE p.equipment_id IS NOT NULL
            """
        )
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "equipments": [
                {
                    "equipment_id": str(e["id"]),
                    "equipment_type": e["equipment_type"],
                    "system_id": str(e["system_id"]),
                    "building_id": str(e["building_id"]),
                    "tenant_id": str(e["tenant_id"]),
                    "local_id": e["local_id"],
                    "name": e["name"],
                    "rated_params": json.loads(e["rated_params"] or "{}"),
                }
                for e in eqs
            ],
            "points": [
                {
                    "point_id": p["id"],
                    "equipment_id": str(p["equipment_id"]),
                    "quantity_type": p["quantity_type"] or "",
                    "unit_std": p["unit_std"],
                    "valid_range_min": float(p["valid_range_min"])
                    if p["valid_range_min"] is not None
                    else None,
                    "valid_range_max": float(p["valid_range_max"])
                    if p["valid_range_max"] is not None
                    else None,
                    "is_controllable": p["is_controllable"],
                    "clamp_min": float(p["clamp_min"]) if p["clamp_min"] is not None else None,
                    "clamp_max": float(p["clamp_max"]) if p["clamp_max"] is not None else None,
                    "control_mode": p["control_mode"],
                }
                for p in pts
            ],
        }
    finally:
        await conn.close()


@pytest.mark.integration
async def test_fdd_replay_hit_refresh_hotreload_clear(it_env) -> None:  # type: ignore[no-untyped-def]
    pytest.importorskip("aiokafka")
    snapshot_wire = await build_snapshot(it_env.pg_dsn)
    state = InternalState(snapshot_wire, it_env.svc_token)
    server, base_url = make_server(state)

    # 阈值文件复制到 tmp（热调阶段改写的是这份）
    thresholds_path = Path("build") / "it-thresholds.yaml"
    thresholds_path.parent.mkdir(exist_ok=True)
    shutil.copy(FIXTURES / "thresholds-it.yaml", thresholds_path)

    tsdb = TsdbClient(it_env.tsdb_dsn)
    await tsdb.start()  # 含 §1.2 权限面校验（tsdb_algo 角色真实连接）
    platform = PlatformClient(base_url, it_env.svc_token)
    snap = SnapshotService(platform)
    thresholds = ThresholdStore(thresholds_path)
    latest = LatestCache()
    consumer = TelemetryLatestConsumer(
        it_env.kafka_brokers.split(","), latest, lag_check_interval_s=10.0
    )
    weather = WeatherService(tsdb, FakeWeatherProvider(), ["it-station"])
    engine = FddEngine(
        tsdb=tsdb,
        snapshot=snap,
        thresholds=thresholds,
        platform=platform,
        latest=latest,
        weather_reader=weather,
    )

    # 0) 天气 job：真 upsert（§12 R/W 面走 tsdb_algo 角色）
    obs = await weather.fetch_actual_job()
    assert obs and obs[0].temp_c == 15.0
    wet = await weather.latest_obs()
    assert wet is not None and wet.temp_c == 15.0

    # 1) Kafka 最新值消费先就位（启动 seek end——后续消息全量入缓存）
    await consumer.start()
    try:
        # 2) gw-sim 回放（真 MQTT → ingestd → Kafka/TSDB）
        proc = await asyncio.create_subprocess_exec(
            it_env.gwsim_bin,
            "-profile",
            str(FIXTURES / "fdd-replay.json"),
            "-summary-file",
            "build/it-gwsim.json",
            env={
                **os.environ,
                "GWSIM_BROKER_URL": it_env.mqtt_broker_url,
                "GWSIM_MQTT_PASSWORD": it_env.mqtt_password,
            },
        )
        rc = await proc.wait()
        assert rc == 0, "gw-sim 回放异常退出"

        # 3) 等数据过链（latest cache 与 TSDB 双面）
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if len(latest) >= 16:
                break
            await asyncio.sleep(1)
        assert len(latest) >= 16, f"Kafka 最新值缓存未就绪: {len(latest)}"

        # 3.5) 物化评估窗（ingest.md §8 runbook 口径：手动 CALL refresh 归属
        #      属主/运维面——tsdb_algo 角色被正确拒绝（must be owner），由测试
        #      编排以管理员连接执行；实测 TS 2.17 cagg materialized_only=true，
        #      与 ddl.md §11.2「real-time 默认开」不符——跨仓发现，已记录交付说明）
        admin = await asyncpg.connect(it_env.tsdb_admin_dsn)
        try:
            await admin.execute("CALL refresh_continuous_aggregate('telemetry_5min', NULL, NULL)")
        finally:
            await admin.close()

        # 4) 第一轮评估 → 新命中（confirm=1）
        r1 = await engine.run_round()
        assert r1.errors == 0, f"规则求值报错: {r1.errors}"
        hit_rules = {h.rule_key for h in r1.submitted_hits}
        assert "chiller.delta_t_low" in hit_rules
        assert "chiller.temp_sensor_reversed" in hit_rules
        assert "cooling_tower.approach_high" in hit_rules
        assert "chiller.supply_temp_high" not in hit_rules  # 负路径
        assert r1.new_hits == 3
        assert r1.submitted
        assert r1.algo_version.startswith("0.1.0+") and len(r1.algo_version.split("+")[1]) == 8

        # mock 侧报文断言（§8.2 wire）
        assert len(state.findings_posts) == 1
        post = state.findings_posts[0]
        assert post["algo_version"] == r1.algo_version
        assert set(post) == {"algo_version", "hits", "cleared"}
        by_rule = {h["rule_key"]: h for h in post["hits"]}
        hit = by_rule["chiller.delta_t_low"]
        assert set(hit) == {
            "equipment_id",
            "rule_key",
            "severity",
            "title",
            "evidence",
            "suggested_action",
            "first_detected_at",
            "last_detected_at",
        }
        assert "tenant_id" not in hit  # 租户由 api 从目标实体解析（§11-5）
        assert hit["severity"] in ("info", "warning", "minor", "major", "critical")
        ev = hit["evidence"]
        assert {p["quantity_type"] for p in ev["points"]} >= {
            "chw_supply_temp",
            "chw_return_temp",
        }
        assert ev["window"]["from"] < ev["window"]["to"]
        assert hit["first_detected_at"] == hit["last_detected_at"]  # 首见轮
        # degF→degC 归一路径证据：ΔT 深负（supply≈20 / return≈−6）
        assert ev["detail"]["delta_t_avg_c"] < -10
        approach = by_rule["cooling_tower.approach_high"]
        assert approach["evidence"]["detail"]["approach_c"] > 6  # 湿球联动判据生效

        # 5) 第二轮（数据仍在窗内或新桶续报）→ upsert 刷新语义
        r2 = await engine.run_round()
        assert r2.errors == 0
        if r2.submitted_hits:  # 同规则续报：first_detected_at 不变（时间轴不漂移）
            posts2 = state.findings_posts[-1]
            for h in posts2["hits"]:
                if h["rule_key"] == "chiller.delta_t_low":
                    assert (
                        h["first_detected_at"] == hit["first_detected_at"]
                    )  # 判定时间轴不变（ddl §9.2 upsert 语义）
                    assert h["last_detected_at"] >= hit["last_detected_at"]

        # 6) 阈值热调（§7.4：改 YAML，下一轮生效，不重启进程）
        time.sleep(0.05)
        text = thresholds_path.read_text(encoding="utf-8")
        thresholds_path.write_text(
            text.replace("delta_t_min_c: 1.2", "delta_t_min_c: -100.0"), encoding="utf-8"
        )
        r3 = await engine.run_round()
        assert r3.errors == 0
        hit3 = {h.rule_key for h in r3.submitted_hits}
        assert "chiller.delta_t_low" not in hit3  # 新阈值已生效
        cleared3 = {c.rule_key for c in r3.submitted_cleared}
        assert "chiller.delta_t_low" in cleared3  # clear=1 → 本轮即消除
        assert r3.algo_version != r1.algo_version  # 指纹纳入阈值（§9 归因闭环）

        # mock 内存落库面：活跃行刷新 + resolved 语义
        statuses = {
            (row["equipment"]["id"], row["rule_key"]): row["status"]
            for row in state.list_findings()
        }
        eq_ch1 = next(
            e["equipment_id"] for e in snapshot_wire["equipments"] if e.get("local_id") == "1#冷机"
        )
        assert statuses[(eq_ch1, "chiller.delta_t_low")] == "resolved"
        assert statuses[(eq_ch1, "chiller.temp_sensor_reversed")] == "open"
    finally:
        await consumer.stop()
        await tsdb.stop()
        await platform.stop()
        server.shutdown()
