"""internal 面真实端点衔接（DAT-163 / IMPL-17 并入项，algo.md §15-1 收口）。

平台侧 internal 三端点（asset-snapshot / fdd findings GET·POST / fdd reports POST）
落地后的衔接回路：PlatformClient 直连**真实** api（thermio-platform），断言以 PG
行状态为准（ALGO_IT_PG_DSN 运维视角）——不再是 mock 的内存面。

门控：ALGO_IT_API_BASE_URL 设置才激活（scripts/it-fdd.sh 起真实 api 进程时注入）；
未设置整组 skip——algo.md §14 的 mock 回放链路（test_fdd_replay/test_report_job）
保持既定方式不受影响（蓝本边界：mock 是 §14 钉死的默认，真实面是增量衔接）。
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Literal

import asyncpg
import pytest
from algo.platform.client import PlatformAuthError, PlatformClient
from algo.platform.contracts import (
    FddFindingsBatch,
    FddReportSubmission,
    FindingCleared,
    FindingHit,
)

pytestmark = pytest.mark.integration

ALGO_VERSION = "0.1.0+itdeadbe"


def _req_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        pytest.skip(f"integration 环境变量缺失: {name}（经 scripts/it-fdd.sh 驱动）")
    return value


def _api_base() -> str:
    url = os.environ.get("ALGO_IT_API_BASE_URL", "")
    if not url:
        pytest.skip("ALGO_IT_API_BASE_URL 未设置（需 scripts/it-fdd.sh 起真实 api）")
    return url


async def _seed_asset(pg_dsn: str) -> tuple[str, str, str]:
    """种一栋楼/一系统/一设备（幂等：固定 id，ON CONFLICT DO NOTHING）。"""
    conn = await asyncpg.connect(pg_dsn)
    try:
        tenant_id = await conn.fetchval("SELECT id FROM tenant ORDER BY created_at LIMIT 1")
        assert tenant_id, "栈内无租户（先跑 it-fdd.sh 的种子面）"
        building_id = uuid.uuid4()
        system_id = uuid.uuid4()
        equipment_id = uuid.uuid4()
        async with conn.transaction():
            await conn.execute(
                """INSERT INTO building (id, tenant_id, name) VALUES ($1, $2, 'IT 真实端点楼')
                   ON CONFLICT DO NOTHING""",
                building_id,
                tenant_id,
            )
            await conn.execute(
                """INSERT INTO hvac_system (id, tenant_id, building_id, system_type, name)
                   VALUES ($1, $2, $3, 'chilled_water', 'IT 真实端点系统')""",
                system_id,
                tenant_id,
                building_id,
            )
            await conn.execute(
                """INSERT INTO equipment (id, tenant_id, system_id, equipment_type, name, local_id)
                   VALUES ($1, $2, $3, 'chiller', 'IT 真实端点冷机', '3#冷机')""",
                equipment_id,
                tenant_id,
                system_id,
            )
        return str(tenant_id), str(building_id), str(equipment_id)
    finally:
        await conn.close()


def _hit(
    equipment_id: str,
    rule_key: str,
    at: datetime,
    severity: Literal["info", "warning", "minor", "major", "critical"] = "warning",
) -> FindingHit:
    return FindingHit(
        equipment_id=equipment_id,
        rule_key=rule_key,
        severity=severity,
        title=f"{rule_key} 持续命中",
        evidence={
            "points": [{"point_id": 1, "quantity_type": "chw_supply_temp"}],
            "window": {"from": at.isoformat(), "to": at.isoformat()},
            "detail": {"delta_t_avg_c": -11.7},
        },
        suggested_action="检查蒸发器",
        first_detected_at=at,
        last_detected_at=at,
    )


async def test_real_internal_face_end_to_end() -> None:
    base = _api_base()
    pg_dsn = _req_env("ALGO_IT_PG_DSN")
    svc_token = _req_env("ALGO_IT_SVC_TOKEN")
    tenant_id, building_id, equipment_id = await _seed_asset(pg_dsn)
    client = PlatformClient(base, svc_token)
    await client.start()
    try:
        now = datetime.now(UTC).replace(microsecond=0)

        # ── 1) asset-snapshot：全量含种入设备 + 增量游标 ──────────────────────
        snap = await client.get_asset_snapshot()
        assert any(e.equipment_id == equipment_id for e in snap.equipments)
        eq = next(e for e in snap.equipments if e.equipment_id == equipment_id)
        assert eq.tenant_id == tenant_id and eq.building_id == building_id
        delta = await client.get_asset_snapshot(updated_since=now + timedelta(hours=1))
        assert all(e.equipment_id != equipment_id for e in delta.points)  # 增量只回 touched

        # ── 2) POST findings：首轮 insert、续报刷新时间轴不变、clear 置 resolved ──
        rule = "chiller.delta_t_low"
        await client.submit_fdd_findings(_batch(ALGO_VERSION, [_hit(equipment_id, rule, now)]))
        await client.submit_fdd_findings(
            _batch(
                ALGO_VERSION,
                [_hit(equipment_id, rule, now + timedelta(minutes=15), severity="major")],
            )
        )
        conn = await asyncpg.connect(pg_dsn)
        try:
            rows = await conn.fetch(
                "SELECT status, severity, first_detected_at, last_detected_at, algo_version"
                " FROM fdd_finding WHERE equipment_id = $1 AND rule_key = $2",
                equipment_id,
                rule,
            )
            assert len(rows) == 1  # 持续命中不产生新行（ddl §9.2 upsert 语义）
            row = rows[0]
            assert row["status"] == "open"
            assert row["severity"] == "major"  # 展示面被同轮最新判定覆盖
            assert row["first_detected_at"].replace(tzinfo=UTC) == now  # 首见时间轴不变
            assert row["algo_version"] == ALGO_VERSION

            # ── 3) GET findings：形状 + 活跃窗口 ──────────────────────────────
            listing = await client.list_fdd_findings(
                building_id, now - timedelta(hours=1), now + timedelta(hours=1)
            )
            mine = [item for item in listing.items if item.rule_key == rule]
            assert len(mine) == 1
            assert mine[0].equipment.local_id == "3#冷机"
            assert mine[0].status == "open"

            # ── 4) POST report：同期 upsert（重生成覆盖不重复）─────────────────
            for _ in range(2):
                await client.submit_fdd_report(
                    _report_payload(building_id, equipment_id, ALGO_VERSION)
                )
            count = await conn.fetchval(
                "SELECT count(*) FROM fdd_report WHERE building_id = $1", building_id
            )
            assert count == 1

            # ── 5) clear：置 resolved；无活跃行重复 clear 幂等 ─────────────────
            cleared_at = now + timedelta(minutes=30)
            for _ in range(2):
                await client.submit_fdd_findings(
                    _batch(
                        ALGO_VERSION,
                        [],
                        [
                            FindingCleared(
                                equipment_id=equipment_id,
                                rule_key=rule,
                                cleared_at=cleared_at,
                            )
                        ],
                    )
                )
            status = await conn.fetchval(
                "SELECT status FROM fdd_finding WHERE equipment_id = $1 AND rule_key = $2",
                equipment_id,
                rule,
            )
            assert status == "resolved"
        finally:
            await conn.close()
    finally:
        await client.stop()


async def test_real_internal_face_rejects_bad_token() -> None:
    base = _api_base()
    client = PlatformClient(base, "wrong-token-it")
    await client.start()
    try:
        with pytest.raises(PlatformAuthError):
            await client.get_asset_snapshot()
    finally:
        await client.stop()


# ── 载荷辅助（pydantic 模型面构造——与生产链路同一模型，wire 层由模型序列化钉死）──


def _batch(
    algo_version: str,
    hits: list[FindingHit],
    cleared: list[FindingCleared] | None = None,
) -> FddFindingsBatch:
    return FddFindingsBatch(algo_version=algo_version, hits=hits, cleared=cleared or [])


def _report_payload(building_id: str, equipment_id: str, algo_version: str) -> FddReportSubmission:
    end = datetime.now(UTC).date()
    start = end - timedelta(days=1)
    return FddReportSubmission.model_validate(
        {
            "building_id": building_id,
            "period_type": "day",
            "period": {"start": start.isoformat(), "end": end.isoformat()},
            "summary": {
                "counts": {"new": 1, "resolved": 0, "persisting": 1},
                "new_by_severity": {"info": 0, "warning": 1, "minor": 0, "major": 0, "critical": 0},
                "open_by_severity": {
                    "info": 0,
                    "warning": 1,
                    "minor": 0,
                    "major": 0,
                    "critical": 0,
                },
                "health_ranking": [
                    {
                        "equipment_id": equipment_id,
                        "equipment_name": "IT 真实端点冷机",
                        "equipment_type": "chiller",
                        "open_count": 1,
                        "weighted_score": 2,
                    }
                ],
            },
            "algo_version": algo_version,
        }
    )
