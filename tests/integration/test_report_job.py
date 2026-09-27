"""FDD 报告链路（algo.md §8.3 / M6-fdd §6）：internal 读回路 + 提交面。

数据流：mock GET /internal/fdd/findings（服务合成发现历史）→ 本地 §4.4 分类聚合
→ POST /internal/fdd/reports 断言 summary。与 replay 测试解耦（合成数据自洽）。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from algo.fdd.report import LOCAL_TZ, FddReportGenerator, previous_day
from algo.fdd.rules import RULES
from algo.fdd.thresholds import ThresholdStore
from algo.platform.client import PlatformClient
from algo.semantics.snapshot import SnapshotService

from tests.integration.mock_internal import InternalState, make_server

EQ = "55555555-0000-0000-0000-000000000001"
BLD = "22222222-2222-2222-2222-222222222222"


def _snap() -> dict:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "equipments": [
            {
                "equipment_id": EQ,
                "equipment_type": "chiller",
                "building_id": BLD,
                "local_id": "1#冷机",
                "name": "E2E 一号冷机",
                "rated_params": {},
            }
        ],
        "points": [],
    }


def _finding(first: datetime, resolved: datetime | None, sev: str) -> dict:
    return {
        "id": f"{first.timestamp():.0f}-{sev}",
        "building_id": BLD,
        "equipment": {
            "id": EQ,
            "name": "E2E 一号冷机",
            "local_id": "1#冷机",
            "equipment_type": "chiller",
        },
        "rule_key": "chiller.delta_t_low",
        "severity": sev,
        "status": "resolved" if resolved else "open",
        "title": "t",
        "suggested_action": None,
        "algo_version": "0.1.0+deadbeef",
        "first_detected_at": first.isoformat(),
        "last_detected_at": first.isoformat(),
        "resolved_at": resolved.isoformat() if resolved else None,
        "ignored_at": None,
        "review": None,
        "created_at": first.isoformat(),
    }


@pytest.mark.integration
async def test_day_report_via_internal_read_loop(it_env) -> None:  # type: ignore[no-untyped-def]
    now = datetime.now(UTC)
    start, end = previous_day(now)
    s = datetime.combine(start, datetime.min.time(), tzinfo=LOCAL_TZ)

    row_new_open = _finding(s + timedelta(hours=2), None, "warning")  # new + persisting
    row_resolved = _finding(s - timedelta(days=1), s + timedelta(hours=3), "major")  # resolved
    row_old_open = _finding(s - timedelta(days=1), None, "critical")  # persisting

    state = InternalState(_snap(), it_env.svc_token)
    state.active[(EQ, "r1")] = row_new_open
    state.active[(EQ, "r2")] = row_old_open
    state.resolved.append(row_resolved)
    server, base_url = make_server(state)

    platform = PlatformClient(base_url, it_env.svc_token)
    snap = SnapshotService(platform)
    thresholds = ThresholdStore("config/thresholds.yaml")
    gen = FddReportGenerator(platform, snap, RULES, thresholds)

    subs = await gen.generate("day", now=now)
    try:
        assert len(subs) == 1  # 快照里一栋楼
        sub = subs[0]
        assert sub.building_id == BLD
        assert sub.period_type == "day"
        assert sub.period.start == start.isoformat() and sub.period.end == end.isoformat()
        assert sub.algo_version.startswith("0.1.0+")
        c = sub.summary.counts
        assert c.new == 1  # row_new_open 首见 ∈ 期
        assert c.resolved == 1  # row_resolved 消除时刻 ∈ 期
        assert c.persisting == 2  # row_new_open + row_old_open 期末未决
        assert set(sub.summary.new_by_severity) == {
            "info",
            "warning",
            "minor",
            "major",
            "critical",
        }
        assert sub.summary.new_by_severity["warning"] == 1
        # 提交面：mock 收到 POST /internal/fdd/reports
        assert len(state.report_posts) == 1
        assert state.report_posts[0]["period"]["start"] == sub.period.start
        # 健康度排名：期末未决 warning(2) + critical(16) = 18（生成时快照带设备名）
        assert sub.summary.health_ranking[0].weighted_score == 18
        assert sub.summary.health_ranking[0].equipment_name == "E2E 一号冷机"
    finally:
        await platform.stop()
        server.shutdown()
