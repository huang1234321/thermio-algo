"""FDD wire 快照（algo.md §14 contract 层）：提交载荷与读端点的形状钉死。

- 提交面（algo → api）：FddFindingsBatch / FddReportSubmission 字段集 = fdd_finding
  / fdd_report 列的可写投影（ddl.md §9.2 + M6-fdd §3.1/§3.4——api 侧维护列一律不出现在报文）；
- 读面（api → algo）：FddFindingListItem = M6-fdd §4.2 zod schema 的 pydantic 消费侧。
"""

from __future__ import annotations

from datetime import UTC, datetime

from algo.platform.contracts import (
    FddFindingList,
    FddFindingListItem,
    FddFindingsBatch,
    FddReportSubmission,
    FindingCleared,
    FindingHit,
)

T0 = datetime(2026, 9, 27, 4, 30, tzinfo=UTC)

# api 侧维护列（M6-fdd §3.2：algo 报文中出现即拒）——提交模型不得含这些字段
API_OWNED_COLUMNS = {
    "id",
    "tenant_id",
    "status",
    "alarm_event_id",
    "resolved_at",
    "created_at",
    "updated_at",
    "review_result",
    "ignored_by",
}


def test_findings_batch_shape() -> None:
    batch = FddFindingsBatch(
        algo_version="0.1.0+9f3ab2c1",
        hits=[
            FindingHit(
                equipment_id="uuid-eq",
                rule_key="chiller.delta_t_low",
                severity="warning",
                title="1#冷机供回水温差持续低于 1.2°C（负荷 42%）",
                evidence={
                    "points": [
                        {"point_id": 1024, "quantity_type": "chw_supply_temp"},
                        {"point_id": 1025, "quantity_type": "chw_return_temp"},
                    ],
                    "window": {
                        "from": "2026-09-26T14:00:00+08:00",
                        "to": "2026-09-26T14:30:00+08:00",
                    },
                    "detail": {"delta_t_avg_c": 0.9, "load_ratio": 0.42},
                },
                suggested_action="检查蒸发器结垢与负荷侧阀门开度",
                first_detected_at=T0,
                last_detected_at=T0,
            )
        ],
        cleared=[
            FindingCleared(
                equipment_id="uuid-eq", rule_key="cooling_tower.approach_high", cleared_at=T0
            )
        ],
    )
    wire = batch.model_dump(mode="json")
    assert set(wire) == {"algo_version", "hits", "cleared"}
    assert not (set(wire["hits"][0]) & API_OWNED_COLUMNS)  # 零越权写字段
    # window 为 RFC3339 带偏移字符串（§7.5；前端拉曲线的时间锚）
    w_from = wire["hits"][0]["evidence"]["window"]["from"]
    assert datetime.fromisoformat(w_from).utcoffset() is not None
    # severity 五级闭合（ddl.md §9.2 CHECK 同源）
    assert set(FindingHit.model_fields["severity"].annotation.__args__) == {  # type: ignore[attr-defined]
        "info",
        "warning",
        "minor",
        "major",
        "critical",
    }


def test_report_submission_shape() -> None:
    sub = FddReportSubmission.model_validate(
        {
            "building_id": "uuid-b",
            "period_type": "day",
            "period": {"start": "2026-09-26", "end": "2026-09-27"},
            "summary": {
                "counts": {"new": 1, "resolved": 1, "persisting": 2},
                "new_by_severity": {"info": 0, "warning": 1, "minor": 0, "major": 0, "critical": 0},
                "open_by_severity": {
                    "info": 1,
                    "warning": 0,
                    "minor": 1,
                    "major": 0,
                    "critical": 0,
                },
                "health_ranking": [
                    {
                        "equipment_id": "uuid-e",
                        "equipment_name": "1#冷机",
                        "equipment_type": "chiller",
                        "open_count": 2,
                        "weighted_score": 6,
                    }
                ],
            },
            "algo_version": "0.1.0+9f3ab2c1",
        }
    )
    assert set(sub.model_dump()) == {
        "building_id",
        "period_type",
        "period",
        "summary",
        "algo_version",
    }
    assert not (set(sub.model_dump()) & API_OWNED_COLUMNS)


def test_period_wire_regex() -> None:
    """period = 闭开区间两个 YYYY-MM-DD（M6-fdd §4.4 FddPeriod）。"""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FddReportSubmission.model_validate(
            {
                "building_id": "b",
                "period_type": "week",
                "period": {"start": "2026-09-14 00:00", "end": "2026-09-21"},
                "summary": {
                    "counts": {"new": 0, "resolved": 0, "persisting": 0},
                    "new_by_severity": {},
                    "open_by_severity": {},
                    "health_ranking": [],
                },
                "algo_version": "0",
            }
        )


def test_finding_list_item_consumes_m6_shape() -> None:
    """GET /internal/fdd/findings 响应消费（extra=ignore：algo 不消费字段原样容忍）。"""
    payload = {
        "items": [
            {
                "id": "018f0000-0000-7000-8000-000000000001",
                "building_id": "uuid-b",
                "equipment": {
                    "id": "uuid-e",
                    "name": "1#冷机",
                    "local_id": "1#冷机",
                    "equipment_type": "chiller",
                    "future_field": True,
                },
                "rule_key": "chiller.delta_t_low",
                "severity": "warning",
                "status": "open",
                "title": "t",
                "suggested_action": None,
                "algo_version": "0.1.0+9f3ab2c1",
                "first_detected_at": "2026-09-26T14:00:00+08:00",
                "last_detected_at": "2026-09-26T14:30:00+08:00",
                "resolved_at": None,
                "ignored_at": None,
                "review": None,
                "created_at": "2026-09-26T14:00:00+08:00",
                "list_only_future_field": 1,  # 形状演进容忍
            }
        ],
        "next_cursor": None,
    }
    parsed = FddFindingList.model_validate(payload)
    item = parsed.items[0]
    assert isinstance(item, FddFindingListItem)
    assert item.equipment.equipment_type == "chiller"
