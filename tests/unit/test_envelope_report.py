"""信封模型约束（§11.1 表逐行）+ 报告聚合分类判据（M6-fdd §4.4）。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from algo.fdd.report import classify, previous_day, previous_week
from algo.platform.contracts import (
    FddFindingListItem,
    FindingEquipmentRef,
    ReportPeriod,
)
from algo.proposal.envelope import ProposalAction, ProposalEnvelope, ProposalTarget
from pydantic import ValidationError

T0 = datetime(2026, 9, 27, 4, 0, tzinfo=UTC)


def env(**kw: object) -> ProposalEnvelope:
    base = {
        "proposal_id": "pp_01J",
        "algo": "optimizer/chiller-sequencer",
        "algo_version": "0.1.0",
        "target": ProposalTarget(equipment_id="eq-1", point="chw_supply_temp_setpoint"),
        "action": ProposalAction(op="set", value=7.0, unit="degC"),
        "previous_value": 8.0,
        "rationale": "低负荷抬高出水温度",
        "expected_saving_kw": 12.5,
        "confidence": 0.7,
        "evidence": {"load_ratio": 0.4},
        "expires_at": T0 + timedelta(minutes=15),
    }
    base.update(kw)
    return ProposalEnvelope.model_validate(base)  # type: ignore[arg-type]


def test_envelope_ok_and_extra_forbid() -> None:
    e = env()
    assert e.action.value == 7.0
    with pytest.raises(ValidationError):
        env(extra_field="x")  # extra=forbid（§11.1：自我约束，不改 wire 兼容性）


def test_envelope_numeric_domain() -> None:
    for bad in ("inf", "-inf", "nan"):
        v = float(bad)
        with pytest.raises(ValidationError):
            env(action=ProposalAction(op="set", value=v, unit="degC"))
        with pytest.raises(ValidationError):
            env(previous_value=v)


def test_envelope_confidence_bounds() -> None:
    env(confidence=0.0)
    env(confidence=1.0)
    with pytest.raises(ValidationError):
        env(confidence=1.5)
    with pytest.raises(ValidationError):
        env(confidence=-0.1)


def test_envelope_previous_value_required() -> None:
    """previous_value 必填数值、不可 null（§11.1 备注：无当前值则不产 proposal）。"""
    with pytest.raises(ValidationError):
        ProposalEnvelope.model_validate(env().model_dump(exclude={"previous_value"}))


def test_envelope_expires_at_requires_offset() -> None:
    naive = (T0 + timedelta(minutes=15)).replace(tzinfo=None)
    with pytest.raises(ValidationError):
        env(expires_at=naive)  # RFC3339 带时区偏移（不接受 naive）


def test_envelope_min_lengths() -> None:
    with pytest.raises(ValidationError):
        env(rationale="")
    with pytest.raises(ValidationError):
        env(proposal_id="")


# ── 报告聚合 ────────────────────────────────────────────────────────────────


def finding(
    eq_id: str,
    first: datetime,
    *,
    resolved: datetime | None = None,
    ignored: datetime | None = None,
    severity: str = "warning",
) -> FddFindingListItem:
    return FddFindingListItem(
        id=f"f-{eq_id}-{first.timestamp():.0f}",
        building_id="b-1",
        equipment=FindingEquipmentRef(
            id=eq_id, name=f"设备{eq_id}", local_id=None, equipment_type="chiller"
        ),
        rule_key="chiller.delta_t_low",
        severity=severity,  # type: ignore[arg-type]
        status="resolved" if resolved else "open",  # type: ignore[arg-type]
        title="t",
        algo_version="0.1.0+abcd1234",
        first_detected_at=first,
        last_detected_at=first,
        resolved_at=resolved,
        ignored_at=ignored,
        created_at=first,
    )


D1 = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)  # 期内
D2 = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)  # 期前
END_PLUS = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)  # 期后消除


def test_classify_counts() -> None:
    findings = [
        finding("eq1", D1),  # new + persisting
        finding("eq2", D2),  # persisting（期前首见、期末未决）
        finding("eq3", D2, resolved=D1),  # resolved（期内消除）
        finding("eq4", D2, resolved=D2),  # 期前已消：都不计
        finding("eq5", D1, resolved=END_PLUS),  # new + persisting（期后才消）
    ]
    s = classify(findings, start=date(2026, 9, 26), end=date(2026, 9, 27))
    assert s.counts.new == 2  # eq1、eq5 首见 ∈ 期
    assert s.counts.resolved == 1  # eq3 期内消除；eq5 期后才消不计
    assert s.counts.persisting == 3  # eq1、eq2、eq5 期末仍未决


def test_classify_severity_maps_all_five_keys() -> None:
    s = classify(
        [finding("eq1", D1, severity="critical")],
        start=date(2026, 9, 26),
        end=date(2026, 9, 27),
    )
    assert set(s.new_by_severity) == {"info", "warning", "minor", "major", "critical"}
    assert s.new_by_severity["critical"] == 1
    assert s.new_by_severity["info"] == 0  # 0 也给（前端免兜底）
    assert set(s.open_by_severity) == {"info", "warning", "minor", "major", "critical"}


def test_classify_health_ranking_weighted_top10() -> None:
    findings = [
        finding("eqA", D1, severity="critical"),  # 16
        finding("eqB", D1, severity="warning"),  # 2
        finding("eqA", D2, severity="major"),  # +8 → eqA=24
    ]
    findings += [finding(f"eq{i:02d}", D1, severity="info") for i in range(3, 15)]
    s = classify(findings, start=date(2026, 9, 26), end=date(2026, 9, 27))
    assert len(s.health_ranking) == 10  # top10 截断
    assert s.health_ranking[0].equipment_id == "eqA"  # 24 分居首
    assert s.health_ranking[0].weighted_score == 24
    assert s.health_ranking[0].open_count == 2
    assert s.health_ranking[0].equipment_name == "设备eqA"


def test_period_helpers() -> None:
    now = datetime(2026, 9, 27, 15, 0, tzinfo=UTC)  # 周日
    assert previous_day(now) == (date(2026, 9, 26), date(2026, 9, 27))
    ws, we = previous_week(now)
    assert (ws, we) == (date(2026, 9, 14), date(2026, 9, 21))  # [周一, 次周一)
    assert ReportPeriod(start=ws.isoformat(), end=we.isoformat())
