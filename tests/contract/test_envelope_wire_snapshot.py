"""信封快照测试（algo.md §14 contract 层）：钉死与 shared-types 的字段清单与约束。

对照物 = thermio-platform packages/shared-types/src/proposal.ts @ commit 8b8297f
（2026-09-26）。**改信封先改 shared-types（发版），本快照随对照 PR 同步**——
跨仓无联合 CI，对齐靠双侧快照 + 评审（platform.md §6.4 机制延伸）。

同步步骤：shared-types proposal.ts 变更 → 更新下方 SHARED_TYPES_FIELDS 与
约束断言 → PR 描述附双仓 commit 对（本仓 @X + platform @Y）。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from algo.proposal.envelope import ProposalEnvelope
from pydantic import ValidationError

# shared-types ProposalEnvelopeSchema z.object 键集（zod 默认 Strip——未知键剥离不报错；
# pydantic 侧 extra=forbid 是自我约束，不构成 wire 不兼容，见 §11.1）
SHARED_TYPES_FIELDS: dict[str, set[str]] = {
    "envelope": {
        "proposal_id",
        "algo",
        "algo_version",
        "target",
        "action",
        "previous_value",
        "rationale",
        "expected_saving_kw",
        "confidence",
        "evidence",
        "expires_at",
    },
    "target": {"equipment_id", "point"},
    "action": {"op", "value", "unit"},
}

T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def valid_payload() -> dict[str, object]:
    return {
        "proposal_id": "pp_01J...",
        "algo": "optimizer/chiller-sequencer",
        "algo_version": "0.1.0+9f3ab2c1",
        "target": {"equipment_id": "uuid-1", "point": "chw_supply_temp_setpoint"},
        "action": {"op": "set", "value": 7.0, "unit": "degC"},
        "previous_value": 8.0,
        "rationale": "低负荷抬高出水温度",
        "expected_saving_kw": 12.5,
        "confidence": 0.7,
        "evidence": {"k": "v"},
        "expires_at": (T0 + timedelta(minutes=15)).isoformat(),
    }


def test_field_list_pinned_to_shared_types() -> None:
    """字段集逐字段对照（多一个少一个都红灯——发版动作的 CI 面）。"""
    e = ProposalEnvelope.model_validate(valid_payload())
    assert set(e.model_dump().keys()) == SHARED_TYPES_FIELDS["envelope"]
    assert set(e.target.model_dump().keys()) == SHARED_TYPES_FIELDS["target"]
    assert set(e.action.model_dump().keys()) == SHARED_TYPES_FIELDS["action"]


@pytest.mark.parametrize(
    ("field", "mutate"),
    [
        ("proposal_id", lambda p: p.update(proposal_id="")),
        ("algo", lambda p: p.update(algo="")),
        ("algo_version", lambda p: p.update(algo_version="")),
        ("rationale", lambda p: p.update(rationale="")),
        ("confidence>1", lambda p: p.update(confidence=1.01)),
        ("confidence<0", lambda p: p.update(confidence=-0.01)),
        ("value NaN", lambda p: p["action"].update(value=float("nan"))),  # type: ignore[union-attr]
        ("previous_value inf", lambda p: p.update(previous_value=float("inf"))),
        ("expires naive", lambda p: p.update(expires_at="2026-09-27T12:15:00")),
    ],
)
def test_constraints_pinned(field: str, mutate) -> None:  # type: ignore[no-untyped-def]
    """zod 约束逐条对应（§11.1 对照表右列）。"""
    payload = valid_payload()
    mutate(payload)
    with pytest.raises(ValidationError):
        ProposalEnvelope.model_validate(payload)


def test_roundtrip_json_wire() -> None:
    """wire 序列化稳定（datetime → RFC3339 带偏移；evidence 自由结构保留）。"""
    e = ProposalEnvelope.model_validate(valid_payload())
    wire = e.model_dump_json()
    e2 = ProposalEnvelope.model_validate_json(wire)
    assert e2 == e
    assert '"expires_at":"2026-09-27T12:15:00Z"' in wire  # pydantic v2 UTC → Z
