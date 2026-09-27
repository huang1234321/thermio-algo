"""ProposalEnvelope pydantic（algo.md §11.1 逐字段 = shared-types proposal.ts）。

对照物 = thermio-platform packages/shared-types/src/proposal.ts（commit 8b8297f，
2026-09-26）。字段级差异由 tests/contract/test_proposal_envelope_snapshot.py 钉死：
**改信封先改 shared-types（发版），快照随对照 PR 同步**（TS-02 精神）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProposalAction(BaseModel):
    """写什么值（op 保持开放——不自造闭合枚举，shared-types 注释原文）。"""

    model_config = ConfigDict(extra="forbid")

    op: str = Field(min_length=1)
    value: float = Field(allow_inf_nan=False)  # JSON 数值域（z.number() 对应）
    unit: str = Field(min_length=1)


class ProposalTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    equipment_id: str = Field(min_length=1)
    point: str = Field(min_length=1)  # 点位语义标识（quantity_type 级）


class ProposalEnvelope(BaseModel):
    """wire 信封。extra=forbid 是自我约束（zod 默认剥离未知键，多发字段 api 剥离不拒）。"""

    model_config = ConfigDict(extra="forbid")

    proposal_id: str = Field(min_length=1)  # 客户端生成，幂等键
    algo: str = Field(min_length=1)  # 能力路径，如 optimizer/chiller-sequencer
    algo_version: str = Field(min_length=1)
    target: ProposalTarget
    action: ProposalAction
    previous_value: float = Field(allow_inf_nan=False)  # 必填数值、不可 null（§11.1 备注）
    rationale: str = Field(min_length=1)  # 必填（可解释，否则运维不采纳）
    expected_saving_kw: float = Field(allow_inf_nan=False)  # 必填（带预期收益）
    confidence: float = Field(ge=0, le=1)
    evidence: dict[str, Any]
    expires_at: datetime  # RFC3339 带时区偏移（naive 拒绝，见校验器）

    @field_validator("expires_at")
    @classmethod
    def _require_offset(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            msg = "expires_at 必须带时区偏移（RFC3339，不接受 naive）"
            raise ValueError(msg)
        return v
