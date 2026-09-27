"""PlatformClient 传输语义（§8.1）+ ProposalSubmitter 提交语义（§11.3）。

httpx.MockTransport 假外部服务（CODE-TST-02）——无真实网络。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from algo.obs import metrics as mt
from algo.platform.client import PlatformAuthError, PlatformClient, PlatformRejectedError
from algo.platform.contracts import FddFindingsBatch
from algo.proposal.envelope import ProposalAction, ProposalEnvelope, ProposalTarget
from algo.proposal.producer import SUBMIT_MIN_TTL, ProposalSubmitter, new_proposal_id
from algo.semantics.types import AssetSnapshot

T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def make_client(
    handler,
    base: str = "http://mock",  # type: ignore[no-untyped-def]
) -> PlatformClient:
    return PlatformClient(base, "tok", transport=httpx.MockTransport(handler))


async def test_auth_header_and_snapshot_parse() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization", "")
        seen["trace"] = request.headers.get("trace_id", "")
        seen["since"] = dict(request.url.params).get("updated_since", "")
        return httpx.Response(
            200,
            json={
                "generated_at": "2026-09-27T12:00:00+08:00",
                "equipments": [
                    {
                        "equipment_id": "eq-1",
                        "equipment_type": "chiller",
                        "rated_params": {"rated_power_kw": 50},
                        "future_field": 1,  # extra=ignore：字段只增不删
                    }
                ],
                "points": [
                    {
                        "point_id": 1,
                        "equipment_id": "eq-1",
                        "quantity_type": "chw_supply_temp",
                        "unit_std": "degC",
                    }
                ],
            },
        )

    c = make_client(handler)
    snap = await c.get_asset_snapshot(updated_since=datetime(2026, 9, 27, 11, 0, tzinfo=UTC))
    assert isinstance(snap, AssetSnapshot)
    assert snap.equipments[0].rated_params == {"rated_power_kw": 50}
    assert seen["auth"] == "Bearer tok"
    assert len(seen["trace"]) == 32  # trace_id 贯穿（ADR-017 首期折中）
    assert seen["since"].startswith("2026-09-27T11:00")


async def test_401_fail_fast_no_retry() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(401, json={"reason_code": "auth.service_unauthorized"})

    c = make_client(handler)
    before = mt.PLATFORM_AUTH_FAILURES_TOTAL._value.get()
    with pytest.raises(PlatformAuthError):
        await c.submit_fdd_findings(FddFindingsBatch(algo_version="0.1.0"))
    assert calls["n"] == 1  # fail-fast：凭证错误不重试
    assert mt.PLATFORM_AUTH_FAILURES_TOTAL._value.get() == before + 1


async def test_5xx_bounded_retry_then_success() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, text="boom")
        return httpx.Response(201, json={"ok": True})

    c = make_client(handler)
    await c.submit_fdd_findings(FddFindingsBatch(algo_version="0.1.0"))
    assert calls["n"] == 3  # 有界重试后成功


async def test_4xx_rejected_no_retry() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(422, json={"reason_code": "common.validation_failed"})

    c = make_client(handler)
    with pytest.raises(PlatformRejectedError):
        await c._call("POST", "/internal/fdd/reports", json_payload={})
    assert calls["n"] == 1


# ── ProposalSubmitter ───────────────────────────────────────────────────────


def envelope(expires: datetime, pid: str | None = None) -> ProposalEnvelope:
    return ProposalEnvelope(
        proposal_id=pid or new_proposal_id(),
        algo="optimizer/chiller-sequencer",
        algo_version="0.1.0",
        target=ProposalTarget(equipment_id="eq-1", point="chw_supply_temp_setpoint"),
        action=ProposalAction(op="set", value=7.0, unit="degC"),
        previous_value=8.0,
        rationale="r",
        expected_saving_kw=1.0,
        confidence=0.5,
        evidence={},
        expires_at=expires,
    )


async def test_proposal_id_format() -> None:
    pid = new_proposal_id()
    assert pid.startswith("pp_") and len(pid) > 10


async def test_submitter_near_expiry_dropped() -> None:
    """临期自检（§11.3）：expires_at ≤ now+2min → 不提交（避免落地即过期）。"""
    posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(json.loads(request.content))
        return httpx.Response(201)

    sub = ProposalSubmitter(make_client(handler))
    now = T0
    ok = await sub.submit(
        [
            envelope(now + SUBMIT_MIN_TTL + timedelta(minutes=1), "pp_keep"),
            envelope(now + timedelta(seconds=30), "pp_drop"),
        ],
        now=now,
    )
    assert [e.proposal_id for e in ok] == ["pp_keep"]
    assert len(posts) == 1  # 临期条目未触网


async def test_submitter_rejection_isolated() -> None:
    """422 拒绝不中断同批其余（宁缺毋滥，失败可见）。"""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body["proposal_id"] == "pp_bad":
            return httpx.Response(422, json={"reason_code": "common.validation_failed"})
        return httpx.Response(201)

    sub = ProposalSubmitter(make_client(handler))
    ok = await sub.submit(
        [
            envelope(T0 + timedelta(minutes=10), "pp_bad"),
            envelope(T0 + timedelta(minutes=10), "pp_ok"),
        ],
        now=T0,
    )
    assert [e.proposal_id for e in ok] == ["pp_ok"]
