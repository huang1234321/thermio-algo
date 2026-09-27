"""internal HTTP 客户端（algo.md §8.1）：Bearer SVC_TOKEN_ALGO、有界重试、fail-fast 凭证错。

纪律：
- 401/403 = 凭证问题 → fail-fast（不重试；ERROR + algo_platform_auth_failures_total）；
- 429 → 尊重 Retry-After，指数退避上限 3 次；
- 5xx/超时 → 幂等端点有界重试，耗尽本轮丢弃 + ERROR（FDD 下轮 upsert 自愈）；
- trace_id 请求头贯穿（ADR-017 首期折中）；token 永不落日志（SEC-KEY-02）。
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any

import httpx

from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.platform.contracts import (
    FddFindingList,
    FddFindingsBatch,
    FddReportSubmission,
)
from algo.semantics.types import AssetSnapshot

log = get_logger(__name__)

RETRY_MAX = 3
RETRY_BACKOFF_S = 1.0


class PlatformAuthError(RuntimeError):
    """internal 401/403（auth.service_unauthorized）——配置错误，重试救不了。"""


class PlatformRejectedError(RuntimeError):
    """4xx 业务拒绝（payload 不合规等）——不重试。"""


class PlatformClient:
    def __init__(
        self,
        base_url: str,
        svc_token: str,
        timeout_s: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # transport 注入面仅供测试替身（CODE-TST-02）；生产路径不传。
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"authorization": f"Bearer {svc_token}"},
            timeout=timeout_s,
            transport=transport,
        )

    async def start(self) -> None:  # 装配占位（httpx 惰性建连）
        await self._client.__aenter__()

    async def stop(self) -> None:
        await self._client.aclose()

    # ── 端点面（platform.md §11 表：algo 白名单四写两读）────────────────────

    async def get_asset_snapshot(self, updated_since: datetime | None = None) -> AssetSnapshot:
        params: dict[str, str] = {}
        if updated_since is not None:
            params["updated_since"] = updated_since.isoformat()
        data = await self._call("GET", "/internal/algo/asset-snapshot", params=params)
        return AssetSnapshot.model_validate(data)

    async def submit_fdd_findings(self, batch: FddFindingsBatch) -> None:
        await self._call(
            "POST", "/internal/fdd/findings", json_payload=batch.model_dump(mode="json")
        )

    async def submit_fdd_report(self, report: FddReportSubmission) -> None:
        await self._call(
            "POST", "/internal/fdd/reports", json_payload=report.model_dump(mode="json")
        )

    async def submit_proposal(self, proposal: dict[str, Any]) -> None:
        """POST /internal/proposals（§11.3：201；rationale/expected_saving_kw 校验在 api）。"""
        await self._call("POST", "/internal/proposals", json_payload=proposal)

    async def list_fdd_findings(
        self,
        building_id: str,
        from_: datetime,
        to: datetime,
        limit: int = 200,
        cursor: str | None = None,
    ) -> FddFindingList:
        params: dict[str, str | int] = {
            "building_id": building_id,
            "from": from_.isoformat(),
            "to": to.isoformat(),
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor
        data = await self._call("GET", "/internal/fdd/findings", params=params)
        return FddFindingList.model_validate(data)

    # ── 传输面 ─────────────────────────────────────────────────────────────

    async def _call(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_payload: dict[str, Any] | None = None,
    ) -> Any:
        trace_id = uuid.uuid4().hex
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = await self._client.request(
                    method,
                    path,
                    params=params,
                    json=json_payload,
                    headers={"trace_id": trace_id},
                )
            except httpx.TimeoutException as exc:
                if attempt > RETRY_MAX:
                    log.error(
                        "internal_call_timeout_dropped",
                        path=path,
                        trace_id=trace_id,
                        attempts=attempt,
                        error=str(exc),
                    )
                    msg = f"internal {path} 超时重试耗尽"
                    raise RuntimeError(msg) from exc
                await asyncio.sleep(RETRY_BACKOFF_S * 2 ** (attempt - 1))
                continue

            if resp.status_code in (200, 201):
                return resp.json() if resp.content else None

            if resp.status_code in (401, 403):
                mt.PLATFORM_AUTH_FAILURES_TOTAL.inc()
                log.error(
                    "internal_auth_failed",
                    path=path,
                    status=resp.status_code,
                    trace_id=trace_id,
                )
                msg = f"internal {path} 凭证失败（{resp.status_code}），fail-fast"
                raise PlatformAuthError(msg)

            if resp.status_code == 429 and attempt <= RETRY_MAX:
                retry_after = float(resp.headers.get("retry-after", RETRY_BACKOFF_S))
                await asyncio.sleep(min(retry_after, 30.0))
                continue

            if resp.status_code >= 500 and attempt <= RETRY_MAX:
                await asyncio.sleep(RETRY_BACKOFF_S * 2 ** (attempt - 1))
                continue

            # 其余 4xx：业务拒绝，不重试（api 侧校验语义）
            log.error(
                "internal_call_rejected",
                path=path,
                status=resp.status_code,
                trace_id=trace_id,
                body=resp.text[:500],
            )
            msg = f"internal {path} 拒绝（{resp.status_code}）: {resp.text[:200]}"
            raise PlatformRejectedError(msg)
