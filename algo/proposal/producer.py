"""ProposalProducer 协议 + 提交语义（algo.md §11.2/§11.3——DAT-126 实现此接口）。

优化器不做任何 I/O（produce 纯函数）；提交/幂等/重试由本模块承担。
algo 的职责在「提交 proposal、收到 201」处截断（ADR-009；§11.4 仲裁侧接口 = 无）。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from ulid import ULID

from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.platform.client import PlatformClient, PlatformRejectedError
from algo.proposal.envelope import ProposalEnvelope

log = get_logger(__name__)

# 提交重试窗口（§11.3）：临期不提交（避免落地即过期；过期沉降是 api 后台任务，algo 不抢跑）
SUBMIT_MIN_TTL = timedelta(minutes=2)


def new_proposal_id() -> str:
    """§11.3：proposal_id = "pp_" + ULID()——一次生成、重试复用（幂等键稳定）。"""
    return f"pp_{ULID()}"


@runtime_checkable
class ProposalProducer(Protocol):
    """优化器实现本协议；引擎负责装配与提交（DAT-126 引用点，§11.2）。"""

    algo: str  # 如 "optimizer/chiller-sequencer"
    algo_version: str

    def produce(self, ctx: Any) -> Sequence[ProposalEnvelope] | None:
        """返回零或多条已完备信封；目标点位/previous_value/clamp 自检失败 → None/空。
        优化器不做任何 I/O——数据一律由 ctx 给足（与 Rule.evaluate 同款纯函数纪律）。
        """
        ...


class ProposalSubmitter:
    def __init__(self, platform: PlatformClient) -> None:
        self._platform = platform

    async def submit(
        self,
        envelopes: Sequence[ProposalEnvelope],
        *,
        now: datetime | None = None,
    ) -> list[ProposalEnvelope]:
        """逐条提交 → 201；返回成功集（失败语义：宁缺毋滥——过期前不会有半态）。

        - 临期自检（§11.3）：expires_at ≤ now + SUBMIT_MIN_TTL → 丢弃（expired 计数）；
        - 传输重试在 PlatformClient（幂等端点有界重试）；
        - 业务拒绝（4xx）→ rejected 计数 + ERROR 日志，不中断同批其余。
        """
        now = now or datetime.now().astimezone()
        accepted: list[ProposalEnvelope] = []
        for env in envelopes:
            if env.expires_at <= now + SUBMIT_MIN_TTL:
                mt.PROPOSAL_SUBMISSIONS_TOTAL.labels(result="expired").inc()
                log.warning("proposal_skipped_near_expiry", proposal_id=env.proposal_id)
                continue
            try:
                await self._platform.submit_proposal(env.model_dump(mode="json"))
            except PlatformRejectedError:
                mt.PROPOSAL_SUBMISSIONS_TOTAL.labels(result="rejected").inc()
                continue
            except Exception:
                mt.PROPOSAL_SUBMISSIONS_TOTAL.labels(result="dropped").inc()
                continue
            mt.PROPOSAL_SUBMISSIONS_TOTAL.labels(result="created").inc()
            accepted.append(env)
        return accepted
