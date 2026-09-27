"""资产快照缓存服务（algo.md §6.3）：5min 增量 + 每小时全量对账 + 陈旧度指标。

对 PG 零直连的必然后果（§6.1）：映射只能从中台 HTTP 拉（GET /internal/algo/asset-snapshot）。
刷新失败用旧快照服务并打 algo_snapshot_age_s。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.platform.client import PlatformClient
from algo.semantics.types import AssetSnapshot

log = get_logger(__name__)

INCREMENTAL_INTERVAL = timedelta(minutes=5)
FULL_RESYNC_INTERVAL = timedelta(hours=1)


class SnapshotService:
    """进程内快照缓存；refresh(incremental=True|False) 由调度任务驱动（§3 snapshot_refresh）。"""

    def __init__(self, client: PlatformClient) -> None:
        self._client = client
        self._snapshot: AssetSnapshot | None = None
        self._generated_at: datetime | None = None
        self._last_full: datetime | None = None

    @property
    def current(self) -> AssetSnapshot | None:
        return self._snapshot

    def age_s(self) -> float:
        if self._generated_at is None:
            return float("inf")
        return max(0.0, time.time() - self._generated_at.timestamp())

    async def refresh(self, *, full: bool, now: datetime | None = None) -> AssetSnapshot:
        """增量/全量刷新（同 ingest.md §6.2 节奏）；失败保留旧快照（本方法抛出由任务层记）。

        增量以快照 generated_at 为 updated_since 游标（服务端以 point/equipment.updated_at
        为准投影，§6.2）；每小时整点全量对账兜底删除漂移。
        """
        now = now or datetime.now().astimezone()
        try:
            since = self._generated_at if (not full and self._generated_at) else None
            snap = await self._client.get_asset_snapshot(updated_since=since)
            if since is None or full:
                self._snapshot = snap  # 全量：整体替换
            else:
                self._snapshot = merge_snapshot(self._snapshot, snap)  # 增量：按 id 合并
            self._generated_at = snap.generated_at
            if full:
                self._last_full = now
        finally:
            mt.SNAPSHOT_AGE_S.set(self.age_s() if self.age_s() != float("inf") else -1)
        assert self._snapshot is not None
        return self._snapshot

    async def ensure_loaded(self) -> AssetSnapshot:
        """引擎取数入口：冷启动首次拉全量（调度任务之外的自愈路径）。"""
        if self._snapshot is None:
            return await self.refresh(full=True)
        return self._snapshot

    def should_full_resync(self, now: datetime) -> bool:
        if self._last_full is None:
            return True
        return (now - self._last_full) >= FULL_RESYNC_INTERVAL


def merge_snapshot(base: AssetSnapshot | None, delta: AssetSnapshot) -> AssetSnapshot:
    """增量合并：设备/点位按主键覆盖更新；全量对账负责清理删除行。"""
    if base is None:
        return delta
    eq = {e.equipment_id: e for e in base.equipments}
    eq.update({e.equipment_id: e for e in delta.equipments})
    pts = {p.point_id: p for p in base.points}
    pts.update({p.point_id: p for p in delta.points})
    return AssetSnapshot(
        generated_at=delta.generated_at,
        equipments=sorted(eq.values(), key=lambda e: e.equipment_id),
        points=sorted(pts.values(), key=lambda p: p.point_id),
    )


async def snapshot_refresh_job(service: SnapshotService) -> None:
    """调度任务 snapshot_refresh（§3）：5min 增量；整点到点全量。"""
    now = datetime.now().astimezone()
    full = service.should_full_resync(now)
    try:
        await service.refresh(full=full, now=now)
        log.info("snapshot_refresh_ok", full=full, age_s=round(service.age_s(), 1))
    except Exception:
        # 刷新失败：旧快照继续服务（§6.3），陈旧度指标已可见
        log.exception("snapshot_refresh_failed", full=full, serving_stale=True)
