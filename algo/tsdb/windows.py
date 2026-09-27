"""FDD 窗口查询（algo.md §5.2：cagg 优先 + 质量门控——唯一高频模式）。

窗口聚合一律查 telemetry_5min 视图（cagg real-time 聚合开：近窗由原始表现场合并，
查视图即得完整数据）；原始表只做点查（client.latest_telemetry）。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from algo.fdd.base import BucketRow
from algo.tsdb.client import TsdbClient

# ingest.md §4 位定义 v1 冻结：bit4 = unit_unconverted（单位未归一，桶不可比）
QUALITY_BIT_UNIT_UNCONVERTED = 1 << 4

_WINDOW_SQL = """
SELECT point_id, bucket, avg, min, max, last, stddev, sample_count, bad_count, quality_mask
FROM telemetry_5min
WHERE point_id = ANY($1::bigint[])
  AND bucket >= $2 AND bucket < $3
ORDER BY bucket
"""


async def fetch_buckets(
    tsdb: TsdbClient, point_ids: Sequence[int], window_from: datetime, window_to: datetime
) -> dict[int, list[BucketRow]]:
    """拉原始（未门控）桶序列，按 point_id 分组。右开区间 [from, to)，对齐桶边界。"""
    if not point_ids:
        return {}
    rows = await tsdb.fetch(_WINDOW_SQL, list(point_ids), window_from, window_to)
    out: dict[int, list[BucketRow]] = {pid: [] for pid in point_ids}
    for r in rows:
        out[r["point_id"]].append(BucketRow(**dict(r)))
    return out


def gate_bucket(bucket: BucketRow, min_good_ratio: float) -> bool:
    """质量门控（§5.2）：坏样本占比超限或 bit4 置位的桶不进规则计算。

    sample_count == 0 的桶视为无数据（视图聚合行本就不该出现，防御性剔除）。
    """
    if bucket.sample_count <= 0:
        return False
    good_ratio = 1.0 - (bucket.bad_count / bucket.sample_count)
    if good_ratio < min_good_ratio:
        return False
    if bucket.quality_mask & QUALITY_BIT_UNIT_UNCONVERTED:
        return False
    return True


def gated_series(series: Sequence[BucketRow], min_good_ratio: float) -> list[BucketRow]:
    """对单点位序列应用质量门控（剔除 = 由调用方转译为 InsufficientData，不硬算）。"""
    return [b for b in series if gate_bucket(b, min_good_ratio)]
