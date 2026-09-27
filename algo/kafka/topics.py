"""topic 常量 + 消息 pydantic 模型（ingest.md §7.4 值结构，逐字段）。"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

# ADR-004 topic 清单（algo 消费面）；quality 暂不订阅（§4.1 表，预留）。
TOPIC_TELEMETRY_RAW = "thermio.telemetry.raw"
TOPIC_TELEMETRY_QUALITY = "thermio.telemetry.quality"
TOPIC_CONTROL_EXECUTED = "thermio.control.executed"

# ADR-004：每个算法能力独立 consumer group。
GROUP_ALGO_FDD = "algo-fdd"
GROUP_ALGO_OPTIMIZER = "algo-optimizer"
GROUP_ALGO_ATTRIBUTION = "algo-attribution"


class TelemetryRow(BaseModel):
    """thermio.telemetry.raw 清洗后行（ingest.md §7.4 值结构；枚举只增不改→未知字段容忍）。"""

    model_config = ConfigDict(extra="ignore")

    point_id: int
    gateway_id: str
    tenant_id: str
    ts: datetime  # RFC3339 带时区；采集真实时刻
    value: float | None = None
    value_text: str | None = None
    quality: int = 0  # 位掩码，0=good（ingest.md §4）
