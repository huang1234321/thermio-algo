"""Kafka 侧（algo/kafka）：topic 常量、消息模型与消费循环（algo.md §4）。"""

from algo.kafka.topics import (
    TOPIC_CONTROL_EXECUTED,
    TOPIC_TELEMETRY_QUALITY,
    TOPIC_TELEMETRY_RAW,
)

__all__ = [
    "TOPIC_CONTROL_EXECUTED",
    "TOPIC_TELEMETRY_QUALITY",
    "TOPIC_TELEMETRY_RAW",
]
