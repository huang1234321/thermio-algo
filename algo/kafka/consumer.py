"""aiokafka 消费循环（algo.md §4）：最新值缓存 + 学习闭环留档消费。

两类消费语义（§4.1 表）：
- telemetry.raw @algo-fdd：最新值语义——启动 seek 到 end；lag 超阈 → WARN + 主动 seek end
  （丢的是旧中间值，窗口数据由 TSDB 兜底）；
- control.executed @algo-attribution：逐条消费、不丢（offset 仅在处理成功后提交）。
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.structs import ConsumerRecord

from algo.kafka.topics import (
    GROUP_ALGO_ATTRIBUTION,
    GROUP_ALGO_FDD,
    TOPIC_CONTROL_EXECUTED,
    TelemetryRow,
)
from algo.kafka.topics import TOPIC_TELEMETRY_RAW as RAW
from algo.obs import metrics as mt
from algo.obs.logging import get_logger

log = get_logger(__name__)

_PointKey = tuple[int, str]  # (point_id, iso ts)


class LatestCache:
    """进程内最新值缓存（§4.3，无状态可重建）+ (point_id, ts) 幂等去重 LRU。

    quality ≠ 0 的行写入缓存但打标（M&V 红线：数据永不丢；质量裁决统一在 §5.2 窗口门控）。
    """

    def __init__(self, dedup_capacity: int = 20_000) -> None:
        self._rows: dict[int, TelemetryRow] = {}
        self._seen: OrderedDict[_PointKey, None] = OrderedDict()
        self._capacity = dedup_capacity

    def offer(self, row: TelemetryRow) -> bool:
        """写入（重复 (point_id, ts) 忽略，at-least-once 幂等）。返回是否新行。"""
        key: _PointKey = (row.point_id, row.ts.isoformat())
        if key in self._seen:
            self._seen.move_to_end(key)
            return False
        self._seen[key] = None
        if len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        self._rows[row.point_id] = row
        return True

    def get(self, point_id: int) -> TelemetryRow | None:
        return self._rows.get(point_id)

    def __len__(self) -> int:
        return len(self._rows)


class TelemetryLatestConsumer:
    """telemetry.raw 最新值消费（group algo-fdd）。"""

    def __init__(
        self,
        brokers: list[str],
        cache: LatestCache,
        lag_warn: int = 5000,
        lag_check_interval_s: float = 30.0,
    ) -> None:
        self._brokers = brokers
        self._cache = cache
        self._lag_warn = lag_warn
        self._lag_check_interval_s = lag_check_interval_s
        self._consumer: AIOKafkaConsumer[ConsumerRecord] | None = None
        self._task: asyncio.Task[None] | None = None
        self._lag_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            RAW,
            bootstrap_servers=self._brokers,
            group_id=GROUP_ALGO_FDD,
            auto_offset_reset="latest",  # 最新值语义：启动 seek 到 end（§4.1）
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume_loop(), name="algo-fdd-consumer")
        self._lag_task = asyncio.create_task(self._lag_loop(), name="algo-fdd-lag")

    async def stop(self) -> None:
        for task in (self._task, self._lag_task):
            if task is not None:
                task.cancel()
        for task in (self._task, self._lag_task):
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, KafkaError):
                    pass
        self._task = self._lag_task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                try:
                    row = TelemetryRow.model_validate_json(msg.value)
                except (ValueError, UnicodeDecodeError):
                    log.warning("telemetry.raw 值解析失败，跳过", raw_len=len(msg.value))
                    continue
                self._cache.offer(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("telemetry.raw 消费循环异常退出")
            raise

    async def _lag_loop(self) -> None:
        """lag 自检（§4.1）：超阈 WARN + 主动 seek end。"""
        assert self._consumer is not None
        try:
            while True:
                await asyncio.sleep(self._lag_check_interval_s)
                try:
                    partitions = self._consumer.assignment()
                    if not partitions:
                        continue
                    end = await self._consumer.end_offsets(partitions)
                    total_lag = 0
                    for tp in partitions:
                        pos = await self._consumer.position(tp)
                        total_lag += max(0, end[tp] - pos)
                    mt.KAFKA_CONSUMER_LAG.labels(group=GROUP_ALGO_FDD, topic=RAW).set(total_lag)
                    if total_lag > self._lag_warn:
                        log.warning(
                            "telemetry 消费 lag 超阈，主动 seek end（旧中间值由 TSDB 窗口兜底）",
                            lag=total_lag,
                            threshold=self._lag_warn,
                        )
                        await self._consumer.seek_to_end(*partitions)
                except KafkaError as exc:  # 暂时性集群抖动：本轮自检放弃，下轮再来
                    log.warning("lag 自检失败（下轮重试）", error=str(exc))
        except asyncio.CancelledError:
            raise


class ExecutedEventConsumer:
    """control.executed 学习闭环消费（§4.4 MVP：整条留档 + 计数；契约未定不预实现归因）。"""

    def __init__(
        self,
        brokers: list[str],
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._brokers = brokers
        self._on_event = on_event
        self._consumer: AIOKafkaConsumer[ConsumerRecord] | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_CONTROL_EXECUTED,
            bootstrap_servers=self._brokers,
            group_id=GROUP_ALGO_ATTRIBUTION,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # offset 仅在处理成功后提交（不丢）
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume_loop(), name="algo-attribution")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, KafkaError):
                pass
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                try:
                    payload: dict[str, Any] = json.loads(msg.value)
                    if not isinstance(payload, dict):
                        raise ValueError("非对象 JSON")
                except (ValueError, UnicodeDecodeError):
                    # 坏消息：留档后提交（不阻塞流；契约定稿前的 passthrough 面）
                    log.warning("control.executed 值非 JSON 对象，原样留档后跳过")
                    mt.ATTRIBUTION_EVENTS_TOTAL.inc()
                    await self._consumer.commit()
                    continue
                # 结构化日志整条留档（raw passthrough，unknown 字段容忍）
                hdrs = {
                    k: (v.decode() if isinstance(v, bytes) else v) for k, v in (msg.headers or [])
                }
                # structlog 首位参数即 event 名，载荷键避让（payload 非 event）
                log.info("control.executed", trace_id=hdrs.get("trace_id"), payload=payload)
                mt.ATTRIBUTION_EVENTS_TOTAL.inc()
                if self._on_event is not None:
                    self._on_event(payload)
                await self._consumer.commit()  # 处理成功才提交（at-least-once → 幂等由消费侧保证）
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("control.executed 消费循环异常退出")
            raise
