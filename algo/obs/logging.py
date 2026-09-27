"""structlog JSON 日志（CODE-LOG-03：统一字段；SEC-KEY-02：凭据不落日志）。"""

from __future__ import annotations

import sys

import structlog

# 统一绑定字段（algo.md §10）：业务上下文 + 归因。凭据（SVC_TOKEN_ALGO 等）
# 永不进入日志——本模块不提供任何记录原始 token 的通道（CODE-LOG-01）。
STD_FIELDS = (
    "tenant_id",
    "building_id",
    "equipment_id",
    "point_id",
    "rule_key",
    "trace_id",
    "algo_version",
)


def setup_logging(level: str = "INFO") -> None:
    """进程级初始化：JSON 到 stdout；级别语义 CODE-LOG-02（ERROR=单轮失败不中断服务）。

    注意：本文件名为 logging.py——stdlib logging 一律在函数内延迟导入，
    防包内导入路径混淆（本模块自身不 import 标准库 logging 于模块级）。
    """
    import logging

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """取标准 logger；调用方按需 bind（structlog.contextvars 或 .bind）。"""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
