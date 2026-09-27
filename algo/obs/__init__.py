"""可观测性（algo/obs）：structlog JSON 日志 + prometheus 指标（algo.md §10）。"""

from algo.obs.logging import STD_FIELDS, get_logger, setup_logging

__all__ = ["STD_FIELDS", "get_logger", "setup_logging"]
