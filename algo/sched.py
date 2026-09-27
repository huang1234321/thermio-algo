"""APScheduler 装配（algo.md §3 任务表；jitter/misfire 策略）。

调度纪律：所有 job max_instances=1、coalesce=True、misfire_grace_time=60s——
评估轮次可跳过不可重叠（迟滞计数以「轮次」为单位，重叠会双计）。
时区默认 Asia/Shanghai（env ALGO_TZ）。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

JobFn = Callable[[], Awaitable[Any]]


def build_scheduler(
    jobs: dict[str, tuple[str, JobFn]],  # job_id → (cron 表达式, 任务体)
    tz_name: str,
) -> AsyncIOScheduler:
    """五段 cron；jitter 5s 对齐墙钟桶边界的评估节奏（§3 表首行）。"""
    tz = ZoneInfo(tz_name)
    sched = AsyncIOScheduler(
        timezone=tz,
        job_defaults={
            "max_instances": 1,
            "coalesce": True,
            "misfire_grace_time": 60,
        },
    )
    for job_id, (cron, fn) in jobs.items():
        minute, hour, day, month, day_of_week = cron.split()
        sched.add_job(
            fn,
            CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=tz,
                jitter=5,
            ),
            id=job_id,
            name=job_id,
        )
    return sched


# 任务表（§3；触发默认值，首版启用状态在 main.py 按 ALGO_JOBS_ENABLED 装配）
JOB_TRIGGERS: dict[str, str] = {
    "fdd_eval": "*/5 * * * *",
    "fdd_report_day": "10 0 * * *",
    "fdd_report_week": "20 0 * * 1",
    "forecast_15m": "*/15 * * * *",  # 槽位预留（IMPL-19 离线验证，默认禁用）
    "optimize_15m": "*/15 * * * *",  # 槽位预留（DAT-126/IMPL-19，默认禁用）
    "weather_actual": "5 * * * *",
    "weather_forecast": "35 * * * *",
    "snapshot_refresh": "*/5 * * * *",
}
