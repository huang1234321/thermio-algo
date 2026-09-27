"""入口：装配配置/日志/指标 → scheduler + consumers → 优雅退出（algo.md §2）。

运行形态（deploy.md §1）：compose 中间件栈 + 宿主 `python -m algo.main`（热迭代不重建镜像）。
--once <job>：单轮执行后退出（运维/集成测试用；不经 scheduler）。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal

from prometheus_client import start_http_server

from algo import __version__
from algo.config import Settings, get_settings
from algo.fdd.engine import FddEngine
from algo.fdd.report import FddReportGenerator
from algo.fdd.thresholds import ThresholdStore
from algo.kafka.consumer import ExecutedEventConsumer, LatestCache, TelemetryLatestConsumer
from algo.obs.logging import get_logger, setup_logging
from algo.platform.client import PlatformClient
from algo.sched import JOB_TRIGGERS, JobFn, build_scheduler
from algo.semantics.snapshot import SnapshotService, snapshot_refresh_job
from algo.tsdb.client import TsdbClient
from algo.weather.job import WeatherService
from algo.weather.providers import build_provider

log = get_logger(__name__)


class App:
    """全部服务件的装配与生命周期（main 的实体；测试经 build_app 复用装配）。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.tsdb = TsdbClient(settings.tsdb_dsn)
        self.platform = PlatformClient(settings.api_internal_base_url, settings.svc_token_algo)
        self.snapshot = SnapshotService(self.platform)
        self.thresholds = ThresholdStore(settings.thresholds_file)
        self.latest = LatestCache(dedup_capacity=settings.dedup_cache_size)
        self.weather = (
            WeatherService(
                self.tsdb,
                build_provider(settings.weather_provider, settings.weather_api_key),
                settings.station_list,
            )
            if settings.station_list
            else WeatherService(self.tsdb, build_provider("", ""), [])
        )
        self.engine = FddEngine(
            tsdb=self.tsdb,
            snapshot=self.snapshot,
            thresholds=self.thresholds,
            platform=self.platform,
            latest=self.latest,
            weather_reader=self.weather,
        )
        self.reports = FddReportGenerator(
            platform=self.platform,
            snapshot=self.snapshot,
            rules=self.engine.registry.rules,
            thresholds=self.thresholds,
        )
        self.telemetry_consumer = TelemetryLatestConsumer(
            settings.broker_list, self.latest, lag_warn=settings.kafka_lag_warn
        )
        self.executed_consumer = ExecutedEventConsumer(settings.broker_list)

    def job_fns(self) -> dict[str, tuple[str, JobFn]]:
        """job_id → (cron, 任务体工厂)。槽位任务（forecast/optimize）默认禁用（§3）。"""
        table: dict[str, tuple[str, JobFn]] = {
            "fdd_eval": (JOB_TRIGGERS["fdd_eval"], self.engine.run_round),
            "fdd_report_day": (
                JOB_TRIGGERS["fdd_report_day"],
                _report(self.reports, "day"),
            ),
            "fdd_report_week": (
                JOB_TRIGGERS["fdd_report_week"],
                _report(self.reports, "week"),
            ),
            "weather_actual": (
                JOB_TRIGGERS["weather_actual"],
                self.weather.fetch_actual_job,
            ),
            "weather_forecast": (
                JOB_TRIGGERS["weather_forecast"],
                self.weather.fetch_forecast_job,
            ),
            "snapshot_refresh": (
                JOB_TRIGGERS["snapshot_refresh"],
                _snapshot_refresh(self.snapshot),
            ),
        }
        return {k: v for k, v in table.items() if k in self.settings.jobs_enabled}

    async def start(self) -> None:
        await self.tsdb.start()  # 权限面校验 fail-fast（§1.2）
        await self.platform.start()
        await self.telemetry_consumer.start()
        await self.executed_consumer.start()

    async def stop(self) -> None:
        # 优雅退出顺序（§3）：停订阅 → 等在跑 job（scheduler 层）→ 冲指标 → 关连接池
        await self.telemetry_consumer.stop()
        await self.executed_consumer.stop()
        await self.platform.stop()
        await self.tsdb.stop()


def _snapshot_refresh(service: SnapshotService) -> JobFn:
    async def run() -> None:
        await snapshot_refresh_job(service)

    return run


def _report(reports: FddReportGenerator, period_type: str) -> JobFn:
    async def run() -> None:
        await reports.generate(period_type)  # type: ignore[arg-type]

    return run


async def _run_job_once(app: App, job_id: str) -> None:
    jobs = app.job_fns()
    if job_id not in jobs:
        msg = f"未知或未启用的 --once 任务: {job_id}（可用: {sorted(jobs)}）"
        raise SystemExit(msg)
    await jobs[job_id][1]()


async def _service_main(app: App) -> None:
    start_http_server(app.settings.algo_metrics_port)  # /metrics（deploy.md 监控栈刮取）
    sched = build_scheduler(app.job_fns(), app.settings.algo_tz)
    sched.start()
    stop_evt: asyncio.Event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_evt.set)
    log.info(
        "algo_service_started",
        version=__version__,
        jobs=sorted(app.job_fns()),
        metrics_port=app.settings.algo_metrics_port,
    )
    await stop_evt.wait()
    log.info("algo_service_stopping")
    sched.shutdown(wait=True)  # 等在跑 job 完成（上限 60s 由 misfire/job 超时兜底）


async def _async_main(args: argparse.Namespace) -> None:
    settings = get_settings()
    setup_logging(settings.algo_log_level)
    app = App(settings)
    await app.start()
    try:
        if args.once:
            await _run_job_once(app, args.once)
        else:
            await _service_main(app)
    finally:
        await app.stop()


def main() -> None:
    parser = argparse.ArgumentParser(prog="thermio-algo")
    parser.add_argument("--once", help="单轮执行指定任务后退出（fdd_eval|…）")
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
