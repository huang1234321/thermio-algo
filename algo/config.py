"""pydantic-settings 环境配置（algo.md §13 全表）。

结构性纪律（§1.2）：**本模型不存在任何 PG 连接配置项**——PG 零直连不是
「不配也连」，是「无从连起」。tests/unit/test_config.py 钉死该结构性质。
凭据无默认值（SEC-KEY-01：部署注入；SEC-KEY-06：示例文件占位）。
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

# 默认启用的调度任务（§3 任务表首版状态：forecast/optimize 槽位预留、默认禁用）
DEFAULT_JOBS = (
    "fdd_eval",
    "fdd_report_day",
    "fdd_report_week",
    "weather_actual",
    "weather_forecast",
    "snapshot_refresh",
)
ALL_JOBS = (*DEFAULT_JOBS, "forecast_15m", "optimize_15m")


class Settings(BaseSettings):
    """全部环境变量（§13 表逐项；新增项必须同步该表与 config/algo.example.env）。"""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # ── 数据入口 ──
    kafka_brokers: str  # 逗号分隔；dev = 本项目 compose 映射端口（环境隔离纪律）
    tsdb_dsn: str  # tsdb_algo 角色（ddl.md §5.4）
    kafka_lag_warn: int = 5000  # 最新值消费 lag 自检阈值（§4.1）
    dedup_cache_size: int = 20_000  # (point_id, ts) 幂等去重 LRU 容量（§4.2）

    # ── 中台通道（对 PG 零直连的写出侧，§8）──
    api_internal_base_url: str
    svc_token_algo: str  # internal Bearer（platform.md §11）；不落日志（SEC-KEY-02）

    # ── 调度 ──
    algo_tz: str = "Asia/Shanghai"
    # --once 形态的 LatestCache 预热窗（DAT-202 D-45）：consumer seek-end 只见
    # 启动后新消息，冷栈首跑（无 committed offsets）200ms 内消费 0 条 →
    # run_status 等运行门第三态拦截。默认 0（服务形态不受影响）；彩排/联调
    # 置 ≥ 一个发布周期让 live 消息入缓存。
    algo_once_prime_s: float = 0.0
    algo_jobs_enabled: str = ",".join(DEFAULT_JOBS)
    algo_log_level: str = "INFO"

    # ── FDD ──
    thresholds_file: str = "config/thresholds.yaml"

    # ── 天气（§12）──
    weather_provider: str = ""
    weather_api_key: str = ""
    weather_stations: str = ""  # 逗号分隔站点 ID（MVP 全楼共用，映射开放项 §15）

    # ── 可观测性 ──
    algo_metrics_port: int = 9101

    @property
    def jobs_enabled(self) -> frozenset[str]:
        enabled = {j.strip() for j in self.algo_jobs_enabled.split(",") if j.strip()}
        unknown = enabled - set(ALL_JOBS)
        if unknown:
            msg = f"ALGO_JOBS_ENABLED 含未知任务: {sorted(unknown)}；合法集={ALL_JOBS}"
            raise ValueError(msg)
        return frozenset(enabled)

    @property
    def broker_list(self) -> list[str]:
        return [b.strip() for b in self.kafka_brokers.split(",") if b.strip()]

    @property
    def station_list(self) -> list[str]:
        return [s.strip() for s in self.weather_stations.split(",") if s.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """进程级单例（测试用 get_settings.cache_clear() 重置）。"""
    return Settings()  # type: ignore[call-arg]
