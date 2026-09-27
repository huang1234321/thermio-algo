"""config 结构性纪律（algo.md §1.2：algo 进程不存在任何 PG 连接配置项——
「无从连起」而非「不配也连」）。"""

from __future__ import annotations

import pytest
from algo.config import DEFAULT_JOBS, Settings


def test_no_pg_config_surface() -> None:
    """配置面零 PG 项：字段名/别名/描述全扫描（结构性保证，CODE-ST-03）。"""
    fields = Settings.model_fields
    banned = ("pg", "postgres", "5432")
    for name, f in fields.items():
        hay = (name + " " + (f.alias or "") + " " + str(f.description or "")).lower()
        for b in banned:
            assert b not in hay, f"配置面出现 PG 痕迹: {name}"


def test_defaults() -> None:
    s = Settings(
        kafka_brokers="localhost:9092",
        tsdb_dsn="postgres://tsdb_algo:x@localhost:5434/thermio_ts",
        api_internal_base_url="http://localhost:8080",
        svc_token_algo="tok",  # noqa: S106 - 测试占位
    )
    assert s.algo_tz == "Asia/Shanghai"
    assert s.algo_metrics_port == 9101
    assert s.jobs_enabled == frozenset(DEFAULT_JOBS)
    assert "forecast_15m" not in s.jobs_enabled  # 槽位默认禁用（§3）
    assert s.broker_list == ["localhost:9092"]


def test_jobs_enabled_unknown_rejected() -> None:
    s = Settings(
        kafka_brokers="k",
        tsdb_dsn="d",
        api_internal_base_url="u",
        svc_token_algo="t",  # noqa: S106 - 测试占位
        algo_jobs_enabled="fdd_eval,nonexistent_job",
    )
    with pytest.raises(ValueError, match="未知任务"):
        _ = s.jobs_enabled


def test_station_list() -> None:
    s = Settings(
        kafka_brokers="k",
        tsdb_dsn="d",
        api_internal_base_url="u",
        svc_token_algo="t",  # noqa: S106 - 测试占位
        weather_stations="31.23:121.47, 30.00:120.00",
    )
    assert s.station_list == ["31.23:121.47", "30.00:120.00"]
