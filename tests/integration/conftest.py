"""integration 层门控（algo.md §14：compose 回放链路）。

默认跳过（pyproject addopts `-m 'not integration'`）；由 scripts/it-fdd.sh 提供
独立栈环境后以 `pytest -m integration` 驱动。环境变量契约见 it_env()。

环境隔离纪律：栈一律是伞仓 deploy/docker-compose.dev.yml 的 thermio- 前缀容器，
禁止复用宿主机或其他项目既有 PG/Kafka/EMQX/TimescaleDB。
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pytest


def _req(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        pytest.skip(f"integration 环境变量缺失: {name}（经 scripts/it-fdd.sh 驱动）")
    return v


@dataclass(frozen=True)
class ItEnv:
    tsdb_dsn: str  # tsdb_algo 角色
    pg_dsn: str  # 夹具/快照构建用（mock 的 api 侧投影，非 algo 进程面）
    kafka_brokers: str
    mqtt_broker_url: str
    mqtt_password: str
    gwsim_bin: str
    svc_token: str


@pytest.fixture()
def it_env() -> ItEnv:
    if os.environ.get("ALGO_IT") != "1":
        pytest.skip("ALGO_IT!=1（integration 需独立栈，经 scripts/it-fdd.sh 驱动）")
    return ItEnv(
        tsdb_dsn=_req("ALGO_IT_TSDB_DSN"),
        pg_dsn=_req("ALGO_IT_PG_DSN"),
        kafka_brokers=_req("ALGO_IT_KAFKA_BROKERS"),
        mqtt_broker_url=os.environ.get("ALGO_IT_MQTT_URL", "tcp://127.0.0.1:1883"),
        mqtt_password=os.environ.get("ALGO_IT_MQTT_PASSWORD", "unused-dev-anonymous"),
        gwsim_bin=_req("ALGO_IT_GWSIM"),
        svc_token=os.environ.get("ALGO_IT_SVC_TOKEN", "it-svc-token"),
    )
