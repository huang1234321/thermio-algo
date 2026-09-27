"""asyncpg 只读池（角色 tsdb_algo）+ 启动权限面校验（algo.md §1.2/§5.1）。

权限面 = ddl.md §5.4 角色矩阵：telemetry/telemetry_5min/telemetry_1h SELECT；
weather_actual/weather_forecast SELECT+INSERT+UPDATE。启动时逐对象校验，
防配置漂移（角色被误收回 → fail-fast 而非运行期半瘫）。
"""

from __future__ import annotations

from datetime import datetime

import asyncpg

from algo.obs import metrics as mt
from algo.obs.logging import get_logger

log = get_logger(__name__)

# (对象, 需要的权限集合)。weather 域为写面（§12 采集 job）。
EXPECTED_PRIVS: dict[str, frozenset[str]] = {
    "telemetry": frozenset({"SELECT"}),
    "telemetry_5min": frozenset({"SELECT"}),
    "telemetry_1h": frozenset({"SELECT"}),
    "weather_actual": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "weather_forecast": frozenset({"SELECT", "INSERT", "UPDATE"}),
}


class TsdbClient:
    """连接池壳：所有查询统一计时（algo_tsdb_query_latency_ms）。"""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 5) -> None:
        self._dsn = dsn
        self._min = min_size
        self._max = max_size
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=self._min, max_size=self._max)
        missing = await self.verify_permissions()
        if missing:
            msg = f"tsdb_algo 权限面校验失败（配置漂移？）: {missing}"
            raise PermissionError(msg)
        log.info("tsdb_pool_ready", dsn=self._redact_dsn())

    async def stop(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            msg = "tsdb pool 未启动"
            raise RuntimeError(msg)
        return self._pool

    def _redact_dsn(self) -> str:
        # 日志只留 host/db（SEC-KEY-02：口令不落日志）；不引额外依赖，手工截取
        import re

        m = re.match(r"^(\w+://[^:/@]+:)[^@]+(@[^/]+/[^?]*)", self._dsn)
        return f"{m.group(1)}***{m.group(2)}" if m else "<redacted>"

    async def verify_permissions(self) -> dict[str, set[str]]:
        """返回缺失权限映射（空 = 全部满足）。"""
        missing: dict[str, set[str]] = {}
        async with self.pool.acquire() as conn:
            for obj, wanted in EXPECTED_PRIVS.items():
                for priv in sorted(wanted):
                    ok = await conn.fetchval(
                        "SELECT has_table_privilege(current_user, $1, $2)", obj, priv
                    )
                    if not ok:
                        missing.setdefault(obj, set()).add(priv)
        return missing

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        with mt.TSDB_QUERY_LATENCY_MS.time():
            async with self.pool.acquire() as conn:
                rows: list[asyncpg.Record] = await conn.fetch(sql, *args)
                return rows

    async def execute(self, sql: str, *args: object) -> str:
        """写面（仅 weather 域 upsert，§12）。"""
        with mt.TSDB_QUERY_LATENCY_MS.time():
            async with self.pool.acquire() as conn:
                status: str = await conn.execute(sql, *args)
                return status

    async def fetchrow(self, sql: str, *args: object) -> asyncpg.Record | None:
        with mt.TSDB_QUERY_LATENCY_MS.time():
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(sql, *args)

    async def latest_telemetry(self, point_id: int) -> tuple[datetime, float | None] | None:
        """原始表点查（§5.1：latest 回读——previous_value 的冷启动回退，§4.3-2）。"""
        row = await self.fetchrow(
            "SELECT ts, value FROM telemetry WHERE point_id = $1 ORDER BY ts DESC LIMIT 1",
            point_id,
        )
        if row is None:
            return None
        return row["ts"], row["value"]
