"""天气采集 job（algo.md §12）：actual/forecast 拉取 → TSDB upsert（自然幂等）。

失败语义：WARN + 下轮重试（非关键路径，不重试风暴）。保留策略由 TSDB 策略管，algo 不清理。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from algo.fdd.base import WeatherObs
from algo.obs import metrics as mt
from algo.obs.logging import get_logger
from algo.tsdb.client import TsdbClient
from algo.weather.providers import ActualObs, ForecastIssue, WeatherProvider

log = get_logger(__name__)

_UPSERT_ACTUAL = """
INSERT INTO weather_actual
  (station_id, obs_ts, temp_c, rh_pct, wind_speed_ms, pressure_hpa, precip_mm,
   condition_code, source)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (station_id, obs_ts) DO UPDATE SET
  temp_c = EXCLUDED.temp_c, rh_pct = EXCLUDED.rh_pct,
  wind_speed_ms = EXCLUDED.wind_speed_ms, pressure_hpa = EXCLUDED.pressure_hpa,
  precip_mm = EXCLUDED.precip_mm, condition_code = EXCLUDED.condition_code,
  source = EXCLUDED.source
"""

_UPSERT_FORECAST = """
INSERT INTO weather_forecast
  (station_id, issued_at, target_ts, temp_c, rh_pct, wind_speed_ms, pressure_hpa,
   precip_mm, condition_code, source)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (station_id, issued_at, target_ts) DO UPDATE SET
  temp_c = EXCLUDED.temp_c, rh_pct = EXCLUDED.rh_pct,
  wind_speed_ms = EXCLUDED.wind_speed_ms, pressure_hpa = EXCLUDED.pressure_hpa,
  precip_mm = EXCLUDED.precip_mm, condition_code = EXCLUDED.condition_code,
  source = EXCLUDED.source
"""


class WeatherService:
    """采集与读取一体：FDD 的湿球判据与 M&V 天气归一化共用（§12）。"""

    def __init__(self, tsdb: TsdbClient, provider: WeatherProvider, stations: list[str]) -> None:
        self._tsdb = tsdb
        self._provider = provider
        self._stations = stations

    async def fetch_actual_job(self) -> list[ActualObs]:
        """调度任务 weather_actual（§3）：拉各站点实况 → upsert。"""
        with mt.JOB_DURATION_MS.labels(job="weather_actual").time():
            return await self._run(self._fetch_one_actual, "weather_actual")

    async def fetch_forecast_job(self) -> list[ForecastIssue]:
        """调度任务 weather_forecast（§3，与实况错峰）。"""
        with mt.JOB_DURATION_MS.labels(job="weather_forecast").time():
            return await self._run(self._fetch_one_forecast, "weather_forecast")

    async def _run(self, one: Callable[[str], Awaitable[Any]], job: str) -> list[Any]:
        out: list[Any] = []
        ok = True
        for station in self._stations:
            try:
                result = await one(station)
                out.append(result)
                mt.WEATHER_FETCH_TOTAL.labels(result="ok").inc()
            except Exception as exc:
                ok = False
                mt.WEATHER_FETCH_TOTAL.labels(result="error").inc()
                log.warning(
                    "weather_fetch_failed_retry_next_round",
                    job=job,
                    station=station,
                    error=str(exc)[:200],
                )
        if not ok:
            mt.JOB_RUNS_TOTAL.labels(job=job, result="error").inc()
        return out

    async def _fetch_one_actual(self, station: str) -> ActualObs:
        obs = await self._provider.fetch_actual(station)
        await self._tsdb.execute(
            _UPSERT_ACTUAL,
            obs.station_id,
            obs.obs_ts,
            obs.temp_c,
            obs.rh_pct,
            obs.wind_speed_ms,
            obs.pressure_hpa,
            obs.precip_mm,
            obs.condition_code,
            obs.source or self._provider.name,
        )
        return obs

    async def _fetch_one_forecast(self, station: str) -> ForecastIssue:
        issue = await self._provider.fetch_forecast(station)
        for t in issue.targets:
            await self._tsdb.execute(
                _UPSERT_FORECAST,
                issue.station_id,
                issue.issued_at,
                t.target_ts,
                t.temp_c,
                t.rh_pct,
                t.wind_speed_ms,
                t.pressure_hpa,
                t.precip_mm,
                t.condition_code,
                issue.source or self._provider.name,
            )
        return issue

    async def latest_obs(self) -> WeatherObs | None:
        """最近一次实况观测（FDD 湿球判据数据面；§15 开放项：楼宇↔站点映射随资产域演进，
        MVP 单站点/全网最近观测口径）。"""
        row = await self._tsdb.fetchrow(
            """
            SELECT station_id, obs_ts, temp_c, rh_pct
            FROM weather_actual ORDER BY obs_ts DESC LIMIT 1
            """
        )
        if row is None:
            return None
        return WeatherObs(
            station_id=row["station_id"],
            obs_ts=row["obs_ts"],
            temp_c=row["temp_c"],
            rh_pct=row["rh_pct"],
        )
