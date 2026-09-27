"""WeatherProvider 适配器（algo.md §12）：condition_code 供应商原样存；MVP 不绑定厂商。

供应商经 WEATHER_PROVIDER 选择（部署配置）；密钥经 env 注入（SEC-KEY-01/05）。
站点 ID 语义由供应商自定（open-meteo 用 "lat,lon"）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

import httpx


@dataclass(frozen=True)
class ActualObs:
    """weather_actual 一行载荷。"""

    station_id: str
    obs_ts: datetime
    temp_c: float | None = None
    rh_pct: float | None = None
    wind_speed_ms: float | None = None
    pressure_hpa: float | None = None
    precip_mm: float | None = None
    condition_code: str | None = None  # 供应商天气现象码原样存（枚举治理在应用层）
    source: str = ""


@dataclass(frozen=True)
class ForecastPoint:
    target_ts: datetime
    temp_c: float | None = None
    rh_pct: float | None = None
    wind_speed_ms: float | None = None
    pressure_hpa: float | None = None
    precip_mm: float | None = None
    condition_code: str | None = None


@dataclass(frozen=True)
class ForecastIssue:
    """weather_actual 同构的预报发布（issued_at × targets 二维）。"""

    station_id: str
    issued_at: datetime
    targets: tuple[ForecastPoint, ...]
    source: str = ""


@runtime_checkable
class WeatherProvider(Protocol):
    name: str

    async def fetch_actual(self, station_id: str) -> ActualObs: ...

    async def fetch_forecast(self, station_id: str) -> ForecastIssue: ...


class OpenMeteoProvider:
    """open-meteo.com 参考适配器（免密钥；station_id = "lat:lon"）。

    站点 ID 用冒号分隔经纬度：WEATHER_STATIONS 为逗号分隔清单（§12），
    ID 内含逗号会破坏清单切分。不绑定厂商的口径不变。
    """

    name = "open-meteo"
    BASE = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, api_key: str = "", client: httpx.AsyncClient | None = None) -> None:
        self._client = client or httpx.AsyncClient(timeout=10.0)
        self._api_key = api_key  # open-meteo 免费层不需要；付费层经 header（SEC-KEY-01）

    async def fetch_actual(self, station_id: str) -> ActualObs:
        lat, lon = self._parse_station(station_id)
        r = await self._client.get(
            self.BASE,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": ",".join(
                    [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "wind_speed_10m",
                        "surface_pressure",
                        "precipitation",
                        "weather_code",
                    ]
                ),
            },
        )
        r.raise_for_status()
        cur = r.json()["current"]
        return ActualObs(
            station_id=station_id,
            obs_ts=_parse_ts(cur["time"]),
            temp_c=cur.get("temperature_2m"),
            rh_pct=cur.get("relative_humidity_2m"),
            wind_speed_ms=cur.get("wind_speed_10m"),
            pressure_hpa=cur.get("surface_pressure"),
            precip_mm=cur.get("precipitation"),
            condition_code=_code(cur.get("weather_code")),
            source=self.name,
        )

    async def fetch_forecast(self, station_id: str) -> ForecastIssue:
        lat, lon = self._parse_station(station_id)
        r = await self._client.get(
            self.BASE,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(
                    [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "wind_speed_10m",
                        "surface_pressure",
                        "precipitation",
                        "weather_code",
                    ]
                ),
                "forecast_days": 2,
            },
        )
        r.raise_for_status()
        data = r.json()
        hourly = data["hourly"]
        issued = _parse_ts(data["hourly"]["time"][0])
        targets = tuple(
            ForecastPoint(
                target_ts=_parse_ts(t),
                temp_c=hourly["temperature_2m"][i],
                rh_pct=hourly["relative_humidity_2m"][i],
                wind_speed_ms=hourly["wind_speed_10m"][i],
                pressure_hpa=hourly["surface_pressure"][i],
                precip_mm=hourly["precipitation"][i],
                condition_code=_code(hourly["weather_code"][i]),
            )
            for i, t in enumerate(hourly["time"])
        )
        return ForecastIssue(
            station_id=station_id, issued_at=issued, targets=targets, source=self.name
        )

    @staticmethod
    def _parse_station(station_id: str) -> tuple[str, str]:
        try:
            lat, lon = station_id.split(":", 1)
            return lat.strip(), lon.strip()
        except ValueError as exc:
            msg = f"open-meteo 站点 ID 应为 'lat:lon'，实得: {station_id!r}"
            raise ValueError(msg) from exc


def _parse_ts(s: str) -> datetime:
    # open-meteo 无时区后缀 → 按 UTC 解析（文档口径：ISO UTC）
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _code(v: object) -> str | None:
    """condition_code 原样存（供应商现象码；None 保持 None）。"""
    return None if v is None else str(v)


def build_provider(name: str, api_key: str) -> WeatherProvider:
    """供应商注册面（MVP：open-meteo；扩充 = 此处加分支）。"""
    if name in ("", "open-meteo"):
        return OpenMeteoProvider(api_key=api_key)
    msg = f"未知 WEATHER_PROVIDER: {name!r}（可用: open-meteo）"
    raise ValueError(msg)
