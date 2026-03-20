from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

import httpx
from eccodes import (
    codes_get_array,
    codes_grib_find_nearest,
    codes_grib_new_from_file,
    codes_release,
)

from wedge.config import CityConfig
from wedge.log import get_logger

log = get_logger("weather.client")

NOMADS_FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_MEMBER_IDS = ("c00",) + tuple(f"p{i:02d}" for i in range(1, 31))
_FORECAST_INTERVAL_HOURS = 3
_MAX_FORECAST_HOURS = 384
_MIN_MEMBER_COUNT = 2


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _resolve_latest_cycle(now: datetime | None = None) -> tuple[date, int]:
    current = now or _utc_now()
    cycle_hour = (current.hour // 6) * 6
    cycle_date = current.date()
    if current.hour < 4:
        cycle_date -= timedelta(days=1)
        cycle_hour = 18
    return cycle_date, cycle_hour


def _forecast_hours_for_target_date(
    target_date: date,
    city_timezone: str,
    run_date: date,
    run_hour: int,
) -> list[int]:
    city_tz = datetime.now().astimezone().tzinfo
    try:
        from zoneinfo import ZoneInfo

        city_tz = ZoneInfo(city_timezone)
    except Exception:
        city_tz = UTC

    run_dt_utc = datetime(run_date.year, run_date.month, run_date.day, run_hour, tzinfo=UTC)
    run_local_date = run_dt_utc.astimezone(city_tz).date()
    day_offset = (target_date - run_local_date).days
    if day_offset < 0:
        return []

    start = max(0, day_offset * 24)
    end = min(_MAX_FORECAST_HOURS, start + 21)
    hours = list(range(start, end + 1, _FORECAST_INTERVAL_HOURS))
    return hours


def _member_file(member_id: str, cycle_hour: int, forecast_hour: int) -> str:
    prefix = "gec00" if member_id == "c00" else f"gep{member_id[1:]}"
    return f"{prefix}.t{cycle_hour:02d}z.pgrb2s.0p25.f{forecast_hour:03d}"


def _extract_point_temperature_f(grib_bytes: bytes, city: CityConfig) -> float | None:
    import io

    handle = None
    try:
        with io.BytesIO(grib_bytes) as fh:
            handle = codes_grib_new_from_file(fh)
            if handle is None:
                return None
            try:
                nearest = codes_grib_find_nearest(handle, city.lat, city.lon)
                if nearest and isinstance(nearest, (list, tuple)):
                    candidate = nearest[0]
                    value = candidate.get("value") if isinstance(candidate, dict) else None
                    if value is not None and math.isfinite(value):
                        return float(value)
            except Exception:
                pass

            values = codes_get_array(handle, "values")
            if values is None or len(values) == 0:
                return None
            finite_values = [float(v) for v in values if math.isfinite(v)]
            if not finite_values:
                return None
            return finite_values[0]
    except Exception as exc:  # noqa: BLE001
        log.warning("noaa_grib_parse_failed", city=city.name, error=str(exc))
        return None
    finally:
        if handle is not None:
            try:
                codes_release(handle)
            except Exception:  # pragma: no cover
                pass


async def fetch_ensemble(
    client: httpx.AsyncClient,
    city: CityConfig,
    target_date: date,
) -> dict[str, object] | None:
    run_date, cycle_hour = _resolve_latest_cycle()
    forecast_hours = _forecast_hours_for_target_date(
        target_date, city.timezone, run_date, cycle_hour
    )
    if not forecast_hours:
        log.warning(
            "noaa_no_forecast_hours",
            city=city.name,
            target_date=target_date.isoformat(),
        )
        return None

    member_temps_f: dict[str, float] = {}
    dir_path = f"/gefs.{run_date.strftime('%Y%m%d')}/{cycle_hour:02d}/atmos/pgrb2sp25"

    for member_id in _MEMBER_IDS:
        member_values: list[float] = []
        for forecast_hour in forecast_hours:
            params = {
                "file": _member_file(member_id, cycle_hour, forecast_hour),
                "lev_2_m_above_ground": "on",
                "var_TMP": "on",
                "subregion": "",
                "leftlon": str(city.lon),
                "rightlon": str(city.lon),
                "toplat": str(city.lat),
                "bottomlat": str(city.lat),
                "dir": dir_path,
            }
            try:
                resp = await client.get(NOMADS_FILTER_URL, params=params, timeout=30.0)
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as exc:
                log.warning(
                    "noaa_fetch_failed",
                    city=city.name,
                    member=member_id,
                    forecast_hour=forecast_hour,
                    error=str(exc),
                )
                continue

            value = _extract_point_temperature_f(resp.content, city)
            if value is not None and math.isfinite(value):
                member_values.append(value)

        if member_values:
            member_temps_f[member_id] = max(member_values)

    if len(member_temps_f) < _MIN_MEMBER_COUNT:
        log.error(
            "noaa_insufficient_members",
            city=city.name,
            target_date=target_date.isoformat(),
            members=len(member_temps_f),
        )
        return None

    return {
        "source": "noaa_gefs",
        "city": city.name,
        "target_date": target_date.isoformat(),
        "run_time": datetime(
            run_date.year,
            run_date.month,
            run_date.day,
            cycle_hour,
            tzinfo=UTC,
        ).isoformat(),
        "member_temps_f": member_temps_f,
    }


async def fetch_actual_temperature(
    client: httpx.AsyncClient,
    city: CityConfig,
    target_date: str,
) -> int | None:
    """Fetch observed daily max temperature for a specific date.

    Uses Open-Meteo Archive API. Returns rounded integer °F, or None on failure.
    """
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "start_date": target_date,
        "end_date": target_date,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": city.timezone,
    }

    for attempt in range(3):
        try:
            resp = await client.get(ARCHIVE_URL, params=params, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
            temps = data.get("daily", {}).get("temperature_2m_max", [])
            if temps and temps[0] is not None:
                return round(temps[0])
            log.warning("no_actual_temp_data", city=city.name, date=target_date)
            return None
        except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
            wait = 2**attempt
            log.warning("archive_api_retry", city=city.name, attempt=attempt + 1, error=str(e))
            if attempt < 2:
                import asyncio

                await asyncio.sleep(wait)
    log.error("archive_api_failed", city=city.name, date=target_date)
    return None
