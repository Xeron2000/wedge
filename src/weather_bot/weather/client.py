from __future__ import annotations

import httpx

from weather_bot.config import CityConfig
from weather_bot.log import get_logger

log = get_logger("weather.client")

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"


async def fetch_ensemble(
    client: httpx.AsyncClient,
    city: CityConfig,
    forecast_days: int = 7,
) -> dict | None:
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "daily": "temperature_2m_max",
        "models": "gfs_seamless",
        "forecast_days": forecast_days,
        "temperature_unit": "fahrenheit",
        "timezone": city.timezone,
    }

    for attempt in range(3):
        try:
            resp = await client.get(ENSEMBLE_URL, params=params, timeout=30.0)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
            wait = 2 ** attempt
            log.warning("open_meteo_retry", city=city.name, attempt=attempt + 1, error=str(e))
            if attempt < 2:
                import asyncio
                await asyncio.sleep(wait)
    log.error("open_meteo_failed", city=city.name)
    return None
