from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from wedge.config import CityConfig
from wedge.weather.client import fetch_ensemble
from wedge.weather.ensemble import parse_distribution

NYC = CityConfig(
    name="NYC",
    lat=40.7772,
    lon=-73.8726,
    timezone="America/New_York",
    station="KLGA",
)


def _native_raw(
    member_temps_f: dict[str, float | None],
    target_date: str = "2026-07-01",
) -> dict[str, object]:
    return {
        "source": "noaa_gefs",
        "city": "NYC",
        "target_date": target_date,
        "run_time": "2026-03-20T12:00:00+00:00",
        "member_temps_f": member_temps_f,
    }


class TestNoaaNativeParseDistribution:
    def test_noaa_native_payload_builds_distribution(self):
        raw = _native_raw(
            {
                "c00": 80.4,
                "p01": 81.6,
                "p02": 81.7,
                "p03": 83.2,
                "p04": 83.1,
                "p05": 83.4,
                "p06": 83.3,
                "p07": 83.5,
                "p08": 83.6,
                "p09": 83.7,
            }
        )

        result = parse_distribution(raw, "NYC", date(2026, 7, 1))

        assert result is not None
        assert result.member_count == 10
        assert abs(sum(result.buckets.values()) - 1.0) < 1e-9


class TestFetchNoaaEnsemble:
    @pytest.mark.asyncio
    async def test_fetch_ensemble_aggregates_member_maxima(self):
        response = httpx.Response(
            200,
            content=b"GRIB",
            request=httpx.Request("GET", "https://nomads.ncep.noaa.gov"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=response)

        with (
            patch(
                "wedge.weather.client._resolve_latest_cycle",
                return_value=(date(2026, 3, 20), 12),
            ),
            patch(
                "wedge.weather.client._forecast_hours_for_target_date",
                return_value=[24, 27],
            ),
            patch("wedge.weather.client._MEMBER_IDS", ("c00", "p01")),
            patch(
                "wedge.weather.client._extract_point_temperature_f",
                side_effect=[80.0, 82.0, 81.0, 83.0],
            ),
        ):
            result = await fetch_ensemble(
                client,
                NYC,
                target_date=date(2026, 7, 1),
            )

        assert result is not None
        assert result["source"] == "noaa_gefs"
        assert result["target_date"] == "2026-07-01"
        assert result["member_temps_f"] == {"c00": 82.0, "p01": 83.0}
        assert client.get.await_count == 4

        first_call = client.get.await_args_list[0]
        kwargs = first_call.kwargs
        assert kwargs["params"]["file"] == "gec00.t12z.pgrb2s.0p25.f024"
        assert kwargs["params"]["var_TMP"] == "on"
        assert kwargs["params"]["lev_2_m_above_ground"] == "on"
        assert kwargs["params"]["dir"] == "/gefs.20260320/12/atmos/pgrb2sp25"

    @pytest.mark.asyncio
    async def test_fetch_ensemble_returns_none_when_member_data_missing(self):
        response = httpx.Response(
            200,
            content=b"GRIB",
            request=httpx.Request("GET", "https://nomads.ncep.noaa.gov"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=response)

        with (
            patch(
                "wedge.weather.client._resolve_latest_cycle",
                return_value=(date(2026, 3, 20), 12),
            ),
            patch(
                "wedge.weather.client._forecast_hours_for_target_date",
                return_value=[24, 27],
            ),
            patch("wedge.weather.client._MEMBER_IDS", ("c00",)),
            patch(
                "wedge.weather.client._extract_point_temperature_f",
                side_effect=[None, None],
            ),
        ):
            result = await fetch_ensemble(
                client,
                NYC,
                target_date=date(2026, 7, 1),
            )

        assert result is None
