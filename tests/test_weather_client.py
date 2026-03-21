from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from wedge.config import CityConfig
from wedge.weather.client import fetch_actual_temperature

NYC = CityConfig(name="NYC", lat=40.7772, lon=-73.8726, timezone="America/New_York", station="KLGA")
SEOUL = CityConfig(name="Seoul", lat=37.4602, lon=126.4407, timezone="Asia/Seoul", station="RKSI")
SHANGHAI = CityConfig(name="Shanghai", lat=31.1434, lon=121.8052, timezone="Asia/Shanghai", station="ZSPD")


def _wunderground_response(temps_f: list[int], max_temp: int | None = None) -> dict:
    observations = []
    for i, t in enumerate(temps_f):
        obs = {"temp": t}
        if i == 0 and max_temp is not None:
            obs["max_temp"] = max_temp
        observations.append(obs)
    return {"observations": observations}


class TestFetchActualTemperature:
    @pytest.mark.asyncio
    async def test_max_temp_from_first_observation(self):
        mock_response = httpx.Response(
            200,
            json=_wunderground_response([40, 45, 50, 53, 51, 48], max_temp=53),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        result = await fetch_actual_temperature(client, NYC, "2026-03-20")
        assert result == 53

    @pytest.mark.asyncio
    async def test_fallback_to_max_hourly_temp(self):
        mock_response = httpx.Response(
            200,
            json=_wunderground_response([40, 45, 50, 53, 51, 48]),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        result = await fetch_actual_temperature(client, NYC, "2026-03-20")
        assert result == 53

    @pytest.mark.asyncio
    async def test_seoul_uses_correct_country(self):
        mock_response = httpx.Response(
            200,
            json=_wunderground_response([50, 54], max_temp=54),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        result = await fetch_actual_temperature(client, SEOUL, "2026-03-21")
        assert result == 54
        call_kwargs = client.get.call_args
        url = call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("url", "")
        assert "RKSI:9:KR" in url

    @pytest.mark.asyncio
    async def test_shanghai_uses_correct_country(self):
        mock_response = httpx.Response(
            200,
            json=_wunderground_response([55, 59], max_temp=59),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        result = await fetch_actual_temperature(client, SHANGHAI, "2026-03-21")
        assert result == 59
        call_kwargs = client.get.call_args
        url = call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("url", "")
        assert "ZSPD:9:CN" in url

    @pytest.mark.asyncio
    async def test_empty_observations_returns_none(self):
        mock_response = httpx.Response(
            200,
            json={"observations": []},
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        result = await fetch_actual_temperature(client, NYC, "2026-03-20")
        assert result is None

    @pytest.mark.asyncio
    async def test_api_failure_returns_none(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        result = await fetch_actual_temperature(client, NYC, "2026-03-20")
        assert result is None

    @pytest.mark.asyncio
    async def test_http_error_retries(self):
        error_response = httpx.Response(
            503, request=httpx.Request("GET", "https://example.com"),
        )
        ok_response = httpx.Response(
            200,
            json=_wunderground_response([44, 48], max_temp=48),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(
            side_effect=[
                httpx.HTTPStatusError(
                    "503", request=httpx.Request("GET", "x"), response=error_response
                ),
                ok_response,
            ]
        )
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await fetch_actual_temperature(client, NYC, "2026-03-20")
        assert result == 48

    @pytest.mark.asyncio
    async def test_request_params_correct(self):
        mock_response = httpx.Response(
            200,
            json=_wunderground_response([70], max_temp=70),
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)
        await fetch_actual_temperature(client, NYC, "2026-07-04")
        call_kwargs = client.get.call_args
        params = call_kwargs.kwargs.get("params", {})
        assert params["units"] == "e"
        assert params["startDate"] == "20260704"
        assert params["endDate"] == "20260704"
        assert "apiKey" in params
