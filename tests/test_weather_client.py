from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from wedge.config import CityConfig
from wedge.weather.client import fetch_actual_temperature

NYC = CityConfig(name="NYC", lat=40.7772, lon=-73.8726, timezone="America/New_York", station="KLGA")


class TestFetchActualTemperature:
    @pytest.mark.asyncio
    async def test_success(self):
        mock_response = httpx.Response(
            200,
            json={
                "daily": {
                    "time": ["2026-07-01"],
                    "temperature_2m_max": [82.4],
                }
            },
            request=httpx.Request("GET", "https://example.com"),
        )

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result == 82  # round(82.4)

    @pytest.mark.asyncio
    async def test_rounding_half(self):
        mock_response = httpx.Response(
            200,
            json={"daily": {"temperature_2m_max": [82.5]}},
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result == 82  # banker's rounding: round(82.5) = 82

    @pytest.mark.asyncio
    async def test_null_temp_returns_none(self):
        mock_response = httpx.Response(
            200,
            json={"daily": {"temperature_2m_max": [None]}},
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_temps_returns_none(self):
        mock_response = httpx.Response(
            200,
            json={"daily": {"temperature_2m_max": []}},
            request=httpx.Request("GET", "https://example.com"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_api_failure_returns_none(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

        result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_http_error_retries(self):
        error_response = httpx.Response(
            503,
            request=httpx.Request("GET", "https://example.com"),
        )
        ok_response = httpx.Response(
            200,
            json={"daily": {"temperature_2m_max": [75.0]}},
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
            result = await fetch_actual_temperature(client, NYC, "2026-07-01")
        assert result == 75
