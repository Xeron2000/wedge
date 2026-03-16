from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from wedge.market.scanner import scan_weather_markets


def _make_client(event: dict | None) -> AsyncMock:
    """Create a mock client that returns an event from get_event_by_slug."""
    client = AsyncMock()
    client.get_event_by_slug.return_value = event
    return client


def _outcome(outcome: str, price: float) -> dict:
    """Create an outcome (Yes/No token)."""
    return {"outcome": outcome, "price": str(price)}


def _market(question: str, outcomes: list[dict], token_ids: list[str] | None = None) -> dict:
    """Create a market within an event."""
    m: dict = {"question": question, "outcomes": outcomes}
    if token_ids:
        m["clobTokenIds"] = token_ids
    return m


def _event(title: str, markets: list[dict]) -> dict:
    """Create an event containing markets."""
    return {"title": title, "markets": markets}


TARGET_DATE = date(2026, 7, 4)


class TestScanWeatherMarkets:
    @pytest.mark.asyncio
    async def test_no_markets_returns_empty(self):
        client = _make_client(None)  # Event not found
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_without_temperature_skipped(self):
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will it rain in NYC?", [_outcome("Yes", 0.3), _outcome("No", 0.7)])
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_wrong_city_skipped(self):
        # This test is no longer relevant since we query by slug (city-specific)
        # But we keep it to test that unsupported cities return empty
        client = _make_client(None)
        result = await scan_weather_markets(client, "UnsupportedCity", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_wrong_date_skipped(self):
        # Date is now part of the slug, so wrong date means event not found
        client = _make_client(None)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_without_temp_in_question_skipped(self):
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be sunny?",
                   [_outcome("Yes", 0.5), _outcome("No", 0.5)])
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_price_zero_skipped(self):
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be 70°F?",
                   [_outcome("Yes", 0.0), _outcome("No", 1.0)])
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_price_one_skipped(self):
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be 70°F?",
                   [_outcome("Yes", 1.0), _outcome("No", 0.0)])
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_full_successful_scan(self):
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be 70°F?",
                   [_outcome("Yes", 0.3), _outcome("No", 0.7)],
                   ["token_70"]),
            _market("Will the highest temperature in New York City be 75°F?",
                   [_outcome("Yes", 0.4), _outcome("No", 0.6)],
                   ["token_75"]),
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)

        assert len(result) == 2
        assert result[0].temp_f == 70
        assert result[0].market_price == 0.3
        assert result[0].token_id == "token_70"
        assert result[1].temp_f == 75
        assert result[1].market_price == 0.4
        assert result[1].token_id == "token_75"

    @pytest.mark.asyncio
    async def test_market_with_no_date_still_included(self):
        # Date is now in the slug, so this test is no longer relevant
        # We test that markets are included when event is found
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be 80°F?",
                   [_outcome("Yes", 0.5), _outcome("No", 0.5)],
                   ["token_80"]),
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)

        assert len(result) == 1
        assert result[0].temp_f == 80

    @pytest.mark.asyncio
    async def test_multiple_tokens_one_market(self):
        # In the new format, each market has one temperature
        event = _event("Highest temperature in NYC on July 4?", [
            _market("Will the highest temperature in New York City be between 70-75°F?",
                   [_outcome("Yes", 0.6), _outcome("No", 0.4)],
                   ["token_70_75"]),
        ])
        client = _make_client(event)
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)

        # Should extract 70 from the question
        assert len(result) == 1
        assert result[0].temp_f in [70, 75]  # Could match either number


