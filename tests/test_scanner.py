from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from wedge.market.scanner import _extract_market_date, scan_weather_markets


def _make_client(markets: list[dict]) -> AsyncMock:
    client = AsyncMock()
    client.get_markets.return_value = markets
    return client


def _token(outcome: str, price: float, token_id: str = "tok_1") -> dict:
    return {"outcome": outcome, "price": str(price), "token_id": token_id}


def _market(question: str, tokens: list[dict], end_date: str | None = None) -> dict:
    m: dict = {"question": question, "tokens": tokens}
    if end_date is not None:
        m["end_date_iso"] = end_date
    return m


TARGET_DATE = date(2026, 7, 4)


class TestScanWeatherMarkets:
    @pytest.mark.asyncio
    async def test_no_markets_returns_empty(self):
        client = _make_client([])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_without_temperature_or_high_skipped(self):
        client = _make_client([_market("Will it rain in NYC?", [_token("70 F", 0.3)])])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_wrong_city_skipped(self):
        client = _make_client([_market("Will the temperature high in Miami be 90 F?", [_token("90 F", 0.4)])])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_market_with_wrong_date_skipped(self):
        client = _make_client([
            _market(
                "Will the temperature high in NYC be 78 F?",
                [_token("78 F", 0.3)],
                end_date="2026-07-05T00:00:00",
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_token_without_temp_in_outcome_skipped(self):
        client = _make_client([
            _market(
                "Will the temperature high in NYC be above average?",
                [_token("Yes", 0.5)],
                end_date="2026-07-04T00:00:00",
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_token_with_price_zero_skipped(self):
        client = _make_client([
            _market(
                "Will the temperature high in NYC be 78 F?",
                [_token("78 F", 0.0)],
                end_date="2026-07-04T00:00:00",
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_token_with_price_one_skipped(self):
        client = _make_client([
            _market(
                "Will the temperature high in NYC be 78 F?",
                [_token("78 F", 1.0)],
                end_date="2026-07-04T00:00:00",
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert result == []

    @pytest.mark.asyncio
    async def test_full_successful_scan(self):
        client = _make_client([
            _market(
                "Will the temperature high in new york be 78 F on July 4?",
                [_token("78 F", 0.3, token_id="tok_78")],
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert len(result) == 1
        b = result[0]
        assert b.token_id == "tok_78"
        assert b.city == "NYC"
        assert b.temp_f == 78
        assert b.market_price == 0.3
        assert b.implied_prob == 0.3

    @pytest.mark.asyncio
    async def test_market_with_no_date_still_included(self):
        # When market_date is None (no date field, no date in question), it passes date check
        client = _make_client([
            _market(
                "Will the temperature high in nyc be 85 F?",
                [_token("85 F", 0.25, token_id="tok_85")],
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert len(result) == 1
        assert result[0].temp_f == 85

    @pytest.mark.asyncio
    async def test_multiple_tokens_one_market(self):
        client = _make_client([
            _market(
                "What will the high temperature in NYC be?",
                [
                    _token("78 F", 0.3, token_id="tok_78"),
                    _token("80 F", 0.4, token_id="tok_80"),
                    _token("no temp here", 0.3, token_id="tok_bad"),
                ],
            )
        ])
        result = await scan_weather_markets(client, "NYC", TARGET_DATE)
        assert len(result) == 2
        temps = {b.temp_f for b in result}
        assert temps == {78, 80}


class TestExtractMarketDate:
    def test_end_date_iso_field(self):
        m = {"end_date_iso": "2026-07-04T00:00:00", "question": ""}
        assert _extract_market_date(m, 2026) == date(2026, 7, 4)

    def test_end_date_field_with_z_suffix(self):
        m = {"end_date": "2026-08-15T12:00:00Z", "question": ""}
        assert _extract_market_date(m, 2026) == date(2026, 8, 15)

    def test_invalid_end_date_falls_through_to_question(self):
        m = {"end_date_iso": "not-a-date", "question": "Temperature high on July 4"}
        result = _extract_market_date(m, 2026)
        assert result == date(2026, 7, 4)

    def test_question_july_4(self):
        m = {"question": "Will the high be 78 F on July 4?"}
        assert _extract_market_date(m, 2026) == date(2026, 7, 4)

    def test_question_invalid_date_february_30(self):
        m = {"question": "Will the high be 78 F on February 30?"}
        assert _extract_market_date(m, 2026) is None

    def test_no_date_info_returns_none(self):
        m = {"question": "Will the high be above average?"}
        assert _extract_market_date(m, 2026) is None

    def test_end_date_iso_takes_priority_over_question(self):
        m = {"end_date_iso": "2026-07-04T00:00:00", "question": "on August 1"}
        assert _extract_market_date(m, 2026) == date(2026, 7, 4)
