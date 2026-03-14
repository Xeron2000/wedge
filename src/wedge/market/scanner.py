from __future__ import annotations

import re
from datetime import date, datetime

from wedge.log import get_logger
from wedge.market.models import MarketBucket
from wedge.market.polymarket import PolymarketClient

log = get_logger("market.scanner")

_TEMP_PATTERN = re.compile(r"(\d+)\s*°?\s*F", re.IGNORECASE)
_DATE_PATTERN = re.compile(
    r"(january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+(\d{1,2})",
    re.IGNORECASE,
)
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}
_CITY_ALIASES = {
    "new york": "NYC",
    "nyc": "NYC",
    "chicago": "Chicago",
    "miami": "Miami",
    "dallas": "Dallas",
    "seattle": "Seattle",
    "atlanta": "Atlanta",
}


async def scan_weather_markets(
    client: PolymarketClient, city: str, target_date: date
) -> list[MarketBucket]:
    """Scan Polymarket for weather temperature contracts matching city and date."""
    markets = await client.get_markets()
    buckets: list[MarketBucket] = []

    for market in markets:
        question = market.get("question", "").lower()
        if "temperature" not in question and "high" not in question:
            continue

        matched_city = None
        for alias, canonical in _CITY_ALIASES.items():
            if alias in question:
                matched_city = canonical
                break

        if matched_city != city:
            continue

        # Validate date from question text or structured field
        market_date = _extract_market_date(market, target_date.year)
        if market_date and market_date != target_date:
            continue

        for token in market.get("tokens", []):
            outcome = token.get("outcome", "")
            temp_match = _TEMP_PATTERN.search(outcome)
            if not temp_match:
                continue

            temp_f = int(temp_match.group(1))
            price = float(token.get("price", 0))

            if not (0 < price < 1):
                continue

            buckets.append(
                MarketBucket(
                    token_id=token.get("token_id", ""),
                    city=city,
                    date=target_date,
                    temp_f=temp_f,
                    market_price=price,
                    implied_prob=price,
                )
            )

    log.info("scan_complete", city=city, date=str(target_date), buckets_found=len(buckets))
    return buckets


def _extract_market_date(market: dict, year: int) -> date | None:
    """Try to extract date from market question or structured fields."""
    # Try structured field first
    end_date = market.get("end_date_iso") or market.get("end_date")
    if end_date:
        try:
            return datetime.fromisoformat(end_date.replace("Z", "+00:00")).date()
        except (ValueError, AttributeError):
            pass

    # Fall back to parsing question text
    question = market.get("question", "")
    match = _DATE_PATTERN.search(question)
    if match:
        month = _MONTH_MAP.get(match.group(1).lower())
        day = int(match.group(2))
        if month and 1 <= day <= 31:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None
