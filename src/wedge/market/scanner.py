from __future__ import annotations

import re
from datetime import date, datetime

from wedge.log import get_logger
from wedge.market.models import MarketBucket
from wedge.market.polymarket import PolymarketClient

log = get_logger("market.scanner")

_TEMP_PATTERN = re.compile(r"(\d+)\s*°?\s*[CF]", re.IGNORECASE)
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
    "miami": "Miami",
    "seoul": "Seoul",
    "london": "London",
    "shanghai": "Shanghai",
    "wellington": "Wellington",
}

# Map city names to Polymarket slug format (high liquidity markets only)
_CITY_TO_SLUG = {
    "NYC": "nyc",
    "Miami": "miami",
    "Seoul": "seoul",
    "London": "london",
    "Shanghai": "shanghai",
    "Wellington": "wellington",
}


async def scan_weather_markets(
    client: PolymarketClient, city: str, target_date: date
) -> list[MarketBucket]:
    """Scan Polymarket for weather temperature contracts matching city and date.

    Uses slug-based queries to fetch specific weather events from Gamma API.
    """
    # Build slug for the specific city and date
    # Format: highest-temperature-in-{city}-on-{month}-{day}-{year}
    city_slug = _CITY_TO_SLUG.get(city)
    if not city_slug:
        log.warning("unsupported_city", city=city)
        return []

    month_name = target_date.strftime("%B").lower()  # e.g., "march"
    day = target_date.day
    year = target_date.year

    slug = f"highest-temperature-in-{city_slug}-on-{month_name}-{day}-{year}"

    # Try to fetch the event by slug
    if hasattr(client, 'get_event_by_slug'):
        event = await client.get_event_by_slug(slug)
    else:
        # Fallback for authenticated client
        log.warning("client_missing_get_event_by_slug", city=city)
        return []

    if not event:
        log.info("scan_complete", city=city, date=str(target_date), buckets_found=0)
        return []

    buckets: list[MarketBucket] = []

    # Parse markets from the event
    for market in event.get("markets", []):
        try:
            question = market.get("question", "").lower()

            # Extract temperature from question
            # Support both Fahrenheit (°F) and Celsius (°C)
            temp_match = _TEMP_PATTERN.search(question)
            if not temp_match:
                continue

            try:
                temp_value = int(temp_match.group(1))
            except (ValueError, IndexError):
                log.warning("invalid_temp_format", question=question)
                continue

            # Detect temperature unit and convert to Fahrenheit if needed
            if "°c" in question or " c " in question or question.endswith(" c"):
                # Convert Celsius to Fahrenheit
                temp_f = int(temp_value * 9 / 5 + 32)
            else:
                temp_f = temp_value

            # Parse outcomes - handle both list and JSON string formats
            outcomes_raw = market.get("outcomes", [])
            if isinstance(outcomes_raw, str):
                # Parse JSON string format: "[\"Yes\", \"No\"]"
                import json
                try:
                    outcomes = json.loads(outcomes_raw)
                except json.JSONDecodeError:
                    log.warning("invalid_outcomes_json", outcomes=outcomes_raw)
                    continue
            else:
                outcomes = outcomes_raw

            if not isinstance(outcomes, list) or len(outcomes) < 2:
                continue

            # Parse outcome prices - handle both formats
            prices_raw = market.get("outcomePrices")
            if prices_raw:
                # New format: separate outcomePrices field
                if isinstance(prices_raw, str):
                    import json
                    try:
                        prices = json.loads(prices_raw)
                    except json.JSONDecodeError:
                        log.warning("invalid_prices_json", prices=prices_raw)
                        continue
                else:
                    prices = prices_raw
            else:
                # Old format: prices embedded in outcomes
                prices = None

            # Find "Yes" outcome and its index
            yes_index = None
            for idx, outcome in enumerate(outcomes):
                if isinstance(outcome, str):
                    # New format: outcomes is list of strings
                    if outcome.lower() == "yes":
                        yes_index = idx
                        break
                elif isinstance(outcome, dict):
                    # Old format: outcomes is list of dicts
                    if outcome.get("outcome", "").lower() == "yes":
                        yes_index = idx
                        break

            if yes_index is None:
                continue

            # Get price for Yes outcome
            if prices:
                # New format: separate prices array
                if yes_index >= len(prices):
                    continue
                try:
                    price = float(prices[yes_index])
                except (ValueError, TypeError):
                    log.warning("invalid_price_value", price=prices[yes_index])
                    continue
            else:
                # Old format: price in outcome dict
                yes_outcome = outcomes[yes_index]
                if not isinstance(yes_outcome, dict):
                    continue
                try:
                    price = float(yes_outcome.get("price", 0))
                except (ValueError, TypeError):
                    log.warning("invalid_price_format", price=yes_outcome.get("price"))
                    continue

            if not (0 < price < 1):
                continue

            # Get token_id corresponding to Yes outcome using the same index
            clob_token_ids_raw = market.get("clobTokenIds", [])
            if isinstance(clob_token_ids_raw, str):
                # Parse JSON string format
                import json
                try:
                    clob_token_ids = json.loads(clob_token_ids_raw)
                except json.JSONDecodeError:
                    clob_token_ids = []
            else:
                clob_token_ids = clob_token_ids_raw
            if not clob_token_ids or yes_index >= len(clob_token_ids):
                token_id = ""
            else:
                token_id = clob_token_ids[yes_index]

            buckets.append(
                MarketBucket(
                    token_id=token_id,
                    city=city,
                    date=target_date,
                    temp_f=temp_f,
                    market_price=price,
                    implied_prob=price,
                )
            )
        except Exception as e:
            log.warning("market_parse_error", market=market.get("question", "unknown"), error=str(e))
            continue

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
