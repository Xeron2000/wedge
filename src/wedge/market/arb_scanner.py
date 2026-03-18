"""Arbitrage-focused market scanner.

Two-phase design:
1. Discovery (startup): scan all cities, rank by volume+OI, cache top-N markets
2. Fast scan (every minute): only fetch prices for cached markets, detect bucket sum gaps

This avoids hammering the Polymarket API while still catching ephemeral arb opportunities.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Awaitable

import httpx

from wedge.log import get_logger
from wedge.market.models import MarketBucket
from wedge.strategy.arbitrage import detect_bucket_arbitrage, ArbitrageSignal

log = get_logger("market.arb_scanner")

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_DEFAULT_TOP_N = 6
_DEFAULT_MIN_VOLUME = 1000.0  # Lower threshold for arb scanner (cast wider net)


@dataclass
class HotMarket:
    """A cached high-liquidity market group (all buckets for city+date)."""
    city: str
    target_date: date
    slugs: list[str]  # Polymarket event slugs for this city+date
    token_ids: list[str]  # All bucket token_ids
    volume_24h: float
    open_interest: float
    last_prices: dict[str, float] = field(default_factory=dict)  # token_id -> price
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ArbScanner:
    """Persistent arbitrage scanner with hot-market caching.

    Usage:
        scanner = ArbScanner(top_n=6, min_gap=0.05)
        await scanner.discover(http_client, city_slugs)  # Once at startup
        # Then every minute:
        signals = await scanner.fast_scan(http_client)
    """

    def __init__(
        self,
        top_n: int = _DEFAULT_TOP_N,
        min_gap: float = 0.05,
        min_buckets: int = 2,
        on_signal: Callable[[ArbitrageSignal], Awaitable[None]] | None = None,
    ) -> None:
        self.top_n = top_n
        self.min_gap = min_gap
        self.min_buckets = min_buckets
        self.on_signal = on_signal
        self._hot_markets: list[HotMarket] = []
        self._discovered = False

    @property
    def hot_markets(self) -> list[HotMarket]:
        return self._hot_markets

    async def discover(self, http_client: httpx.AsyncClient, city_slugs: dict[str, str]) -> int:
        """Scan all cities, rank by liquidity, cache top-N market groups.

        Args:
            http_client: Shared HTTP client
            city_slugs: {city_name: slug_prefix} e.g. {'NYC': 'nyc', 'London': 'london'}

        Returns:
            Number of hot markets cached
        """
        log.info("arb_discovery_start", cities=len(city_slugs), top_n=self.top_n)
        candidates: list[HotMarket] = []

        for city, slug_prefix in city_slugs.items():
            try:
                markets = await self._fetch_city_markets(http_client, city, slug_prefix)
                candidates.extend(markets)
            except Exception as exc:
                log.warning("arb_discovery_city_failed", city=city, error=str(exc))

        # Rank by volume_24h + open_interest, take top N
        candidates.sort(key=lambda m: m.volume_24h + m.open_interest, reverse=True)
        self._hot_markets = candidates[: self.top_n]
        self._discovered = True

        log.info(
            "arb_discovery_complete",
            cached=len(self._hot_markets),
            markets=[(m.city, str(m.target_date), f"vol={m.volume_24h:.0f}") for m in self._hot_markets],
        )
        return len(self._hot_markets)

    async def fast_scan(self, http_client: httpx.AsyncClient) -> list[ArbitrageSignal]:
        """Fetch latest prices for hot markets only, detect arbitrage.

        Lightweight: only fetches price updates, no heavy market scanning.
        Designed to run every minute.

        Returns:
            List of detected ArbitrageSignal instances
        """
        if not self._hot_markets:
            log.debug("arb_fast_scan_no_markets")
            return []

        signals: list[ArbitrageSignal] = []

        for hot in self._hot_markets:
            try:
                buckets = await self._fetch_bucket_prices(http_client, hot)
                if not buckets:
                    continue

                # Update cached prices
                hot.last_prices = {b.token_id: b.market_price for b in buckets}
                hot.last_updated = datetime.now(timezone.utc)

                signal = detect_bucket_arbitrage(buckets, min_gap=self.min_gap, min_buckets=self.min_buckets)
                if signal:
                    log.info(
                        "arb_fast_scan_hit",
                        city=hot.city,
                        date=str(hot.target_date),
                        gap=round(signal.gap, 4),
                        price_sum=round(signal.price_sum, 4),
                    )
                    signals.append(signal)
                    if self.on_signal:
                        await self.on_signal(signal)
            except Exception as exc:
                log.warning("arb_fast_scan_error", city=hot.city, error=str(exc))

        return signals

    async def _fetch_city_markets(
        self, http_client: httpx.AsyncClient, city: str, slug_prefix: str
    ) -> list[HotMarket]:
        """Fetch all active markets for a city, group by date."""
        from wedge.market.scanner import scan_weather_markets
        from wedge.market.polymarket import PublicPolymarketClient

        poly = PublicPolymarketClient()
        today = datetime.now(timezone.utc).date()

        hot_markets: list[HotMarket] = []
        seen_dates: dict[date, HotMarket] = {}

        # Scan next 3 days
        for days_ahead in range(1, 4):
            target_date = today + __import__('datetime').timedelta(days=days_ahead)
            try:
                buckets = await scan_weather_markets(poly, city, target_date)
                if not buckets:
                    continue

                total_vol = sum(b.volume_24h for b in buckets)
                total_oi = sum(b.open_interest for b in buckets)

                if total_vol < _DEFAULT_MIN_VOLUME:
                    continue

                if target_date not in seen_dates:
                    hot = HotMarket(
                        city=city,
                        target_date=target_date,
                        slugs=[f"{slug_prefix}-{target_date}"],
                        token_ids=[b.token_id for b in buckets],
                        volume_24h=total_vol,
                        open_interest=total_oi,
                    )
                    seen_dates[target_date] = hot
                    hot_markets.append(hot)
            except Exception as exc:
                log.debug("arb_city_date_failed", city=city, date=str(target_date), error=str(exc))

        return hot_markets

    async def _fetch_bucket_prices(
        self, http_client: httpx.AsyncClient, hot: HotMarket
    ) -> list[MarketBucket]:
        """Fetch latest prices for a hot market's buckets via Gamma API."""
        from wedge.market.polymarket import PublicPolymarketClient
        from wedge.market.scanner import scan_weather_markets

        poly = PublicPolymarketClient()
        try:
            buckets = await scan_weather_markets(poly, hot.city, hot.target_date)
            return buckets
        except Exception as exc:
            log.warning("arb_price_fetch_failed", city=hot.city, error=str(exc))
            return []
