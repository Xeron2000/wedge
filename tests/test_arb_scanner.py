"""Tests for market/arb_scanner.py - hot market caching and fast arbitrage scan."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.market.arb_scanner import ArbScanner, HotMarket
from wedge.market.models import MarketBucket
from wedge.strategy.arbitrage import ArbitrageSignal


def _bucket(token_id: str, price: float, temp: int = 70, city: str = "NYC", dt: date | None = None) -> MarketBucket:
    return MarketBucket(
        token_id=token_id,
        city=city,
        date=dt or date(2026, 7, 1),
        temp_value=temp,
        temp_unit="F",
        market_price=price,
        implied_prob=price,
        volume_24h=10000.0,
        open_interest=5000.0,
    )


def _hot_market(city: str = "NYC", dt: date | None = None, volume: float = 10000.0) -> HotMarket:
    return HotMarket(
        city=city,
        target_date=dt or date(2026, 7, 1),
        slugs=[f"{city.lower()}-2026-07-01"],
        token_ids=["a", "b", "c", "d"],
        volume_24h=volume,
        open_interest=5000.0,
    )


class TestHotMarket:
    def test_fields(self):
        hm = _hot_market()
        assert hm.city == "NYC"
        assert hm.volume_24h == 10000.0
        assert len(hm.token_ids) == 4

    def test_bucket_count(self):
        hm = HotMarket(
            city="NYC",
            target_date=date(2026, 7, 1),
            slugs=[],
            token_ids=["a", "b", "c"],
            volume_24h=5000.0,
            open_interest=1000.0,
        )
        assert hm.bucket_count == 3


class TestArbScannerInit:
    def test_default_params(self):
        scanner = ArbScanner()
        assert scanner.top_n == 6
        assert not scanner._discovered
        assert scanner.hot_markets == []

    def test_custom_params(self):
        scanner = ArbScanner(top_n=3, min_gap=0.03)
        assert scanner.top_n == 3
        assert scanner.min_gap == 0.03

    def test_on_signal_callback(self):
        cb = AsyncMock()
        scanner = ArbScanner(on_signal=cb)
        assert scanner.on_signal is cb


class TestArbScannerDiscover:
    @pytest.mark.asyncio
    async def test_discover_caches_top_n(self):
        scanner = ArbScanner(top_n=2)
        city_slugs = {"NYC": "nyc", "London": "london", "Miami": "miami"}
        target = date(2026, 7, 1)

        # Build HotMarket stubs directly (correct return type)
        nyc_hot = HotMarket(city="NYC", target_date=target, slugs=["nyc"], token_ids=["a","b","c","d"], volume_24h=50000.0, open_interest=10000.0)
        lon_hot = HotMarket(city="London", target_date=target, slugs=["lon"], token_ids=["e","f","g","h"], volume_24h=20000.0, open_interest=5000.0)
        mia_hot = HotMarket(city="Miami", target_date=target, slugs=["mia"], token_ids=["i","j","k","l"], volume_24h=5000.0, open_interest=1000.0)

        async def fake_fetch(http_client, city, slug_prefix):
            if city == "NYC": return [nyc_hot]
            if city == "London": return [lon_hot]
            return [mia_hot]

        with patch.object(scanner, '_fetch_city_markets', new=fake_fetch):
            n = await scanner.discover(MagicMock(), city_slugs)

        assert n == 2  # top_n=2
        assert scanner._discovered
        assert len(scanner.hot_markets) == 2
        # NYC and London should be top 2 by volume
        cities = {m.city for m in scanner.hot_markets}
        assert "NYC" in cities
        assert "London" in cities

    @pytest.mark.asyncio
    async def test_discover_empty_returns_zero(self):
        scanner = ArbScanner(top_n=6)

        async def fake_fetch(http_client, city, slug_prefix):
            return []

        with patch.object(scanner, '_fetch_city_markets', new=fake_fetch):
            n = await scanner.discover(MagicMock(), {"NYC": "nyc"})

        assert n == 0
        assert not scanner._discovered


class TestArbScannerFastScan:
    @pytest.mark.asyncio
    async def test_fast_scan_no_hot_markets(self):
        scanner = ArbScanner()
        signals = await scanner.fast_scan(MagicMock())
        assert signals == []

    @pytest.mark.asyncio
    async def test_fast_scan_detects_arbitrage(self):
        scanner = ArbScanner(top_n=6, min_gap=0.05, min_buckets=3)
        scanner._hot_markets = [_hot_market("NYC")]
        scanner._discovered = True

        arb_buckets = [
            _bucket("a", 0.20, 30),
            _bucket("b", 0.20, 35),
            _bucket("c", 0.20, 40),
        ]  # sum = 0.60, gap = 0.40

        async def fake_fetch_prices(http_client, hot):
            return arb_buckets

        with patch.object(scanner, '_fetch_bucket_prices', side_effect=fake_fetch_prices):
            signals = await scanner.fast_scan(MagicMock())

        assert len(signals) == 1
        assert signals[0].city == "NYC"
        assert signals[0].gap > 0.05

    @pytest.mark.asyncio
    async def test_fast_scan_no_arbitrage(self):
        scanner = ArbScanner(top_n=6, min_gap=0.05, min_buckets=3)
        scanner._hot_markets = [_hot_market("NYC")]
        scanner._discovered = True

        normal_buckets = [
            _bucket("a", 0.35, 30),
            _bucket("b", 0.35, 35),
            _bucket("c", 0.35, 40),
        ]  # sum = 1.05, no gap

        async def fake_fetch_prices(http_client, hot):
            return normal_buckets

        with patch.object(scanner, '_fetch_bucket_prices', side_effect=fake_fetch_prices):
            signals = await scanner.fast_scan(MagicMock())

        assert signals == []

    @pytest.mark.asyncio
    async def test_fast_scan_calls_callback(self):
        cb = AsyncMock()
        scanner = ArbScanner(top_n=6, min_gap=0.05, min_buckets=3, on_signal=cb)
        scanner._hot_markets = [_hot_market("NYC")]
        scanner._discovered = True

        arb_buckets = [
            _bucket("a", 0.20, 30),
            _bucket("b", 0.20, 35),
            _bucket("c", 0.20, 40),
        ]

        async def fake_fetch_prices(http_client, hot):
            return arb_buckets

        with patch.object(scanner, '_fetch_bucket_prices', side_effect=fake_fetch_prices):
            await scanner.fast_scan(MagicMock())

        cb.assert_called_once()
        sig = cb.call_args[0][0]
        assert isinstance(sig, ArbitrageSignal)

    @pytest.mark.asyncio
    async def test_fast_scan_multiple_markets(self):
        scanner = ArbScanner(top_n=6, min_gap=0.05, min_buckets=3)
        scanner._hot_markets = [
            _hot_market("NYC"),
            _hot_market("London"),
        ]
        scanner._discovered = True

        arb_buckets = [
            _bucket("a", 0.20, 30),
            _bucket("b", 0.20, 35),
            _bucket("c", 0.20, 40),
        ]
        normal_buckets = [
            _bucket("x", 0.35, 30, city="London"),
            _bucket("y", 0.35, 35, city="London"),
            _bucket("z", 0.35, 40, city="London"),
        ]

        call_count = 0

        async def fake_fetch_prices(http_client, hot):
            nonlocal call_count
            call_count += 1
            if hot.city == "NYC":
                return arb_buckets
            return normal_buckets

        with patch.object(scanner, '_fetch_bucket_prices', side_effect=fake_fetch_prices):
            signals = await scanner.fast_scan(MagicMock())

        assert call_count == 2
        assert len(signals) == 1
        assert signals[0].city == "NYC"

    @pytest.mark.asyncio
    async def test_fast_scan_handles_fetch_error(self):
        scanner = ArbScanner(top_n=6, min_gap=0.05, min_buckets=3)
        scanner._hot_markets = [_hot_market("NYC")]
        scanner._discovered = True

        async def fake_fetch_prices(http_client, hot):
            return []  # empty = error/no data

        with patch.object(scanner, '_fetch_bucket_prices', side_effect=fake_fetch_prices):
            signals = await scanner.fast_scan(MagicMock())

        assert signals == []
