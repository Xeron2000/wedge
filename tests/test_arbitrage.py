"""Tests for strategy/arbitrage.py - cross-bucket arbitrage detection."""
from __future__ import annotations

from datetime import date

import pytest

from wedge.market.models import MarketBucket
from wedge.strategy.arbitrage import ArbitrageSignal, detect_bucket_arbitrage


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
    )


class TestDetectBucketArbitrage:
    def test_detects_gap_below_threshold(self):
        buckets = [
            _bucket("a", 0.15, 30),
            _bucket("b", 0.30, 35),
            _bucket("c", 0.25, 40),
            _bucket("d", 0.21, 45),
        ]  # sum = 0.91
        sig = detect_bucket_arbitrage(buckets)
        assert sig is not None
        assert abs(sig.price_sum - 0.91) < 1e-9
        assert abs(sig.gap - 0.09) < 1e-6
        assert sig.bucket_count == 4
        assert set(sig.token_ids) == {"a", "b", "c", "d"}

    def test_no_signal_when_sum_at_threshold(self):
        # sum = exactly 0.95 → no signal (not strictly less than)
        buckets = [
            _bucket("a", 0.30, 30),
            _bucket("b", 0.35, 35),
            _bucket("c", 0.30, 40),
        ]  # sum = 0.95
        sig = detect_bucket_arbitrage(buckets)
        assert sig is None

    def test_no_signal_when_sum_above_threshold(self):
        buckets = [
            _bucket("a", 0.35, 30),
            _bucket("b", 0.40, 35),
            _bucket("c", 0.30, 40),
        ]  # sum = 1.05
        sig = detect_bucket_arbitrage(buckets)
        assert sig is None

    def test_too_few_buckets_returns_none(self):
        buckets = [
            _bucket("a", 0.10, 30),
            _bucket("b", 0.10, 35),
        ]  # only 2, min_buckets=3
        sig = detect_bucket_arbitrage(buckets)
        assert sig is None

    def test_empty_buckets_returns_none(self):
        assert detect_bucket_arbitrage([]) is None

    def test_mixed_cities_returns_none(self):
        buckets = [
            _bucket("a", 0.10, 30, city="NYC"),
            _bucket("b", 0.10, 35, city="London"),
            _bucket("c", 0.10, 40, city="NYC"),
        ]
        sig = detect_bucket_arbitrage(buckets)
        assert sig is None

    def test_mixed_dates_returns_none(self):
        buckets = [
            _bucket("a", 0.10, 30, dt=date(2026, 7, 1)),
            _bucket("b", 0.10, 35, dt=date(2026, 7, 2)),
            _bucket("c", 0.10, 40, dt=date(2026, 7, 1)),
        ]
        sig = detect_bucket_arbitrage(buckets)
        assert sig is None

    def test_custom_threshold(self):
        buckets = [
            _bucket("a", 0.30, 30),
            _bucket("b", 0.30, 35),
            _bucket("c", 0.30, 40),
        ]  # sum = 0.90
        # Default threshold 0.95: signal
        assert detect_bucket_arbitrage(buckets, threshold=0.95) is not None
        # Stricter threshold 0.85: no signal
        assert detect_bucket_arbitrage(buckets, threshold=0.85) is None

    def test_custom_min_buckets(self):
        buckets = [
            _bucket("a", 0.10, 30),
            _bucket("b", 0.10, 35),
        ]  # sum = 0.20, gap huge
        # Default min_buckets=3: no signal
        assert detect_bucket_arbitrage(buckets, min_buckets=3) is None
        # Relaxed min_buckets=2: signal
        assert detect_bucket_arbitrage(buckets, min_buckets=2) is not None

    def test_signal_fields_correct(self):
        buckets = [
            _bucket("x", 0.20, 30),
            _bucket("y", 0.20, 35),
            _bucket("z", 0.20, 40),
        ]  # sum = 0.60
        sig = detect_bucket_arbitrage(buckets)
        assert sig is not None
        assert sig.city == "NYC"
        assert sig.date == date(2026, 7, 1)
        assert sig.expected_profit_pct == sig.gap
        assert sig.bucket_count == 3

    def test_str_representation(self):
        buckets = [
            _bucket("a", 0.20, 30),
            _bucket("b", 0.20, 35),
            _bucket("c", 0.20, 40),
        ]
        sig = detect_bucket_arbitrage(buckets)
        s = str(sig)
        assert "ARBITRAGE" in s
        assert "NYC" in s
        assert "profit" in s

    def test_bucket_count_property(self):
        buckets = [_bucket(str(i), 0.05, 30 + i) for i in range(10)]
        sig = detect_bucket_arbitrage(buckets, min_buckets=3)
        assert sig is not None
        assert sig.bucket_count == 10

    def test_near_threshold_precision(self):
        # sum = 0.9499... should trigger, 0.9500 should not
        buckets3 = [
            _bucket("a", 0.3166, 30),
            _bucket("b", 0.3166, 35),
            _bucket("c", 0.3167, 40),
        ]  # sum ≈ 0.9499
        sig = detect_bucket_arbitrage(buckets3)
        assert sig is not None

    def test_price_sum_accuracy(self):
        prices = [0.12, 0.18, 0.22, 0.15, 0.20]
        buckets = [_bucket(str(i), p, 30 + i) for i, p in enumerate(prices)]
        sig = detect_bucket_arbitrage(buckets, min_buckets=3)
        assert sig is not None
        assert abs(sig.price_sum - sum(prices)) < 1e-9
        assert abs(sig.gap - (1.0 - sum(prices))) < 1e-9



class TestGroupBucketsByCityDate:
    def test_groups_by_city_and_date(self):
        from wedge.strategy.arbitrage import group_buckets_by_city_date
        from datetime import date
        b1 = _bucket("a", 0.2, 30, "NYC", date(2026, 7, 1))
        b2 = _bucket("b", 0.2, 35, "NYC", date(2026, 7, 1))
        b3 = _bucket("c", 0.2, 30, "London", date(2026, 7, 1))
        b4 = _bucket("d", 0.2, 30, "NYC", date(2026, 7, 2))
        groups = group_buckets_by_city_date([b1, b2, b3, b4])
        assert len(groups) == 3
        assert len(groups[("NYC", date(2026, 7, 1))]) == 2
        assert len(groups[("London", date(2026, 7, 1))]) == 1
        assert len(groups[("NYC", date(2026, 7, 2))]) == 1

    def test_empty_input(self):
        from wedge.strategy.arbitrage import group_buckets_by_city_date
        assert group_buckets_by_city_date([]) == {}


class TestScanArbitrage:
    def test_finds_multiple_opportunities(self):
        from wedge.strategy.arbitrage import scan_arbitrage
        from datetime import date
        nyc = [_bucket(str(i), 0.15, 30+i, "NYC", date(2026,7,1)) for i in range(4)]
        lon = [_bucket(str(i+10), 0.18, 30+i, "London", date(2026,7,1)) for i in range(4)]
        sigs = scan_arbitrage(nyc + lon, min_buckets=3)
        assert len(sigs) == 2
        assert sigs[0].gap >= sigs[1].gap

    def test_no_opportunities(self):
        from wedge.strategy.arbitrage import scan_arbitrage
        buckets = [_bucket(str(i), 0.25, 30+i) for i in range(4)]
        sigs = scan_arbitrage(buckets, min_buckets=3)
        assert sigs == []

    def test_str_representation(self):
        buckets = [_bucket(str(i), 0.2, 30+i) for i in range(4)]
        from wedge.strategy.arbitrage import detect_bucket_arbitrage
        sig = detect_bucket_arbitrage(buckets, min_buckets=3)
        assert sig is not None
        s = str(sig)
        assert "ARBITRAGE" in s
        assert "NYC" in s
        assert "gap" in s
        assert sig.expected_profit_pct == sig.gap
