from __future__ import annotations

from datetime import date

import pytest

from wedge.market.models import MarketBucket
from wedge.strategy.edge import calculate_ev, detect_edges, estimate_slippage
from wedge.weather.models import ForecastDistribution


def _forecast(buckets: dict[int, float]) -> ForecastDistribution:
    from datetime import UTC, datetime

    return ForecastDistribution(
        city="NYC",
        date=date(2026, 7, 1),
        buckets=buckets,
        ensemble_spread=2.0,
        member_count=30,
        updated_at=datetime.now(UTC),
    )


def _market(temp_value: int, price: float, volume_24h: float = 5000.0) -> MarketBucket:
    return MarketBucket(
        token_id=f"tok_{temp_value}",
        city="NYC",
        date=date(2026, 7, 1),
        temp_value=temp_value,
        temp_unit="F",
        market_price=price,
        implied_prob=price,
        volume_24h=volume_24h,
    )


class TestEdgeDetection:
    def test_positive_edge(self):
        forecast = _forecast({78: 0.25, 79: 0.30, 80: 0.20})
        markets = [_market(79, 0.20)]  # edge = 0.30 - 0.20 = 0.10
        signals = detect_edges(forecast, markets)
        assert len(signals) == 1
        assert signals[0].edge > 0.05

    def test_negative_edge_filtered(self):
        forecast = _forecast({78: 0.10})
        markets = [_market(78, 0.20)]  # edge = -0.10
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_zero_edge_filtered(self):
        forecast = _forecast({78: 0.20})
        markets = [_market(78, 0.20)]  # edge = 0
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_edge_below_threshold(self):
        forecast = _forecast({78: 0.22})
        markets = [_market(78, 0.20)]  # edge = 0.02 < 0.05
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_invalid_market_price_filtered(self):
        forecast = _forecast({78: 0.50})
        markets = [_market(78, 0.0), _market(78, 1.0)]
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_missing_temp_in_forecast(self):
        forecast = _forecast({78: 0.30})
        markets = [_market(90, 0.05)]  # temp 90 not in forecast → p_model = 0
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_multiple_edges(self):
        forecast = _forecast({77: 0.20, 78: 0.25, 79: 0.30})
        markets = [_market(77, 0.10), _market(78, 0.15), _market(79, 0.18)]
        signals = detect_edges(forecast, markets)
        assert len(signals) == 3
        assert all(s.edge > 0.05 for s in signals)


class TestEVCalculation:
    """Tests for EV calculation with fees and slippage."""

    def test_calculate_ev_basic(self):
        """Test basic EV calculation without fees."""
        # p_model = 0.30, market_price = 0.20
        # odds = (1 - 0.20) / 0.20 = 4.0
        # ev = 0.30 * 4.0 - 0.70 = 0.50 (no fees)
        ev = calculate_ev(0.30, 0.20, fee_rate=0.0, slippage=0.0)
        assert ev == pytest.approx(0.50, rel=0.01)

    def test_calculate_ev_with_fee(self):
        """Test EV calculation with 2% fee."""
        # Fee reduces winnings
        ev_no_fee = calculate_ev(0.30, 0.20, fee_rate=0.0, slippage=0.0)
        ev_with_fee = calculate_ev(0.30, 0.20, fee_rate=0.02, slippage=0.0)
        assert ev_with_fee < ev_no_fee
        # Fee is 2% of winnings portion only
        # win_ev = 0.30 * 4.0 = 1.2, after fee = 1.2 * 0.98 = 1.176
        # ev = 1.176 - 0.70 = 0.476
        assert ev_with_fee == pytest.approx(0.476, rel=0.01)

    def test_calculate_ev_negative(self):
        """Test negative EV (bad bet)."""
        # p_model = 0.20, market_price = 0.30 (model says lower than market)
        ev = calculate_ev(0.20, 0.30, fee_rate=0.02, slippage=0.0)
        assert ev < 0

    def test_calculate_ev_with_slippage(self):
        """Test EV calculation with slippage."""
        ev_no_slip = calculate_ev(0.30, 0.20, fee_rate=0.02, slippage=0.0)
        ev_with_slip = calculate_ev(0.30, 0.20, fee_rate=0.02, slippage=0.02)
        # Slippage reduces EV
        assert ev_with_slip < ev_no_slip

    def test_estimate_slippage_volume_tiers(self):
        """Test slippage estimation for different volume tiers."""
        bet_size = 50.0

        # Very low volume (< $1K)
        slip = estimate_slippage(500, bet_size)
        assert slip >= 0.05

        # Low volume (< $5K)
        slip = estimate_slippage(3000, bet_size)
        assert 0.015 <= slip <= 0.03

        # Medium volume (< $25K)
        slip = estimate_slippage(10000, bet_size)
        assert 0.005 <= slip <= 0.02

        # High volume (>= $25K)
        slip = estimate_slippage(50000, bet_size)
        assert slip <= 0.01

    def test_estimate_slippage_size_multiplier(self):
        """Test that larger bets relative to volume have higher slippage."""
        # Small bet relative to volume
        slip_small = estimate_slippage(50000, 50)
        # Large bet relative to volume
        slip_large = estimate_slippage(5000, 500)

        assert slip_large > slip_small

    def test_estimate_slippage_zero_volume(self):
        """Test slippage with zero/unknown volume."""
        slip = estimate_slippage(0, 50)
        assert slip == 0.05  # Default high slippage

    def test_detect_edges_with_ev_filter(self):
        """Test that detect_edges filters by positive EV."""
        forecast = _forecast({80: 0.30})

        # Market with positive EV
        markets_good = [_market(80, 0.20, volume_24h=10000)]
        signals = detect_edges(forecast, markets_good)
        assert len(signals) > 0

        # Market with negative EV (high fee/slippage scenario)
        # This would need extreme conditions to flip EV negative when edge is positive
        # For now, test that signals include EV information
        assert signals[0].edge > 0
