"""Tests for tail strategy with risk controls."""

from datetime import date

import pytest

from wedge.market.models import MarketBucket, Position
from wedge.strategy.models import EdgeSignal
from wedge.strategy.tail import (
    check_daily_loss_limit,
    check_event_duplication,
    check_regional_correlation,
    evaluate_tail,
    get_climate_region,
)


def _signal(city: str, temp_f: int, p_model: float, market_price: float,
              date_offset: int = 0) -> EdgeSignal:
    """Create an edge signal."""
    return EdgeSignal(
        city=city,
        date=date(2026, 7, 1 + date_offset),
        temp_f=temp_f,
        token_id=f"tok_{city}_{temp_f}",
        p_model=p_model,
        p_market=market_price,
        edge=p_model - market_price,
        odds=(1.0 - market_price) / market_price,
    )


def _position(signal: EdgeSignal, size: float = 50.0) -> Position:
    """Create a position from signal."""
    return Position(
        bucket=MarketBucket(
            token_id=signal.token_id,
            city=signal.city,
            date=signal.date,
            temp_f=signal.temp_f,
            market_price=signal.p_market,
            implied_prob=signal.p_market,
        ),
        size=size,
        entry_price=signal.p_market,
        strategy="tail",
        p_model=signal.p_model,
        edge=signal.edge,
    )


class TestClimateRegion:
    """Test climate region mapping."""

    def test_northeast_us(self):
        assert get_climate_region("NYC") is not None

    def test_south_us(self):
        assert get_climate_region("Miami") is not None

    def test_uk_europe(self):
        assert get_climate_region("London") is not None

    def test_east_asia(self):
        assert get_climate_region("Seoul") is not None
        assert get_climate_region("Shanghai") is not None

    def test_oceania(self):
        assert get_climate_region("Wellington") is not None

    def test_unknown_city(self):
        assert get_climate_region("UnknownCity") is None


class TestRegionalCorrelation:
    """Test regional correlation checking."""

    def test_no_existing_positions(self):
        """Test with no existing positions - should allow."""
        signal = _signal("NYC", 85, 0.30, 0.15)
        result = check_regional_correlation([], signal, max_correlated=2)
        assert result is True

    def test_one_position_same_region(self):
        """Test with one position in same region - should allow."""
        signal = _signal("NYC", 85, 0.30, 0.15)
        existing = [_position(_signal("NYC", 80, 0.25, 0.10))]

        result = check_regional_correlation(existing, signal, max_correlated=2)
        assert result is True

    def test_two_positions_same_region(self):
        """Test with two positions in same region - should block."""
        signal = _signal("NYC", 85, 0.30, 0.15)
        existing = [
            _position(_signal("NYC", 80, 0.25, 0.10)),
            _position(_signal("NYC", 82, 0.28, 0.12)),
        ]

        result = check_regional_correlation(existing, signal, max_correlated=2)
        assert result is False

    def test_positions_different_regions(self):
        """Test with positions in different regions - should allow."""
        signal = _signal("NYC", 85, 0.30, 0.15)
        existing = [
            _position(_signal("London", 75, 0.25, 0.10)),
            _position(_signal("Miami", 90, 0.28, 0.12)),
        ]

        result = check_regional_correlation(existing, signal, max_correlated=2)
        assert result is True

    def test_seoul_shanghai_correlated(self):
        """Test that Seoul and Shanghai are in same region (East Asia)."""
        signal_seoul = _signal("Seoul", 85, 0.30, 0.15)
        existing = [_position(_signal("Shanghai", 88, 0.28, 0.12))]

        # Both in East Asia region
        result = check_regional_correlation(existing, signal_seoul, max_correlated=1)
        assert result is False


class TestEventDuplication:
    """Test event duplication checking."""

    def test_same_city_same_date(self):
        """Test same city, same date - should block."""
        signal = _signal("NYC", 85, 0.30, 0.15, date_offset=0)
        existing = [_position(_signal("NYC", 85, 0.25, 0.10, date_offset=0))]

        result = check_event_duplication(existing, signal)
        assert result is False

    def test_same_city_close_date(self):
        """Test same city, close date (within window) - should block."""
        signal = _signal("NYC", 85, 0.30, 0.15, date_offset=2)
        existing = [_position(_signal("NYC", 85, 0.25, 0.10, date_offset=0))]

        result = check_event_duplication(existing, signal)
        assert result is False

    def test_same_city_far_date(self):
        """Test same city, far date (outside window) - should allow."""
        signal = _signal("NYC", 85, 0.30, 0.15, date_offset=10)
        existing = [_position(_signal("NYC", 85, 0.25, 0.10, date_offset=0))]

        result = check_event_duplication(existing, signal)
        assert result is True

    def test_different_city_same_date(self):
        """Test different city, same date - should allow."""
        signal = _signal("NYC", 85, 0.30, 0.15, date_offset=0)
        existing = [_position(_signal("London", 75, 0.25, 0.10, date_offset=0))]

        result = check_event_duplication(existing, signal)
        assert result is True


class TestDailyLossLimit:
    """Test daily loss limit checking."""

    def test_no_positions(self):
        """Test with no positions - should allow trading."""
        result = check_daily_loss_limit([], daily_loss_limit=200.0)
        assert result is True

    def test_small_loss(self):
        """Test with small loss - should allow trading."""
        positions = [_position(_signal("NYC", 85, 0.30, 0.15), size=50.0)]
        result = check_daily_loss_limit(positions, daily_loss_limit=200.0)
        assert result is True

    def test_exceeds_loss_limit(self):
        """Test with losses exceeding limit - should block."""
        # Create positions with significant losses
        positions = [
            _position(_signal("NYC", 85, 0.30, 0.15), size=100.0),
            _position(_signal("NYC", 86, 0.28, 0.12), size=100.0),
            _position(_signal("NYC", 87, 0.25, 0.10), size=100.0),
        ]
        result = check_daily_loss_limit(positions, daily_loss_limit=200.0)
        # Should block when losses exceed $200
        assert result is False


class TestEvaluateTail:
    """Test full tail strategy evaluation."""

    def test_no_signals(self):
        """Test with no qualifying signals."""
        signals = [_signal("NYC", 85, 0.15, 0.20)]  # Negative edge
        positions = evaluate_tail(signals, budget=500.0)
        assert len(positions) == 0

    def test_signals_below_odds_threshold(self):
        """Test with signals below odds threshold."""
        # Low odds (less than 10.0)
        signals = [_signal("NYC", 85, 0.30, 0.50)]  # odds = 1.0
        positions = evaluate_tail(signals, budget=500.0, min_odds=10.0)
        assert len(positions) == 0

    def test_single_tail_position(self):
        """Test with single qualifying tail signal."""
        signals = [_signal("NYC", 95, 0.15, 0.05)]  # High edge, high odds
        positions = evaluate_tail(
            signals, budget=500.0,
            edge_threshold=0.08, min_odds=10.0
        )
        assert len(positions) == 1
        assert positions[0].strategy == "tail"

    def test_regional_correlation_limit(self):
        """Test that regional correlation limit is enforced."""
        # Multiple signals for same region
        signals = [
            _signal("NYC", 85, 0.30, 0.10),  # High edge
            _signal("NYC", 86, 0.28, 0.10),  # High edge
            _signal("NYC", 87, 0.25, 0.10),  # High edge
        ]
        positions = evaluate_tail(
            signals, budget=500.0,
            max_correlated=2,  # Only allow 2 per region
        )
        # Should only have 2 positions max for same region
        assert len(positions) <= 2

    def test_daily_loss_limit_stops_trading(self):
        """Test that daily loss limit stops trading."""
        signals = [_signal("NYC", 95, 0.15, 0.05)]
        # Existing positions with large losses
        existing_losses = [
            _position(_signal("NYC", 80, 0.30, 0.15), size=100.0),
            _position(_signal("NYC", 81, 0.28, 0.12), size=100.0),
        ]

        positions = evaluate_tail(
            signals, budget=500.0,
            daily_loss_limit=50.0,  # Very low limit
            existing_positions=existing_losses,
        )
        # Should return empty due to loss limit
        assert len(positions) == 0

    def test_event_duplication_prevention(self):
        """Test that event duplication is prevented."""
        # Same city, close dates
        signals = [
            _signal("NYC", 85, 0.30, 0.10, date_offset=0),
            _signal("NYC", 85, 0.28, 0.10, date_offset=1),  # Same event
        ]
        existing = [_position(_signal("NYC", 85, 0.25, 0.10, date_offset=0))]

        positions = evaluate_tail(
            signals, budget=500.0,
            existing_positions=existing,
        )
        # Should filter out duplicate event
        assert len(positions) < len(signals)
