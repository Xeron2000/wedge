"""Tests for risk management modules."""

import pytest
from datetime import date, datetime, timedelta

from wedge.risk.circuit_breaker import (
    BreakerLevel,
    CircuitBreaker,
    CircuitBreakerConfig,
)
from wedge.risk.correlation import (
    build_correlation_matrix,
    calculate_portfolio_exposure,
    check_correlation_limit,
    get_diversification_score,
    get_base_correlation,
)
from wedge.market.models import MarketBucket, Position


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_init(self):
        """Test circuit breaker initialization."""
        cb = CircuitBreaker()
        cb.initialize(1000.0)

        assert cb._initial_bankroll == 1000.0
        assert cb._current_bankroll == 1000.0
        assert cb._peak_bankroll == 1000.0
        assert cb.can_trade() is True

    def test_daily_loss_limit(self):
        """Test daily loss limit trigger."""
        config = CircuitBreakerConfig(daily_loss_limit=0.05)  # 5%
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # Lose $50 (5% of $1000)
        cb.record_trade(pnl=-50.0)

        assert cb.can_trade() is False
        assert "Daily loss" in (cb.get_halt_reason() or "")

    def test_weekly_loss_limit(self):
        """Test weekly loss limit trigger."""
        config = CircuitBreakerConfig(
            weekly_loss_limit=0.10,  # 10%
            daily_loss_limit=1.0,    # Disable daily
        )
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # Lose $100 over multiple trades
        cb.record_trade(pnl=-25.0)
        assert cb.can_trade() is True

        cb.record_trade(pnl=-25.0)
        assert cb.can_trade() is True

        cb.record_trade(pnl=-25.0)
        assert cb.can_trade() is True

        cb.record_trade(pnl=-25.0)  # Total: -$100 = 10%

        assert cb.can_trade() is False

    def test_consecutive_losses(self):
        """Test consecutive losses trigger."""
        config = CircuitBreakerConfig(
            max_consecutive_losses=3,
            daily_loss_limit=1.0,  # Disable
        )
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # 3 consecutive losses
        cb.record_trade(pnl=-10.0)
        cb.record_trade(pnl=-10.0)
        cb.record_trade(pnl=-10.0)

        assert cb.can_trade() is False
        assert "Consecutive losses" in (cb.get_halt_reason() or "")

    def test_consecutive_losses_reset_on_win(self):
        """Test that wins reset consecutive loss counter."""
        config = CircuitBreakerConfig(
            max_consecutive_losses=3,
            daily_loss_limit=1.0,
        )
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # 2 losses, 1 win, 2 more losses = should not trigger
        cb.record_trade(pnl=-10.0)
        cb.record_trade(pnl=-10.0)
        cb.record_trade(pnl=20.0)  # Win resets counter
        cb.record_trade(pnl=-10.0)
        cb.record_trade(pnl=-10.0)

        assert cb.can_trade() is True

    def test_brier_score_trigger(self):
        """Test Brier score trigger."""
        config = CircuitBreakerConfig(
            brier_score_threshold=0.25,
            daily_loss_limit=1.0,
        )
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # Add bad predictions (p=0.8 but outcome=0)
        for _ in range(15):
            cb.record_trade(pnl=-10.0, p_model=0.8, outcome=0)

        assert cb.can_trade() is False
        assert "Brier" in (cb.get_halt_reason() or "")

    def test_drawdown_tracking(self):
        """Test drawdown tracking."""
        config = CircuitBreakerConfig(
            max_drawdown=0.15,  # 15%
            daily_loss_limit=1.0,
        )
        cb = CircuitBreaker(config=config)
        cb.initialize(1000.0)

        # Bankroll goes up then down
        cb.update_bankroll(1200.0)  # New peak
        cb.update_bankroll(1000.0)  # Drawdown = $200 / $1200 = 16.7%

        assert cb.can_trade() is False
        assert "drawdown" in (cb.get_halt_reason() or "").lower()

    def test_get_state(self):
        """Test state reporting."""
        cb = CircuitBreaker()
        cb.initialize(1000.0)

        cb.record_trade(pnl=-30.0)  # 3% loss

        state = cb.get_state()

        assert "daily_loss" in state
        assert "weekly_loss" in state
        assert "drawdown" in state
        assert "consecutive_losses" in state
        assert "brier_score" in state

        # Should be GREEN or YELLOW depending on utilization
        # -30 / 50 = 0.6 utilization, which is < 0.8 warning threshold
        assert state["daily_loss"].level in (BreakerLevel.GREEN, BreakerLevel.YELLOW)


class TestCorrelationMatrix:
    """Test correlation matrix functionality."""

    def test_same_city_correlation(self):
        """Test that same city has correlation of 1.0."""
        matrix = build_correlation_matrix(["NYC", "Miami", "Chicago"])

        assert matrix.get_correlation("NYC", "NYC") == 1.0
        assert matrix.get_correlation("Miami", "Miami") == 1.0

    def test_same_region_correlation(self):
        """Test that cities in same region have high correlation."""
        # NYC and Boston are both in northeast_us
        corr = get_base_correlation("NYC", "Boston")
        assert corr > 0.7  # High correlation

    def test_different_region_correlation(self):
        """Test that cities in different regions have lower correlation."""
        # NYC (northeast) and Wellington (oceania)
        corr = get_base_correlation("NYC", "Wellington")
        assert corr < 0.3  # Low correlation

    def test_portfolio_exposure_empty(self):
        """Test portfolio exposure with no positions."""
        exposure = calculate_portfolio_exposure([])

        assert exposure.total_exposure == 0.0
        assert exposure.var_95 == 0.0
        assert exposure.var_99 == 0.0

    def test_portfolio_exposure_single_position(self):
        """Test portfolio exposure with single position."""
        positions = [
            Position(
                bucket=MarketBucket(
                    token_id="test1",
                    city="NYC",
                    date=date.today(),
                    temp_f=80,
                    market_price=0.30,
                    implied_prob=0.30,
                ),
                size=50.0,
                entry_price=0.30,
                strategy="ladder",
            )
        ]

        exposure = calculate_portfolio_exposure(positions)

        assert exposure.total_exposure == 50.0
        assert exposure.max_single_city == 50.0
        assert exposure.max_single_city_pct == 1.0

    def test_portfolio_exposure_diversified(self):
        """Test portfolio exposure with diversified positions."""
        positions = [
            Position(
                bucket=MarketBucket(
                    token_id="test1",
                    city="NYC",
                    date=date.today(),
                    temp_f=80,
                    market_price=0.30,
                    implied_prob=0.30,
                ),
                size=50.0,
                entry_price=0.30,
                strategy="ladder",
            ),
            Position(
                bucket=MarketBucket(
                    token_id="test2",
                    city="Wellington",
                    date=date.today(),
                    temp_f=70,
                    market_price=0.25,
                    implied_prob=0.25,
                ),
                size=50.0,
                entry_price=0.25,
                strategy="tail",
            ),
        ]

        exposure = calculate_portfolio_exposure(positions)

        assert exposure.total_exposure == 100.0
        # Diversification ratio should be positive for uncorrelated cities
        assert exposure.diversification_ratio > 0.0

    def test_diversification_score(self):
        """Test diversification score calculation."""
        # Single city = low score
        single_positions = [
            Position(
                bucket=MarketBucket(
                    token_id="test1",
                    city="NYC",
                    date=date.today(),
                    temp_f=80,
                    market_price=0.30,
                    implied_prob=0.30,
                ),
                size=50.0,
                entry_price=0.30,
                strategy="ladder",
            )
        ]
        score_single = get_diversification_score(single_positions)

        # Multiple cities = higher score
        multi_positions = [
            Position(
                bucket=MarketBucket(
                    token_id="test1",
                    city="NYC",
                    date=date.today(),
                    temp_f=80,
                    market_price=0.30,
                    implied_prob=0.30,
                ),
                size=50.0,
                entry_price=0.30,
                strategy="ladder",
            ),
            Position(
                bucket=MarketBucket(
                    token_id="test2",
                    city="London",
                    date=date.today(),
                    temp_f=65,
                    market_price=0.25,
                    implied_prob=0.25,
                ),
                size=50.0,
                entry_price=0.25,
                strategy="ladder",
            ),
            Position(
                bucket=MarketBucket(
                    token_id="test3",
                    city="Tokyo",
                    date=date.today(),
                    temp_f=75,
                    market_price=0.35,
                    implied_prob=0.35,
                ),
                size=50.0,
                entry_price=0.35,
                strategy="ladder",
            ),
        ]
        score_multi = get_diversification_score(multi_positions)

        assert score_multi > score_single

    def test_correlation_limit_check(self):
        """Test correlation limit checking."""
        existing_positions = [
            Position(
                bucket=MarketBucket(
                    token_id="test1",
                    city="NYC",
                    date=date.today(),
                    temp_f=80,
                    market_price=0.30,
                    implied_prob=0.30,
                ),
                size=200.0,
                entry_price=0.30,
                strategy="ladder",
            ),
            Position(
                bucket=MarketBucket(
                    token_id="test2",
                    city="Boston",
                    date=date.today(),
                    temp_f=78,
                    market_price=0.28,
                    implied_prob=0.28,
                ),
                size=200.0,
                entry_price=0.28,
                strategy="ladder",
            ),
        ]

        # NYC and Boston are highly correlated
        # Total correlated exposure = $400
        allowed = check_correlation_limit(
            existing_positions,
            new_city="NYC",
            max_correlated_exposure=500.0,
            max_correlation=0.7,
        )

        # Should be allowed (400 < 500)
        assert allowed is True

        # But adding another correlated city should be rejected
        allowed = check_correlation_limit(
            existing_positions,
            new_city="NYC",
            max_correlated_exposure=300.0,  # Lower limit
            max_correlation=0.7,
        )

        assert allowed is False


class TestIntegration:
    """Integration tests for risk management."""

    def test_circuit_breaker_with_real_positions(self):
        """Test circuit breaker integration with positions."""
        cb = CircuitBreaker()
        cb.initialize(1000.0)

        # Simulate a losing streak
        for i in range(5):
            pnl = -20.0 - (i * 5)  # Increasing losses
            cb.record_trade(pnl=pnl, p_model=0.6, outcome=0)

            if not cb.can_trade():
                break

        # Should be halted by now
        assert cb.can_trade() is False

    def test_correlation_matrix_positive_semidefinite(self):
        """Test that correlation matrix is positive semi-definite."""
        import numpy as np

        cities = ["NYC", "Miami", "London", "Tokyo", "Sydney"]
        matrix = build_correlation_matrix(cities)

        # All eigenvalues should be >= 0
        eigenvalues = np.linalg.eigvalsh(matrix.matrix)
        assert all(e >= -1e-10 for e in eigenvalues)  # Small numerical tolerance
