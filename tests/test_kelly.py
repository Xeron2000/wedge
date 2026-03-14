from __future__ import annotations

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from wedge.strategy.kelly import fractional_kelly


class TestKelly:
    def test_basic_positive_edge(self):
        bet = fractional_kelly(p_model=0.77, market_price=0.68, bankroll=1000)
        assert bet > 0
        assert bet <= 100  # max_bet
        assert bet <= 1000 * 0.05  # max_bet_pct

    def test_negative_edge_returns_zero(self):
        bet = fractional_kelly(p_model=0.10, market_price=0.20, bankroll=1000)
        assert bet == 0.0

    def test_zero_edge_returns_zero(self):
        bet = fractional_kelly(p_model=0.20, market_price=0.20, bankroll=1000)
        assert bet == 0.0

    def test_zero_bankroll(self):
        bet = fractional_kelly(p_model=0.80, market_price=0.50, bankroll=0)
        assert bet == 0.0

    def test_negative_bankroll(self):
        bet = fractional_kelly(p_model=0.80, market_price=0.50, bankroll=-100)
        assert bet == 0.0

    def test_price_at_zero(self):
        bet = fractional_kelly(p_model=0.80, market_price=0.0, bankroll=1000)
        assert bet == 0.0

    def test_price_at_one(self):
        bet = fractional_kelly(p_model=0.80, market_price=1.0, bankroll=1000)
        assert bet == 0.0

    def test_max_bet_cap(self):
        bet = fractional_kelly(
            p_model=0.99, market_price=0.01, bankroll=100000, max_bet=100
        )
        assert bet <= 100

    def test_max_bet_pct_cap(self):
        bet = fractional_kelly(
            p_model=0.99, market_price=0.01, bankroll=1000, max_bet_pct=0.05
        )
        assert bet <= 50  # 1000 * 0.05


class TestKellyPBT:
    @given(
        p_model=st.floats(min_value=0, max_value=1),
        market_price=st.floats(min_value=0, max_value=1),
        bankroll=st.floats(min_value=-1000, max_value=100000),
    )
    @settings(max_examples=500)
    def test_always_clamped(self, p_model, market_price, bankroll):
        bet = fractional_kelly(p_model, market_price, bankroll)
        assert math.isfinite(bet)
        assert bet >= 0
        cap = min(100, max(0, bankroll) * 0.05)
        assert bet <= cap + 1e-9

    @given(
        market_price=st.floats(min_value=0.01, max_value=0.99),
        bankroll=st.floats(min_value=100, max_value=10000),
    )
    @settings(max_examples=200)
    def test_monotonic_in_p_model(self, market_price, bankroll):
        """Higher p_model should never decrease the bet size."""
        p1 = market_price + 0.05
        p2 = market_price + 0.10
        if p1 >= 1 or p2 >= 1:
            return
        bet1 = fractional_kelly(p1, market_price, bankroll)
        bet2 = fractional_kelly(p2, market_price, bankroll)
        assert bet2 >= bet1 - 1e-9  # allow floating point tolerance
