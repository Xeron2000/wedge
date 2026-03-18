"""Tests for performance.py missing-field branches (lines 64, 70)."""
from __future__ import annotations

import pytest
from wedge.strategy.performance import update_city_performance


class FakeDB:
    def __init__(self, trades):
        self._trades = trades
        self._perf = {}

    async def get_settled_trades(self, start_date, end_date):
        return self._trades

    async def upsert_city_performance(self, city, window_days, brier_score, sample_count, updated_at=""):
        self._perf[(city, window_days)] = (brier_score, sample_count)

    async def get_city_performance(self, city, window_days=30):
        return self._perf.get((city, window_days), (None,))[0]


class TestMissingFields:
    @pytest.mark.asyncio
    async def test_none_p_model_skipped(self):
        """Trades with p_model=None are skipped; if valid < min_samples, return None."""
        trades = [{"city": "NYC", "date": "2026-03-01", "p_model": None, "pnl": 1.0}] * 10
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is None

    @pytest.mark.asyncio
    async def test_none_pnl_skipped(self):
        """Trades with pnl=None are skipped."""
        trades = [{"city": "NYC", "date": "2026-03-01", "p_model": 0.7, "pnl": None}] * 10
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is None

    @pytest.mark.asyncio
    async def test_partial_valid_below_min_returns_none(self):
        """3 valid trades (< 5 min_samples) returns None."""
        valid = [{"city": "NYC", "date": "2026-03-01", "p_model": 0.7, "pnl": 1.0}] * 3
        nulls = [{"city": "NYC", "date": "2026-03-01", "p_model": None, "pnl": None}] * 7
        db = FakeDB(valid + nulls)
        result = await update_city_performance(db, "NYC")
        assert result is None

    @pytest.mark.asyncio
    async def test_five_valid_returns_score(self):
        """Exactly 5 valid trades should return a Brier score."""
        trades = [{"city": "NYC", "date": "2026-03-01", "p_model": 0.8, "pnl": 1.0}] * 5
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is not None
        assert 0.0 <= result <= 1.0
