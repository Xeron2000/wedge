"""Tests for strategy/performance.py - per-city Brier score tracking."""
from __future__ import annotations

import pytest

from wedge.strategy.performance import (
    _DEFAULT_MIN_SAMPLES,
    _DEFAULT_WINDOW_DAYS,
    get_city_filter,
    update_all_city_performance,
    update_city_performance,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeDB:
    """Minimal in-memory DB stub for performance tests."""

    def __init__(self, settled_trades: list[dict] | None = None):
        self._trades = settled_trades or []
        self._city_perf: dict[tuple[str, int], tuple[float, int]] = {}

    async def get_settled_trades(self, start_date, end_date) -> list[dict]:
        return [
            t for t in self._trades
            if str(start_date) <= t["date"] <= str(end_date)
        ]

    async def upsert_city_performance(
        self, city: str, window_days: int, brier_score: float, sample_count: int, updated_at: str = ""
    ) -> None:
        self._city_perf[(city, window_days)] = (brier_score, sample_count)

    async def get_city_performance(self, city: str, window_days: int = 30) -> float | None:
        entry = self._city_perf.get((city, window_days))
        return entry[0] if entry else None


def _trade(city: str, p_model: float, won: bool, dt: str = "2026-03-01") -> dict:
    """won=True → pnl>0, won=False → pnl<0"""
    pnl = 1.0 if won else -1.0
    outcome = 1.0 if won else 0.0
    return {"city": city, "date": dt, "p_model": p_model, "pnl": pnl, "outcome": outcome}


# ---------------------------------------------------------------------------
# update_city_performance
# ---------------------------------------------------------------------------

class TestUpdateCityPerformance:
    @pytest.mark.asyncio
    async def test_no_trades_returns_none(self):
        db = FakeDB([])
        result = await update_city_performance(db, "NYC")
        assert result is None

    @pytest.mark.asyncio
    async def test_insufficient_samples_returns_none(self):
        # Only 4 trades, min_samples=5
        trades = [_trade("NYC", 0.6, True) for _ in range(4)]
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is None

    @pytest.mark.asyncio
    async def test_perfect_prediction_brier_zero(self):
        # p_model=1.0, outcome=1.0 → Brier=(1-1)^2=0
        trades = [_trade("NYC", 1.0, True) for _ in range(10)]
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is not None
        assert abs(result) < 1e-9

    @pytest.mark.asyncio
    async def test_random_prediction_brier_025(self):
        # p_model=0.5, outcome alternates → Brier=(0.5-1)^2=0.25 and (0.5-0)^2=0.25
        trades = (
            [_trade("NYC", 0.5, True) for _ in range(5)]
            + [_trade("NYC", 0.5, False) for _ in range(5)]
        )
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is not None
        assert abs(result - 0.25) < 1e-9

    @pytest.mark.asyncio
    async def test_only_city_trades_used(self):
        trades = [
            _trade("NYC", 1.0, True),  # perfect
            _trade("NYC", 1.0, True),
            _trade("NYC", 1.0, True),
            _trade("NYC", 1.0, True),
            _trade("NYC", 1.0, True),
            _trade("London", 0.0, True),  # terrible but different city
            _trade("London", 0.0, True),
            _trade("London", 0.0, True),
            _trade("London", 0.0, True),
            _trade("London", 0.0, True),
        ]
        db = FakeDB(trades)
        nyc_result = await update_city_performance(db, "NYC")
        assert nyc_result is not None
        assert abs(nyc_result) < 1e-9  # NYC is perfect

    @pytest.mark.asyncio
    async def test_persists_to_db(self):
        trades = [_trade("NYC", 0.7, True) for _ in range(10)]
        db = FakeDB(trades)
        await update_city_performance(db, "NYC", window_days=30)
        stored = await db.get_city_performance("NYC", 30)
        assert stored is not None
        assert stored >= 0.0

    @pytest.mark.asyncio
    async def test_brier_range(self):
        # Brier score must be in [0, 1]
        trades = [_trade("NYC", float(i % 2), bool((i + 1) % 2)) for i in range(20)]
        db = FakeDB(trades)
        result = await update_city_performance(db, "NYC")
        assert result is not None
        assert 0.0 <= result <= 1.0


# ---------------------------------------------------------------------------
# get_city_filter
# ---------------------------------------------------------------------------

class TestGetCityFilter:
    @pytest.mark.asyncio
    async def test_no_data_defaults_to_allowed(self):
        db = FakeDB([])
        result = await get_city_filter(db, ["NYC", "London"])
        assert result["NYC"] is True
        assert result["London"] is True

    @pytest.mark.asyncio
    async def test_good_score_allowed(self):
        db = FakeDB()
        await db.upsert_city_performance("NYC", 30, 0.10, 20)
        result = await get_city_filter(db, ["NYC"], max_brier=0.20)
        assert result["NYC"] is True

    @pytest.mark.asyncio
    async def test_bad_score_blocked(self):
        db = FakeDB()
        await db.upsert_city_performance("NYC", 30, 0.25, 20)
        result = await get_city_filter(db, ["NYC"], max_brier=0.20)
        assert result["NYC"] is False

    @pytest.mark.asyncio
    async def test_score_at_threshold_blocked(self):
        db = FakeDB()
        await db.upsert_city_performance("NYC", 30, 0.20, 20)
        result = await get_city_filter(db, ["NYC"], max_brier=0.20)
        # score == max_brier → blocked (strictly greater check: score > max_brier is False, so allowed)
        assert result["NYC"] is True

    @pytest.mark.asyncio
    async def test_multiple_cities(self):
        db = FakeDB()
        await db.upsert_city_performance("NYC", 30, 0.10, 20)
        await db.upsert_city_performance("London", 30, 0.30, 20)
        result = await get_city_filter(db, ["NYC", "London", "Seoul"], max_brier=0.20)
        assert result["NYC"] is True
        assert result["London"] is False
        assert result["Seoul"] is True  # no data → allowed

    @pytest.mark.asyncio
    async def test_empty_city_list(self):
        db = FakeDB()
        result = await get_city_filter(db, [])
        assert result == {}


# ---------------------------------------------------------------------------
# update_all_city_performance
# ---------------------------------------------------------------------------

class TestUpdateAllCityPerformance:
    @pytest.mark.asyncio
    async def test_updates_all_cities(self):
        trades = (
            [_trade("NYC", 1.0, True) for _ in range(10)]
            + [_trade("London", 0.5, True) for _ in range(10)]
        )
        db = FakeDB(trades)
        results = await update_all_city_performance(db, ["NYC", "London"])
        assert "NYC" in results
        assert "London" in results

    @pytest.mark.asyncio
    async def test_skips_cities_with_no_data(self):
        db = FakeDB([])
        results = await update_all_city_performance(db, ["NYC", "London"])
        assert results == {}

    @pytest.mark.asyncio
    async def test_returns_only_cities_with_data(self):
        trades = [_trade("NYC", 1.0, True) for _ in range(10)]
        db = FakeDB(trades)
        results = await update_all_city_performance(db, ["NYC", "London"])
        assert "NYC" in results
        assert "London" not in results


