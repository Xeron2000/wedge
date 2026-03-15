from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from wedge.config import Settings
from wedge.db import Database
from wedge.monitoring.metrics import show_stats


@pytest.fixture
def settings(tmp_path):
    return Settings(mode="dry_run", bankroll=1000.0, db_path=str(tmp_path / "test.db"))


@pytest.fixture
async def db(settings):
    d = Database(settings.db_path)
    await d.connect()
    yield d
    await d.close()


class TestShowStats:
    @pytest.mark.asyncio
    async def test_empty_db_no_trades(self, settings, db):
        # brier=None, no trades, no best_trade
        mock_pnl = {
            "total_trades": 0,
            "wins": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "best_trade": None,
            "worst_trade": None,
        }
        with (
            patch.object(db, "get_brier_score", return_value=None) as mock_brier,
            patch.object(db, "get_pnl_summary", return_value=mock_pnl),
            patch("wedge.monitoring.metrics.Database", return_value=db),
        ):
            await show_stats(settings, days=30)
            mock_brier.assert_called_once_with(30)

    @pytest.mark.asyncio
    async def test_with_brier_good(self, settings, db):
        mock_pnl = {
            "total_trades": 10,
            "wins": 7,
            "win_rate": 0.7,
            "total_pnl": 55.0,
            "best_trade": None,
            "worst_trade": None,
        }
        with (
            patch.object(db, "get_brier_score", return_value=0.15),
            patch.object(db, "get_pnl_summary", return_value=mock_pnl),
            patch("wedge.monitoring.metrics.Database", return_value=db),
        ):
            await show_stats(settings, days=30)

    @pytest.mark.asyncio
    async def test_with_brier_ok(self, settings, db):
        mock_pnl = {
            "total_trades": 10,
            "wins": 5,
            "win_rate": 0.5,
            "total_pnl": 10.0,
            "best_trade": None,
            "worst_trade": None,
        }
        with (
            patch.object(db, "get_brier_score", return_value=0.22),
            patch.object(db, "get_pnl_summary", return_value=mock_pnl),
            patch("wedge.monitoring.metrics.Database", return_value=db),
        ):
            await show_stats(settings, days=30)

    @pytest.mark.asyncio
    async def test_with_brier_paused(self, settings, db):
        mock_pnl = {
            "total_trades": 10,
            "wins": 3,
            "win_rate": 0.3,
            "total_pnl": -15.0,
            "best_trade": None,
            "worst_trade": None,
        }
        with (
            patch.object(db, "get_brier_score", return_value=0.30),
            patch.object(db, "get_pnl_summary", return_value=mock_pnl),
            patch("wedge.monitoring.metrics.Database", return_value=db),
        ):
            await show_stats(settings, days=30)

    @pytest.mark.asyncio
    async def test_with_best_worst_trade(self, settings, db):
        mock_pnl = {
            "total_trades": 5,
            "wins": 3,
            "win_rate": 0.6,
            "total_pnl": 30.0,
            "best_trade": 25.0,
            "worst_trade": -8.0,
        }
        with (
            patch.object(db, "get_brier_score", return_value=None),
            patch.object(db, "get_pnl_summary", return_value=mock_pnl),
            patch("wedge.monitoring.metrics.Database", return_value=db),
        ):
            await show_stats(settings, days=7)
