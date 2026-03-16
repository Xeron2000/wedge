from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from wedge.db import Database
from wedge.execution.dry_run import DryRunExecutor
from wedge.execution.models import OrderRequest


@pytest.fixture
async def db(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.connect()
    yield db
    await db.close()


def _order(run_id="run1", temp_f=78, size=10.0) -> OrderRequest:
    return OrderRequest(
        run_id=run_id,
        token_id=f"tok_{temp_f}",
        city="NYC",
        date=date(2026, 7, 1),
        temp_value=temp_f,
        temp_unit="F",
        strategy="ladder",
        limit_price=0.20,
        size=size,
    )


class TestGetLastBalance:
    @pytest.mark.asyncio
    async def test_no_snapshots_returns_default(self, db):
        balance = await db.get_last_balance(default=1000.0)
        assert balance == 1000.0

    @pytest.mark.asyncio
    async def test_returns_most_recent_snapshot(self, db):
        await db.insert_bankroll_snapshot(950.0, 0, "2026-07-01T00:00:00")
        await db.insert_bankroll_snapshot(920.0, 0, "2026-07-01T06:00:00")
        await db.insert_bankroll_snapshot(935.0, 0, "2026-07-01T12:00:00")
        balance = await db.get_last_balance(default=1000.0)
        assert balance == 935.0

    @pytest.mark.asyncio
    async def test_single_snapshot(self, db):
        await db.insert_bankroll_snapshot(777.0, 0, "2026-07-01T00:00:00")
        balance = await db.get_last_balance(default=1000.0)
        assert balance == 777.0

    @pytest.mark.asyncio
    async def test_custom_default(self, db):
        balance = await db.get_last_balance(default=500.0)
        assert balance == 500.0


class TestBalancePersistsAcrossExecutors:
    """Simulates multiple pipeline cycles to verify balance continuity."""

    @pytest.mark.asyncio
    async def test_balance_carries_over(self, db):
        await db.insert_run("run1", "2026-07-01T00:00:00")

        # Cycle 1: start at 1000, place an order
        balance1 = await db.get_last_balance(default=1000.0)
        assert balance1 == 1000.0
        executor1 = DryRunExecutor(db, balance1, max_bet=100)
        result = await executor1.place_order(_order(run_id="run1", temp_f=78, size=50.0))
        assert result.success
        await db.insert_bankroll_snapshot(await executor1.get_balance(), 0, "2026-07-01T04:30:00")

        # Cycle 2: should resume from 950, not 1000
        await db.insert_run("run2", "2026-07-01T10:30:00")
        balance2 = await db.get_last_balance(default=1000.0)
        assert balance2 == 950.0
        executor2 = DryRunExecutor(db, balance2, max_bet=100)
        result = await executor2.place_order(_order(run_id="run2", temp_f=79, size=30.0))
        assert result.success
        await db.insert_bankroll_snapshot(await executor2.get_balance(), 0, "2026-07-01T10:35:00")

        # Cycle 3: should resume from 920
        balance3 = await db.get_last_balance(default=1000.0)
        assert balance3 == 920.0

    @pytest.mark.asyncio
    async def test_zero_orders_keeps_balance(self, db):
        await db.insert_bankroll_snapshot(850.0, 0, "2026-07-01T00:00:00")

        # Cycle with no orders still snapshots current balance
        balance = await db.get_last_balance(default=1000.0)
        assert balance == 850.0
        executor = DryRunExecutor(db, balance, max_bet=100)
        # No orders placed
        await db.insert_bankroll_snapshot(await executor.get_balance(), 0, "2026-07-01T06:00:00")

        balance_after = await db.get_last_balance(default=1000.0)
        assert balance_after == 850.0
