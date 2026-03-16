"""Test position deduplication across pipeline runs."""
from __future__ import annotations

import pytest
from datetime import date, datetime, UTC

from wedge.db import Database
from wedge.execution.dry_run import DryRunExecutor
from wedge.execution.models import OrderRequest


@pytest.fixture
async def db():
    """Create in-memory test database."""
    db = Database(":memory:")
    await db.connect()
    yield db
    await db.close()


@pytest.mark.asyncio
async def test_no_duplicate_positions_across_runs(db):
    """Test that we don't create duplicate positions for the same market."""
    executor = DryRunExecutor(db, initial_balance=1000.0, max_bet=100.0)

    # First run: place order
    await db.insert_run("run1", datetime.now(UTC).isoformat())

    request = OrderRequest(
        run_id="run1",
        token_id="test_token",
        city="NYC",
        date=date(2026, 3, 20),
        temp_f=70,
        strategy="ladder",
        limit_price=0.40,
        size=100.0,
        p_model=0.50,
        p_market=0.40,
        edge=0.10,
    )

    result = await executor.place_order(request)
    assert result.success

    # Check that position exists
    has_position = await db.has_open_position("NYC", "2026-03-20", 70)
    assert has_position is True

    # Second run: try to place same order
    await db.insert_run("run2", datetime.now(UTC).isoformat())

    # Should detect existing position
    has_position = await db.has_open_position("NYC", "2026-03-20", 70)
    assert has_position is True

    # Get all positions - should only have 1
    positions = await db.get_open_positions()
    assert len(positions) == 1
    assert positions[0]["city"] == "NYC"
    assert positions[0]["temp_f"] == 70


@pytest.mark.asyncio
async def test_different_markets_allowed(db):
    """Test that different markets can have positions."""
    executor = DryRunExecutor(db, initial_balance=1000.0, max_bet=100.0)

    await db.insert_run("run1", datetime.now(UTC).isoformat())

    # Place order for NYC 70°F
    request1 = OrderRequest(
        run_id="run1",
        token_id="test_token_1",
        city="NYC",
        date=date(2026, 3, 20),
        temp_f=70,
        strategy="ladder",
        limit_price=0.40,
        size=50.0,
        p_model=0.50,
        p_market=0.40,
        edge=0.10,
    )
    result1 = await executor.place_order(request1)
    assert result1.success

    # Place order for NYC 72°F (different temp)
    request2 = OrderRequest(
        run_id="run1",
        token_id="test_token_2",
        city="NYC",
        date=date(2026, 3, 20),
        temp_f=72,
        strategy="ladder",
        limit_price=0.30,
        size=50.0,
        p_model=0.40,
        p_market=0.30,
        edge=0.10,
    )
    result2 = await executor.place_order(request2)
    assert result2.success

    # Should have 2 positions
    positions = await db.get_open_positions()
    assert len(positions) == 2


@pytest.mark.asyncio
async def test_settled_positions_dont_block_new_orders(db):
    """Test that settled positions don't prevent new orders."""
    executor = DryRunExecutor(db, initial_balance=1000.0, max_bet=100.0)

    await db.insert_run("run1", datetime.now(UTC).isoformat())

    # Place and settle order
    request = OrderRequest(
        run_id="run1",
        token_id="test_token",
        city="NYC",
        date=date(2026, 3, 20),
        temp_f=70,
        strategy="ladder",
        limit_price=0.40,
        size=100.0,
        p_model=0.50,
        p_market=0.40,
        edge=0.10,
    )
    result = await executor.place_order(request)
    assert result.success

    # Settle the trade
    await db.settle_trades("NYC", "2026-03-20", actual_temp=70)

    # Should not have open position anymore
    has_position = await db.has_open_position("NYC", "2026-03-20", 70)
    assert has_position is False

    # Should be able to place new order for same market
    await db.insert_run("run2", datetime.now(UTC).isoformat())
    executor2 = DryRunExecutor(db, initial_balance=1000.0, max_bet=100.0)

    request2 = OrderRequest(
        run_id="run2",
        token_id="test_token",
        city="NYC",
        date=date(2026, 3, 20),
        temp_f=70,
        strategy="ladder",
        limit_price=0.40,
        size=100.0,
        p_model=0.50,
        p_market=0.40,
        edge=0.10,
    )
    result2 = await executor2.place_order(request2)
    assert result2.success
