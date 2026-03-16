from __future__ import annotations

from datetime import date

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


@pytest.fixture
async def executor(db):
    await db.insert_run("test_run_1", "2026-07-01T00:00:00")
    return DryRunExecutor(db, initial_balance=1000, max_bet=100)


def _order(run_id="test_run_1", temp_value=78, size=10.0) -> OrderRequest:
    return OrderRequest(
        run_id=run_id,
        token_id="tok_78",
        city="NYC",
        date=date(2026, 7, 1),
        temp_value=temp_value,
        temp_unit="F",
        strategy="ladder",
        limit_price=0.20,
        size=size,
    )


class TestDryRunExecutor:
    @pytest.mark.asyncio
    async def test_place_order_success(self, executor):
        result = await executor.place_order(_order())
        assert result.success
        assert result.order_id is not None
        assert result.filled_price == 0.20
        balance = await executor.get_balance()
        assert balance == 990  # 1000 - 10

    @pytest.mark.asyncio
    async def test_insufficient_balance(self, executor):
        result = await executor.place_order(_order(size=1100))
        assert not result.success
        assert "insufficient" in result.error

    @pytest.mark.asyncio
    async def test_exceeds_max_bet(self, executor):
        result = await executor.place_order(_order(size=150))
        assert not result.success
        assert "max bet" in result.error.lower()

    @pytest.mark.asyncio
    async def test_idempotent_duplicate(self, executor):
        result1 = await executor.place_order(_order())
        result2 = await executor.place_order(_order())
        assert result1.success
        assert result2.success
        assert result2.error == "duplicate"
        # Balance should only decrease once
        balance = await executor.get_balance()
        assert balance == 990

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, executor):
        result = await executor.cancel_order("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_cancel_existing_order(self, executor):
        order_result = await executor.place_order(_order())
        assert order_result.success
        cancel_result = await executor.cancel_order(order_result.order_id)
        assert cancel_result is True

    @pytest.mark.asyncio
    async def test_get_positions(self, executor):
        await executor.place_order(_order())
        positions = await executor.get_positions()
        assert len(positions) == 1
        assert positions[0].strategy == "ladder"


class TestDatabaseSettlement:
    @pytest.mark.asyncio
    async def test_settle_trades(self, db):
        await db.insert_run("run1", "2026-07-01T00:00:00")
        await db.insert_trade(
            run_id="run1", city="NYC", date="2026-07-01",
            temp_f=78, strategy="ladder", entry_price=0.20,
            size=10.0, p_model=0.25, p_market=0.20, edge=0.05,
            created_at="2026-07-01T00:00:00",
        )
        count = await db.settle_trades("NYC", "2026-07-01", actual_temp=78)
        assert count == 1

    @pytest.mark.asyncio
    async def test_settle_losing_trade(self, db):
        await db.insert_run("run2", "2026-07-01T00:00:00")
        await db.insert_trade(
            run_id="run2", city="NYC", date="2026-07-01",
            temp_f=78, strategy="ladder", entry_price=0.20,
            size=10.0, p_model=0.25, p_market=0.20, edge=0.05,
            created_at="2026-07-01T00:00:00",
        )
        count = await db.settle_trades("NYC", "2026-07-01", actual_temp=80)
        assert count == 1
