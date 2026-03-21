from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from wedge.db import Database
from wedge.execution.live import LiveExecutor
from wedge.execution.models import OrderRequest


@pytest.fixture
async def db(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.connect()
    await db.insert_run("run1", "2026-07-01T00:00:00")
    yield db
    await db.close()


@pytest.fixture
def mock_client():
    client = AsyncMock()
    client.cancel_order.return_value = True
    return client


@pytest.fixture
def executor(db, mock_client):
    return LiveExecutor(db=db, client=mock_client, initial_balance=1000.0, max_bet=100.0, maker_timeout=2)


def _order(
    run_id: str = "run1",
    temp_f: int = 78,
    size: float = 10.0,
    limit_price: float = 0.20,
) -> OrderRequest:
    return OrderRequest(
        run_id=run_id,
        token_id=f"tok_{temp_f}",
        city="NYC",
        date=date(2026, 7, 1),
        temp_value=temp_f,
        temp_unit="F",
        strategy="ladder",
        limit_price=limit_price,
        size=size,
    )


class TestPlaceOrder:
    @pytest.mark.asyncio
    async def test_validation_failure_returns_error(self, executor):
        # size > balance → validation error
        result = await executor.place_order(_order(size=2000.0))
        assert not result.success
        assert result.error is not None
        assert "insufficient" in result.error

    @pytest.mark.asyncio
    async def test_validation_failure_size_zero(self, executor):
        result = await executor.place_order(_order(size=0.0))
        assert not result.success
        assert "size must be positive" in result.error

    @pytest.mark.asyncio
    async def test_validation_failure_exceeds_max_bet(self, executor):
        result = await executor.place_order(_order(size=150.0))
        assert not result.success
        assert "max bet" in result.error.lower()

    @pytest.mark.asyncio
    async def test_duplicate_returns_success_with_duplicate_error(self, db, mock_client, executor):
        mock_client.place_limit_order.return_value = {"id": "order_abc"}
        mock_client.get_order_status = AsyncMock(return_value={"state": "filled"})
        # First order succeeds and inserts into DB
        r1 = await executor.place_order(_order())
        assert r1.success

        # Second order with same run_id + temp_f → insert_trade returns False → duplicate
        r2 = await executor.place_order(_order())
        assert r2.success
        assert r2.error == "duplicate"

    @pytest.mark.asyncio
    async def test_limit_order_failure_skips_trade(self, mock_client, executor):
        # If place_limit_order returns None, trade is skipped
        mock_client.place_limit_order.return_value = None
        result = await executor.place_order(_order(temp_f=90))
        assert not result.success
        assert result.error == "limit_not_filled"
        # Balance should be refunded
        balance = await executor.get_balance()
        assert balance == 1000.0

    @pytest.mark.asyncio
    async def test_limit_order_timeout_skips_trade(self, mock_client, executor):
        # Limit order placed but never fills → timeout → skip
        mock_client.place_limit_order.return_value = {"id": "order_timeout"}
        mock_client.get_order_status = AsyncMock(return_value={"state": "open"})
        result = await executor.place_order(_order(temp_f=91))
        assert not result.success
        assert result.error == "limit_not_filled"
        mock_client.cancel_order.assert_called_once_with("order_timeout")
        balance = await executor.get_balance()
        assert balance == 1000.0

    @pytest.mark.asyncio
    async def test_success_with_id_in_result(self, mock_client, executor):
        # Limit order fills within timeout
        mock_client.place_limit_order.return_value = {"id": "order_xyz"}
        mock_client.get_order_status = AsyncMock(return_value={"state": "filled"})

        result = await executor.place_order(_order(temp_f=80))
        assert result.success
        assert result.order_id == "order_xyz"
        assert result.filled_price is not None
        assert result.filled_size == 10.0
        balance = await executor.get_balance()
        assert balance == 990.0

    @pytest.mark.asyncio
    async def test_no_id_skips_trade(self, mock_client, executor):
        # place_limit_order returns result without "id" → skip
        mock_client.place_limit_order.return_value = {"status": "open"}
        result = await executor.place_order(_order(temp_f=82))
        assert not result.success
        assert result.error == "limit_not_filled"
        balance = await executor.get_balance()
        assert balance == 1000.0


class TestCancelOrder:
    @pytest.mark.asyncio
    async def test_delegates_to_client(self, mock_client, executor):
        mock_client.cancel_order.return_value = True
        result = await executor.cancel_order("order_123")
        assert result is True
        mock_client.cancel_order.assert_called_once_with("order_123")

    @pytest.mark.asyncio
    async def test_returns_false_when_client_fails(self, mock_client, executor):
        mock_client.cancel_order.return_value = False
        result = await executor.cancel_order("bad_id")
        assert result is False


class TestGetPositions:
    @pytest.mark.asyncio
    async def test_returns_empty_list(self, mock_client, executor):
        mock_client.get_positions.return_value = []
        positions = await executor.get_positions()
        assert positions == []


class TestGetBalance:
    @pytest.mark.asyncio
    async def test_returns_initial_balance(self, executor):
        balance = await executor.get_balance()
        assert balance == 1000.0

    @pytest.mark.asyncio
    async def test_balance_decreases_after_order(self, mock_client, executor):
        mock_client.place_limit_order.return_value = {"id": "o1"}
        mock_client.get_order_status = AsyncMock(return_value={"state": "filled"})
        await executor.place_order(_order(temp_f=85, size=25.0))
        balance = await executor.get_balance()
        assert balance == 975.0
