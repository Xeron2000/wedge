from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.market.polymarket import PolymarketClient


class TestPolymarketClientInit:
    def test_stores_credentials(self):
        c = PolymarketClient("pk", "ak", "as")
        assert c._private_key == "pk"
        assert c._api_key == "ak"
        assert c._api_secret == "as"
        assert c._client is None


class TestConnect:
    @pytest.mark.asyncio
    async def test_connect_success(self):
        mock_clob = MagicMock()
        with patch.dict("sys.modules", {"py_clob_client": MagicMock(), "py_clob_client.client": MagicMock()}):
            with patch("wedge.market.polymarket.asyncio.to_thread", new_callable=AsyncMock) as mock_thread:
                mock_thread.return_value = mock_clob
                c = PolymarketClient("pk", "ak", "as")
                await c.connect()
                assert c._client is mock_clob

    @pytest.mark.asyncio
    async def test_connect_import_error(self):
        c = PolymarketClient("pk", "ak", "as")

        async def fake_to_thread(fn, *args, **kwargs):
            # Run the inner function in a context where import fails
            import builtins
            real_import = builtins.__import__

            def fail_import(name, *a, **kw):
                if name == "py_clob_client.client":
                    raise ImportError("not installed")
                return real_import(name, *a, **kw)

            builtins.__import__ = fail_import
            try:
                return fn()
            finally:
                builtins.__import__ = real_import

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=fake_to_thread):
            await c.connect()
        assert c._client is None

    @pytest.mark.asyncio
    async def test_connect_generic_exception(self):
        c = PolymarketClient("pk", "ak", "as")

        async def fake_to_thread(fn, *args, **kwargs):
            mock_module = MagicMock()
            mock_module.ClobClient.side_effect = RuntimeError("boom")
            with patch.dict("sys.modules", {
                "py_clob_client": MagicMock(),
                "py_clob_client.client": mock_module,
            }):
                return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=fake_to_thread):
            await c.connect()
        assert c._client is None


class TestGetMarkets:
    @pytest.mark.asyncio
    async def test_no_client_returns_empty(self):
        c = PolymarketClient("pk", "ak", "as")
        result = await c.get_markets()
        assert result == []

    @pytest.mark.asyncio
    async def test_success(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.get_markets.return_value = [{"id": "m1"}]
        c._client = mock_client

        with patch("wedge.market.polymarket.asyncio.to_thread", new_callable=AsyncMock) as mock_thread:
            mock_thread.side_effect = lambda fn, *a, **kw: asyncio.coroutine(lambda: fn())()

            async def run_fn(fn, *a, **kw):
                return fn()

            mock_thread.side_effect = run_fn
            result = await c.get_markets()
        assert result == [{"id": "m1"}]

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.get_markets.side_effect = RuntimeError("network error")
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.get_markets()
        assert result == []


class TestGetOrderBook:
    @pytest.mark.asyncio
    async def test_no_client_returns_none(self):
        c = PolymarketClient("pk", "ak", "as")
        result = await c.get_order_book("tok_1")
        assert result is None

    @pytest.mark.asyncio
    async def test_success(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.get_order_book.return_value = {"bids": [], "asks": []}
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.get_order_book("tok_1")
        assert result == {"bids": [], "asks": []}

    @pytest.mark.asyncio
    async def test_exception_returns_none(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.get_order_book.side_effect = RuntimeError("error")
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.get_order_book("tok_1")
        assert result is None


class TestPlaceLimitOrder:
    @pytest.mark.asyncio
    async def test_no_client_returns_none(self):
        c = PolymarketClient("pk", "ak", "as")
        result = await c.place_limit_order("tok_1", "buy", 0.5, 10.0)
        assert result is None

    @pytest.mark.asyncio
    async def test_success(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.create_order.return_value = {"id": "order_123"}
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.place_limit_order("tok_1", "buy", 0.5, 10.0)
        assert result == {"id": "order_123"}
        mock_client.create_order.assert_called_once_with(token_id="tok_1", price=0.5, size=10.0, side="buy")

    @pytest.mark.asyncio
    async def test_exception_returns_none(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.create_order.side_effect = RuntimeError("rejected")
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.place_limit_order("tok_1", "buy", 0.5, 10.0)
        assert result is None


class TestCancelOrder:
    @pytest.mark.asyncio
    async def test_no_client_returns_false(self):
        c = PolymarketClient("pk", "ak", "as")
        result = await c.cancel_order("order_1")
        assert result is False

    @pytest.mark.asyncio
    async def test_success_returns_true(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.cancel.return_value = None
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.cancel_order("order_1")
        assert result is True
        mock_client.cancel.assert_called_once_with("order_1")

    @pytest.mark.asyncio
    async def test_exception_returns_false(self):
        c = PolymarketClient("pk", "ak", "as")
        mock_client = MagicMock()
        mock_client.cancel.side_effect = RuntimeError("not found")
        c._client = mock_client

        async def run_fn(fn, *a, **kw):
            return fn()

        with patch("wedge.market.polymarket.asyncio.to_thread", side_effect=run_fn):
            result = await c.cancel_order("order_1")
        assert result is False
