from __future__ import annotations

import asyncio
from typing import Any

import httpx

from wedge.log import get_logger

log = get_logger("market.polymarket")


class PublicPolymarketClient:
    """Public Polymarket client for market data (no authentication required)."""

    def __init__(self) -> None:
        self._base_url = "https://gamma-api.polymarket.com"

    async def get_markets(self) -> list[dict]:
        """Fetch all markets from public Gamma API."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(f"{self._base_url}/markets")
                response.raise_for_status()
                return response.json()
        except Exception as e:
            log.error("polymarket_get_markets_failed", error=str(e))
            return []


class PolymarketClient:
    """Async wrapper around py-clob-client (synchronous library)."""

    def __init__(self, private_key: str, api_key: str, api_secret: str) -> None:
        self._private_key = private_key
        self._api_key = api_key
        self._api_secret = api_secret
        self._client: Any = None

    async def connect(self) -> None:
        def _init() -> Any:
            try:
                from py_clob_client.client import ClobClient

                return ClobClient(
                    host="https://clob.polymarket.com",
                    key=self._private_key,
                    chain_id=137,
                )
            except ImportError:
                log.warning("py_clob_client_not_installed")
                return None
            except Exception as e:
                log.error("polymarket_init_failed", error=str(e))
                return None

        self._client = await asyncio.to_thread(_init)

    async def get_markets(self) -> list[dict]:
        if not self._client:
            return []

        def _fetch() -> list[dict]:
            try:
                return self._client.get_markets()
            except Exception as e:
                log.error("polymarket_get_markets_failed", error=str(e))
                return []

        return await asyncio.to_thread(_fetch)

    async def get_order_book(self, token_id: str) -> dict | None:
        if not self._client:
            return None

        def _fetch() -> dict | None:
            try:
                return self._client.get_order_book(token_id)
            except Exception as e:
                log.error("polymarket_orderbook_failed", token_id=token_id, error=str(e))
                return None

        return await asyncio.to_thread(_fetch)

    async def place_limit_order(
        self, token_id: str, side: str, price: float, size: float
    ) -> dict | None:
        if not self._client:
            return None

        def _place() -> dict | None:
            try:
                return self._client.create_order(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=side,
                )
            except Exception as e:
                log.error("polymarket_order_failed", token_id=token_id, error=str(e))
                return None

        return await asyncio.to_thread(_place)

    async def cancel_order(self, order_id: str) -> bool:
        if not self._client:
            return False

        def _cancel() -> bool:
            try:
                self._client.cancel(order_id)
                return True
            except Exception:
                return False

        return await asyncio.to_thread(_cancel)
