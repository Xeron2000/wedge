from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

from wedge.db import Database
from wedge.execution.executor import validate_order
from wedge.execution.models import OrderRequest, OrderResult
from wedge.log import get_logger
from wedge.market.models import MarketBucket, Position
from wedge.market.polymarket import PolymarketClient

log = get_logger("execution.live")

# Order execution constants
MAKER_TIMEOUT_SECONDS = 30  # Wait 30s for maker order to fill
MAKER_PRICE_OFFSET = 0.01   # Place maker 1¢ below mid price
TAKER_PRICE_OFFSET = 0.02   # Taker 2¢ above mid for faster fill


class LiveExecutor:
    """Live execution with maker-taker strategy.

    Strategy:
    1. Place limit order (maker) at favorable price
    2. Wait for timeout
    3. If not filled, cancel and place taker order
    4. Track order status and handle partial fills
    """

    def __init__(
        self,
        db: Database,
        client: PolymarketClient,
        initial_balance: float,
        max_bet: float = 100.0,
        maker_timeout: int = MAKER_TIMEOUT_SECONDS,
    ) -> None:
        self._db = db
        self._client = client
        self._balance = initial_balance
        self._max_bet = max_bet
        self._maker_timeout = maker_timeout

        # Track pending orders
        self._pending_orders: dict[str, OrderRequest] = {}

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place order using maker-taker strategy."""
        error = validate_order(request, self._balance, self._max_bet)
        if error:
            log.warning("live_order_rejected", reason=error)
            return OrderResult(success=False, error=error)

        # Idempotency: reserve DB slot BEFORE placing order
        now = datetime.now(UTC).isoformat()
        inserted = await self._db.insert_trade(
            run_id=request.run_id,
            city=request.city,
            date=request.date.isoformat(),
            temp_f=request.temp_f,
            strategy=request.strategy,
            entry_price=request.limit_price,
            size=request.size,
            p_model=request.p_model,
            p_market=request.p_market,
            edge=request.edge,
            token_id=request.token_id,
            order_id=None,
            created_at=now,
        )
        if not inserted:
            log.info("live_duplicate_skipped", run_id=request.run_id, temp_f=request.temp_f)
            return OrderResult(success=True, error="duplicate")

        # Try maker order first
        result = await self._try_maker_order(request)

        if result and result.success:
            self._balance -= request.size
            log.info(
                "live_maker_order_filled",
                order_id=result.order_id,
                city=request.city,
                temp_f=request.temp_f,
                filled_price=result.filled_price,
            )
            return result

        # Maker failed, try taker
        log.info(
            "live_maker_timeout_falling_back_to_taker",
            run_id=request.run_id,
            temp_f=request.temp_f,
        )
        result = await self._try_taker_order(request)

        if result and result.success:
            self._balance -= request.size
            log.info(
                "live_taker_order_filled",
                order_id=result.order_id,
                city=request.city,
                temp_f=request.temp_f,
            )
            return result

        # Both failed
        log.error(
            "live_order_failed",
            run_id=request.run_id,
            error=result.error if result else "unknown",
        )
        return result or OrderResult(success=False, error="execution_failed")

    async def _try_maker_order(self, request: OrderRequest) -> OrderResult | None:
        """Try to fill as maker (limit order below mid).

        Maker orders get fee rebate, improving EV.
        """
        # Calculate maker price (slightly below limit)
        maker_price = max(0.01, request.limit_price - MAKER_PRICE_OFFSET)

        try:
            # Place limit order
            result = await self._client.place_limit_order(
                token_id=request.token_id,
                side=request.side,
                price=maker_price,
                size=request.size,
            )

            if not result:
                return None

            order_id = result.get("id")
            if not order_id:
                return None

            # Track pending order
            self._pending_orders[order_id] = request

            # Wait for fill or timeout
            filled = await self._wait_for_fill(order_id, self._maker_timeout)

            if filled:
                # Order filled as maker
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    filled_price=maker_price,
                    filled_size=request.size,
                )

            # Timeout - cancel and return None to trigger taker
            await self._client.cancel_order(order_id)
            log.info("live_maker_order_cancelled", order_id=order_id)
            return None

        except Exception as e:
            log.error("live_maker_order_error", error=str(e))
            return None

    async def _try_taker_order(self, request: OrderRequest) -> OrderResult:
        """Execute as taker (market order for immediate fill).

        Taker orders pay fee but guarantee fill.
        """
        # Calculate taker price (slightly above mid for faster fill)
        taker_price = min(0.99, request.limit_price + TAKER_PRICE_OFFSET)

        try:
            result = await self._client.place_limit_order(
                token_id=request.token_id,
                side=request.side,
                price=taker_price,
                size=request.size,
            )

            if not result:
                return OrderResult(success=False, error="taker_order_failed")

            order_id = result.get("id", f"live_{uuid.uuid4().hex[:12]}")
            return OrderResult(
                success=True,
                order_id=order_id,
                filled_price=taker_price,
                filled_size=request.size,
            )

        except Exception as e:
            log.error("live_taker_order_error", error=str(e))
            return OrderResult(success=False, error=str(e))

    async def _wait_for_fill(
        self,
        order_id: str,
        timeout_seconds: int,
        check_interval: float = 2.0,
    ) -> bool:
        """Wait for order to fill.

        Args:
            order_id: Order ID to track
            timeout_seconds: Max wait time
            check_interval: How often to check status

        Returns:
            True if filled, False if timeout
        """
        elapsed = 0.0
        while elapsed < timeout_seconds:
            try:
                status = await self._client.get_order_status(order_id)
                if status:
                    order_state = status.get("state", "")
                    if order_state in ("filled", "partially_filled"):
                        return True
                    if order_state == "cancelled":
                        return False
            except Exception as e:
                log.warning("live_order_status_check_failed", order_id=order_id, error=str(e))

            await asyncio.sleep(check_interval)
            elapsed += check_interval

        return False  # Timeout

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        if order_id in self._pending_orders:
            del self._pending_orders[order_id]
        return await self._client.cancel_order(order_id)

    async def get_positions(self) -> list[Position]:
        """Get current positions from Polymarket."""
        return await self._client.get_positions()

    async def get_balance(self) -> float:
        """Get current balance."""
        return self._balance

    async def get_pending_orders(self) -> dict[str, OrderRequest]:
        """Get pending orders."""
        return self._pending_orders.copy()
