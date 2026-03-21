from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from wedge.db import Database
from wedge.execution.executor import validate_order
from wedge.execution.models import OrderRequest, OrderResult
from wedge.log import get_logger
from wedge.market.models import Position
from wedge.market.polymarket import PolymarketClient

log = get_logger("execution.live")

# Order execution constants
MAKER_TIMEOUT_SECONDS = 30  # Wait 30s for limit order to fill


class LiveExecutor:
    """Live execution with limit-only strategy.

    Strategy:
    1. Place limit order at model's fair price
    2. Wait for timeout
    3. If not filled, cancel — skip this trade entirely
    4. No taker/market orders to avoid slippage in illiquid markets
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

        # Thread-safety for concurrent order execution
        self._balance_lock = asyncio.Lock()

        # Track pending orders
        self._pending_orders: dict[str, OrderRequest] = {}

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place limit order only. Skips trade if not filled within timeout.

        Thread-safe: uses asyncio.Lock to protect balance checks and updates.
        """
        # Validate order with lock to prevent race conditions
        async with self._balance_lock:
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
                temp_f=request.temp_value,
                temp_unit=request.temp_unit,
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
                log.info(
                    "live_duplicate_skipped",
                    run_id=request.run_id,
                    temp_value=request.temp_value,
                )
                return OrderResult(success=True, error="duplicate")

            # Deduct balance immediately to prevent double-spending
            self._balance -= request.size

        # Place limit order and wait for fill
        result = await self._try_limit_order(request)

        if result and result.success:
            log.info(
                "live_limit_order_filled",
                order_id=result.order_id,
                city=request.city,
                temp_value=request.temp_value,
                filled_price=result.filled_price,
            )
            await self._persist_balance_snapshot()
            return result

        # Not filled — cancel, refund, skip this trade
        log.info(
            "live_limit_order_skipped",
            run_id=request.run_id,
            city=request.city,
            temp_value=request.temp_value,
            reason="limit order did not fill within timeout, skipping to avoid slippage",
        )
        async with self._balance_lock:
            self._balance += request.size
        await self._db.delete_trade(
            run_id=request.run_id,
            city=request.city,
            date=request.date.isoformat(),
            temp_f=request.temp_value,
            strategy=request.strategy,
        )
        await self._persist_balance_snapshot()
        return result or OrderResult(success=False, error="limit_not_filled")

    async def _persist_balance_snapshot(self) -> None:
        """Persist current balance so a crash mid-run doesn't lose accounting state."""
        async with self._balance_lock:
            balance = self._balance
        await self._db.insert_bankroll_snapshot(balance, 0.0, datetime.now(UTC).isoformat())

    async def _try_limit_order(self, request: OrderRequest) -> OrderResult | None:
        """Place limit order at model's fair price. Cancel if not filled within timeout."""
        limit_price = request.limit_price

        try:
            result = await self._client.place_limit_order(
                token_id=request.token_id,
                side=request.side,
                price=limit_price,
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
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    filled_price=limit_price,
                    filled_size=request.size,
                )

            # Timeout - cancel and return None
            await self._client.cancel_order(order_id)
            log.info("live_limit_order_cancelled", order_id=order_id)
            return None

        except Exception as e:
            log.error("live_limit_order_error", error=str(e))
            return None

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
