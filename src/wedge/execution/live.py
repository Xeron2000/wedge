from __future__ import annotations

import uuid
from datetime import UTC, datetime

from wedge.db import Database
from wedge.execution.executor import validate_order
from wedge.execution.models import OrderRequest, OrderResult
from wedge.log import get_logger
from wedge.market.models import MarketBucket, Position
from wedge.market.polymarket import PolymarketClient

log = get_logger("execution.live")


class LiveExecutor:
    def __init__(
        self,
        db: Database,
        client: PolymarketClient,
        initial_balance: float,
        max_bet: float = 100.0,
    ) -> None:
        self._db = db
        self._client = client
        self._balance = initial_balance
        self._max_bet = max_bet

    async def place_order(self, request: OrderRequest) -> OrderResult:
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
            order_id=None,  # filled after exchange confirms
            created_at=now,
        )
        if not inserted:
            log.info("live_duplicate_skipped", run_id=request.run_id, temp_f=request.temp_f)
            return OrderResult(success=True, error="duplicate")

        result = await self._client.place_limit_order(
            token_id=request.token_id,
            side=request.side,
            price=request.limit_price,
            size=request.size,
        )

        if not result:
            return OrderResult(success=False, error="polymarket_api_failed")

        order_id = result.get("id", f"live_{uuid.uuid4().hex[:12]}")
        self._balance -= request.size
        log.info("live_order_placed", order_id=order_id, city=request.city, temp_f=request.temp_f)
        return OrderResult(
            success=True,
            order_id=order_id,
            filled_price=request.limit_price,
            filled_size=request.size,
        )

    async def cancel_order(self, order_id: str) -> bool:
        return await self._client.cancel_order(order_id)

    async def get_positions(self) -> list[Position]:
        return []

    async def get_balance(self) -> float:
        return self._balance
