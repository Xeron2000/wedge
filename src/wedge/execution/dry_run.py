from __future__ import annotations

import uuid
from datetime import UTC, datetime

from wedge.db import Database
from wedge.execution.executor import validate_order
from wedge.execution.models import OrderRequest, OrderResult
from wedge.log import get_logger
from wedge.market.models import MarketBucket, Position

log = get_logger("execution.dry_run")


class DryRunExecutor:
    def __init__(self, db: Database, initial_balance: float, max_bet: float = 100.0) -> None:
        self._db = db
        self._balance = initial_balance
        self._max_bet = max_bet
        self._positions: list[Position] = []
        self._order_ids: set[str] = set()

    async def place_order(self, request: OrderRequest) -> OrderResult:
        error = validate_order(request, self._balance, self._max_bet)
        if error:
            log.warning("dry_run_order_rejected", reason=error, **request.model_dump(mode="json"))
            return OrderResult(success=False, error=error)

        order_id = f"dry_{uuid.uuid4().hex[:12]}"
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
            order_id=order_id,
            created_at=now,
        )

        if not inserted:
            log.info("dry_run_duplicate_skipped", run_id=request.run_id, temp_f=request.temp_f)
            return OrderResult(success=True, order_id=order_id, error="duplicate")

        self._balance -= request.size
        self._order_ids.add(order_id)

        self._positions.append(
            Position(
                bucket=MarketBucket(
                    token_id=request.token_id,
                    city=request.city,
                    date=request.date,
                    temp_f=request.temp_f,
                    market_price=request.limit_price,
                    implied_prob=request.limit_price,
                ),
                size=request.size,
                entry_price=request.limit_price,
                strategy=request.strategy,
            )
        )

        log.info(
            "dry_run_order_placed",
            order_id=order_id,
            city=request.city,
            temp_f=request.temp_f,
            size=f"${request.size:.2f}",
            price=request.limit_price,
        )
        return OrderResult(
            success=True,
            order_id=order_id,
            filled_price=request.limit_price,
            filled_size=request.size,
        )

    async def cancel_order(self, order_id: str) -> bool:
        found = order_id in self._order_ids
        log.info("dry_run_cancel", order_id=order_id, found=found)
        return found

    async def get_positions(self) -> list[Position]:
        return list(self._positions)

    async def get_balance(self) -> float:
        return self._balance
