from __future__ import annotations

from typing import Protocol

from weather_bot.execution.models import OrderRequest, OrderResult
from weather_bot.market.models import Position


class Executor(Protocol):
    async def place_order(self, request: OrderRequest) -> OrderResult: ...
    async def cancel_order(self, order_id: str) -> bool: ...
    async def get_positions(self) -> list[Position]: ...
    async def get_balance(self) -> float: ...


def validate_order(request: OrderRequest, balance: float, max_bet: float) -> str | None:
    """Shared validation. Returns error message or None if valid."""
    if request.size <= 0:
        return "size must be positive"
    if request.limit_price <= 0 or request.limit_price >= 1:
        return "limit_price must be in (0, 1)"
    if request.size > balance:
        return f"insufficient balance: {balance:.2f} < {request.size:.2f}"
    if request.size > max_bet:
        return f"exceeds max bet: {request.size:.2f} > {max_bet:.2f}"
    return None
