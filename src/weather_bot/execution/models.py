from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel


class OrderRequest(BaseModel):
    run_id: str
    token_id: str
    city: str
    date: date
    temp_f: int
    strategy: Literal["ladder", "tail"]
    side: Literal["buy"] = "buy"
    limit_price: float
    size: float  # USD amount
    p_model: float = 0.0
    p_market: float = 0.0
    edge: float = 0.0


class OrderResult(BaseModel):
    success: bool
    order_id: str | None = None
    filled_price: float | None = None
    filled_size: float | None = None
    error: str | None = None
