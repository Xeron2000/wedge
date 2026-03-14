from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class EdgeSignal(BaseModel):
    city: str
    date: date
    temp_f: int
    token_id: str
    p_model: float
    p_market: float
    edge: float  # p_model - p_market
    odds: float  # (1 - market_price) / market_price
