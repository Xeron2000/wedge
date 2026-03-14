from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel


class MarketBucket(BaseModel):
    token_id: str
    city: str
    date: date
    temp_f: int
    market_price: float  # 0-1
    implied_prob: float  # = market_price


class Position(BaseModel):
    bucket: MarketBucket
    side: Literal["buy"] = "buy"
    size: float  # USD amount
    entry_price: float
    strategy: Literal["ladder", "tail"]
    p_model: float = 0.0
    edge: float = 0.0
