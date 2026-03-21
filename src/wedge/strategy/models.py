from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class EdgeSignal(BaseModel):
    city: str
    date: date
    temp_value: int  # Temperature value from market (same unit as market)
    temp_unit: str  # "F" or "C" - same unit as Polymarket market
    token_id: str
    p_model: float
    p_market: float
    edge: float  # p_model - p_market
    odds: float  # (1 - market_price) / market_price
    ensemble_spread: float = 0.0  # Forecast uncertainty (°F std dev across ensemble members)
    forecast_age_hours: float = 0.0  # Hours since GFS model update
    weight: float = 1.0  # Signal weight (adjusted by forecast freshness)
    side: str = "buy"  # "buy" = long Yes, "sell" = short Yes (buy No)
