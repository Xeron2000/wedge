from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class ForecastDistribution(BaseModel):
    city: str
    date: date
    buckets: dict[int, float]  # temp_f → probability
    ensemble_spread: float  # standard deviation
    member_count: int
    updated_at: datetime
