from __future__ import annotations

from wedge.market.models import MarketBucket
from wedge.strategy.models import EdgeSignal
from wedge.weather.models import ForecastDistribution

_EPS = 1e-6


def detect_edges(
    forecast: ForecastDistribution,
    markets: list[MarketBucket],
    ladder_threshold: float = 0.05,
    tail_threshold: float = 0.08,
) -> list[EdgeSignal]:
    """Find buckets where model probability exceeds market pricing."""
    signals: list[EdgeSignal] = []

    for bucket in markets:
        if not (_EPS < bucket.market_price < 1 - _EPS):
            continue

        p_model = forecast.buckets.get(bucket.temp_f, 0.0)
        edge = p_model - bucket.market_price

        min_threshold = min(ladder_threshold, tail_threshold)
        if edge <= min_threshold:
            continue

        odds = (1.0 - bucket.market_price) / bucket.market_price

        signals.append(
            EdgeSignal(
                city=bucket.city,
                date=bucket.date,
                temp_f=bucket.temp_f,
                token_id=bucket.token_id,
                p_model=p_model,
                p_market=bucket.market_price,
                edge=edge,
                odds=odds,
            )
        )

    return signals
