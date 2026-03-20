from __future__ import annotations

from datetime import UTC, date

from wedge.market.models import MarketBucket
from wedge.strategy.models import EdgeSignal
from wedge.weather.models import ForecastDistribution

_EPS = 1e-6
_DEFAULT_FEE_RATE = 0.02


def estimate_slippage(volume_24h: float, bet_size: float) -> float:
    """Estimate slippage cost as fraction of bet size."""
    if volume_24h <= 0:
        return 0.05

    if volume_24h < 1000:
        base_slippage = 0.05
    elif volume_24h < 5000:
        base_slippage = 0.02
    elif volume_24h < 25000:
        base_slippage = 0.01
    else:
        base_slippage = 0.005

    size_ratio = bet_size / max(volume_24h, 1)
    size_multiplier = 1.0 + (size_ratio * 10)
    return min(base_slippage * size_multiplier, 0.10)


def calculate_ev(
    p_model: float,
    market_price: float,
    fee_rate: float = _DEFAULT_FEE_RATE,
    slippage: float = 0.0,
) -> float:
    """Calculate expected value of a binary option bet."""
    if not (_EPS < market_price < 1 - _EPS):
        return 0.0

    odds = (1.0 - market_price) / market_price
    win_ev = p_model * (1 - fee_rate) * odds
    loss_ev = 1.0 - p_model
    return win_ev - loss_ev - slippage


def detect_edges(
    forecast: ForecastDistribution,
    markets: list[MarketBucket],
    ladder_threshold: float = 0.05,
    tail_threshold: float = 0.08,
    fee_rate: float = _DEFAULT_FEE_RATE,
    target_date: date | None = None,
) -> list[EdgeSignal]:
    """Find buckets where model probability exceeds market pricing."""
    del tail_threshold, target_date

    signals: list[EdgeSignal] = []
    min_threshold = ladder_threshold

    for bucket in markets:
        if not (_EPS < bucket.market_price < 1 - _EPS):
            continue

        if bucket.temp_unit == "C":
            lookup_temp = round(bucket.temp_value * 9 / 5 + 32)
        else:
            lookup_temp = bucket.temp_value

        p_model = forecast.buckets.get(lookup_temp, 0.0)
        volume_24h = getattr(bucket, "volume_24h", 5000.0)
        slippage = estimate_slippage(volume_24h, bet_size=50.0)
        ev = calculate_ev(p_model, bucket.market_price, fee_rate, slippage)
        edge = p_model - bucket.market_price

        if ev <= 0 or edge <= min_threshold:
            continue

        odds = (1.0 - bucket.market_price) / bucket.market_price

        from datetime import datetime

        now = datetime.now(UTC)
        age_hours = (now - forecast.updated_at).total_seconds() / 3600.0
        if age_hours < 1.0:
            forecast_weight = 1.3
        elif age_hours < 2.0:
            forecast_weight = 1.0
        elif age_hours < 4.0:
            forecast_weight = 0.8
        else:
            forecast_weight = 0.6

        signals.append(
            EdgeSignal(
                city=bucket.city,
                date=bucket.date,
                temp_value=bucket.temp_value,
                temp_unit=bucket.temp_unit,
                token_id=bucket.token_id,
                p_model=p_model,
                p_market=bucket.market_price,
                edge=edge,
                odds=odds,
                ensemble_spread=forecast.ensemble_spread,
                forecast_age_hours=round(age_hours, 2),
                weight=forecast_weight,
            )
        )

    return signals
