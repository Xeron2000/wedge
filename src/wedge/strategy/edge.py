from __future__ import annotations

from datetime import date

from wedge.market.models import MarketBucket
from wedge.strategy.models import EdgeSignal
from wedge.weather.models import ForecastDistribution

_EPS = 1e-6

# Default fee and slippage parameters
_DEFAULT_FEE_RATE = 0.02  # 2% Polymarket fee on winnings


def estimate_slippage(volume_24h: float, bet_size: float) -> float:
    """Estimate slippage cost as fraction of bet_size.

    Slippage increases as bet size grows relative to market volume.
    Low liquidity markets have higher slippage.

    Args:
        volume_24h: 24h trading volume in USD
        bet_size: Intended bet size in USD

    Returns:
        Slippage cost as fraction of bet_size (e.g., 0.02 = 2%)
    """
    if volume_24h <= 0:
        return 0.05  # Unknown volume, assume high slippage

    # Volume-based slippage tiers
    if volume_24h < 1000:
        base_slippage = 0.05  # 5% for illiquid markets
    elif volume_24h < 5000:
        base_slippage = 0.02  # 2% for low liquidity
    elif volume_24h < 25000:
        base_slippage = 0.01  # 1% for medium liquidity
    else:
        base_slippage = 0.005  # 0.5% for high liquidity

    # Scale by bet size relative to volume
    # Larger bets relative to volume cause more slippage
    size_ratio = bet_size / max(volume_24h, 1)
    size_multiplier = 1.0 + (size_ratio * 10)  # Up to 2x for very large bets

    return min(base_slippage * size_multiplier, 0.10)  # Cap at 10%


def calculate_ev(
    p_model: float,
    market_price: float,
    fee_rate: float = _DEFAULT_FEE_RATE,
    slippage: float = 0.0,
) -> float:
    """Calculate expected value of a binary option bet.

    EV = (p_model * (1 - fee) * odds) - (1 - p_model) - slippage

    Args:
        p_model: Calibrated model probability
        market_price: Market price (0-1)
        fee_rate: Fee rate on winnings (default 2% for Polymarket)
        slippage: Slippage cost as fraction of bet

    Returns:
        Expected value (positive = profitable bet)
    """
    if not (_EPS < market_price < 1 - _EPS):
        return 0.0

    odds = (1.0 - market_price) / market_price

    # Expected profit per unit bet
    # Win scenario: gain odds, pay fee on winnings
    win_ev = p_model * (1 - fee_rate) * odds
    # Loss scenario: lose stake
    loss_ev = (1 - p_model)

    ev = win_ev - loss_ev - slippage
    return ev


def detect_edges(
    forecast: ForecastDistribution,
    markets: list[MarketBucket],
    ladder_threshold: float = 0.05,
    tail_threshold: float = 0.08,
    fee_rate: float = _DEFAULT_FEE_RATE,
    target_date: date | None = None,
) -> list[EdgeSignal]:
    """Find buckets where model probability exceeds market pricing.

    Applies calibration and calculates EV including fees and slippage.

    Args:
        forecast: Weather forecast distribution
        markets: List of market buckets
        ladder_threshold: Minimum edge for ladder strategy
        tail_threshold: Minimum edge for tail strategy
        fee_rate: Fee rate on winnings (default 2%)
        target_date: Date for calibration lookup (optional)

    Returns:
        List of edge signals with positive EV
    """
    from wedge.weather.calibration import apply_calibration, get_season

    signals: list[EdgeSignal] = []

    for bucket in markets:
        if not (_EPS < bucket.market_price < 1 - _EPS):
            continue

        # Get raw model probability
        # Convert market temp to Fahrenheit for lookup if needed
        if bucket.temp_unit == "C":
            lookup_temp = round(bucket.temp_value * 9 / 5 + 32)
        else:
            lookup_temp = bucket.temp_value
        p_raw = forecast.buckets.get(lookup_temp, 0.0)

        # Apply calibration if date is provided
        if target_date is not None:
            season = get_season(target_date)
            p_model = apply_calibration(p_raw, bucket.city, target_date)
        else:
            p_model = p_raw

        # Calculate EV with fees and slippage
        # Use bucket volume if available, otherwise estimate
        volume_24h = getattr(bucket, "volume_24h", 5000.0)
        slippage = estimate_slippage(volume_24h, bet_size=50.0)  # Assume $50 bet
        ev = calculate_ev(p_model, bucket.market_price, fee_rate, slippage)

        # Calculate raw edge (for backwards compatibility)
        edge = p_model - bucket.market_price

        # Use EV-based threshold (more accurate than raw edge)
        # Convert edge threshold to EV threshold for comparison
        min_threshold = min(ladder_threshold, tail_threshold)

        # Only include signals with positive EV AND sufficient edge
        if ev <= 0 or edge <= min_threshold:
            continue

        odds = (1.0 - bucket.market_price) / bucket.market_price

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
            )
        )

    return signals
