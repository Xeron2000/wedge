from __future__ import annotations

from datetime import date
from enum import Enum

from wedge.market.models import MarketBucket, Position
from wedge.strategy.kelly import fractional_kelly
from wedge.strategy.models import EdgeSignal


class ClimateRegion(Enum):
    """Major climate regions for correlation checking.

    Cities in the same region are correlated by weather events.
    """
    NORTHEAST_US = "northeast_us"  # NYC
    SOUTH_US = "south_us"  # Miami
    UK_NW_EUROPE = "uk_nw_europe"  # London
    EAST_ASIA = "east_asia"  # Seoul, Shanghai
    OCEANIA = "oceania"  # Wellington


# Map cities to climate regions
_CITY_TO_REGION = {
    "NYC": ClimateRegion.NORTHEAST_US,
    "Miami": ClimateRegion.SOUTH_US,
    "London": ClimateRegion.UK_NW_EUROPE,
    "Seoul": ClimateRegion.EAST_ASIA,
    "Shanghai": ClimateRegion.EAST_ASIA,
    "Wellington": ClimateRegion.OCEANIA,
}


def get_climate_region(city: str) -> ClimateRegion | None:
    """Get climate region for a city."""
    return _CITY_TO_REGION.get(city)


def check_regional_correlation(
    positions: list[Position],
    new_signal: EdgeSignal,
    max_correlated: int = 2,
) -> bool:
    """Check if adding new position would exceed regional correlation limit.

    Prevents overexposure to single weather events (e.g.,寒流 affecting multiple cities).

    Args:
        positions: Existing positions
        new_signal: New signal being considered
        max_correlated: Maximum positions allowed in same region (default 2)

    Returns:
        True if position is allowed, False if would exceed limit
    """
    new_region = get_climate_region(new_signal.city)
    if new_region is None:
        return True  # Unknown region, allow

    # Count existing positions in same region
    correlated_count = 0
    for pos in positions:
        pos_region = get_climate_region(pos.bucket.city)
        if pos_region == new_region:
            correlated_count += 1

    return correlated_count < max_correlated


def check_event_duplication(
    positions: list[Position],
    new_signal: EdgeSignal,
    event_window_days: int = 3,
) -> bool:
    """Check if new signal is for same weather event as existing position.

    Prevents betting on same weather event multiple times (e.g., same heat wave).

    Args:
        positions: Existing positions
        new_signal: New signal being considered
        event_window_days: Days around event to consider duplicate (default 3)

    Returns:
        True if position is allowed (not duplicate), False if duplicate
    """
    for pos in positions:
        # Same city
        if pos.bucket.city != new_signal.city:
            continue

        # Check if dates are within event window
        date_diff = abs((new_signal.date - pos.bucket.date).days)
        if date_diff <= event_window_days:
            # Same city, similar date = likely same weather event
            return False

    return True


def check_daily_loss_limit(
    positions: list[Position],
    daily_loss_limit: float = 200.0,
) -> bool:
    """Check if daily loss limit has been reached.

    Args:
        positions: Existing positions (including settled P&L)
        daily_loss_limit: Maximum daily loss allowed (default $200)

    Returns:
        True if can continue trading, False if limit hit
    """
    # Calculate daily P&L from settled positions
    daily_pnl = sum(
        pos.size * (pos.entry_price - 1.0)  # Simplified: assume loss = stake
        for pos in positions
    )
    return daily_pnl > -daily_loss_limit


def evaluate_tail(
    signals: list[EdgeSignal],
    budget: float,
    edge_threshold: float = 0.08,
    min_odds: float = 10.0,
    kelly_fraction: float = 0.15,
    max_bet: float = 100.0,
    max_bet_pct: float = 0.05,
    max_correlated: int = 2,
    daily_loss_limit: float = 200.0,
    existing_positions: list[Position] | None = None,
    spread_baseline: float = 3.0,  # Ensemble spread baseline (°F) for Kelly damping
) -> list[Position]:
    """Select tail positions: extreme temps with high odds and significant edge.

    Applies risk controls:
    - Regional correlation limit (max 2 positions per climate region)
    - Event duplication check (no betting same weather event twice)
    - Daily loss limit (stop trading if limit hit)

    Args:
        signals: Edge signals to evaluate
        budget: Available budget for tail strategy
        edge_threshold: Minimum edge required (default 0.08)
        min_odds: Minimum odds required (default 10.0)
        kelly_fraction: Kelly criterion fraction (default 0.15)
        max_bet: Maximum bet size in USD (default 100)
        max_bet_pct: Maximum bet as % of bankroll (default 5%)
        max_correlated: Maximum correlated positions (default 2)
        daily_loss_limit: Daily loss limit in USD (default $200)
        existing_positions: Existing positions for correlation check

    Returns:
        List of tail positions
    """
    # Check daily loss limit first
    if existing_positions:
        if not check_daily_loss_limit(existing_positions, daily_loss_limit):
            return []  # Stop trading for today

    # Filter signals by edge and odds
    tail_signals = [
        s for s in signals if s.edge > edge_threshold and s.odds >= min_odds
    ]
    if not tail_signals:
        return []

    # Sort by EV (edge * odds)
    tail_signals.sort(key=lambda s: s.edge * s.odds, reverse=True)

    positions: list[Position] = []
    remaining = budget

    for signal in tail_signals:
        # Check regional correlation
        all_positions = (existing_positions or []) + positions
        if not check_regional_correlation(all_positions, signal, max_correlated):
            continue

        # Check event duplication
        if not check_event_duplication(all_positions, signal):
            continue

        # Calculate bet size
        result = fractional_kelly(
            p_model=signal.p_model,
            market_price=signal.p_market,
            bankroll=remaining,
            fraction=kelly_fraction * signal.weight,
            max_bet=max_bet,
            max_bet_pct=max_bet_pct,
            ensemble_spread=signal.ensemble_spread,
            spread_baseline=spread_baseline,
        )
        bet = result.bet_size
        if bet <= 0:
            continue
        if bet > remaining:
            break

        positions.append(
            Position(
                bucket=MarketBucket(
                    token_id=signal.token_id,
                    city=signal.city,
                    date=signal.date,
                    temp_value=signal.temp_value,
                    temp_unit=signal.temp_unit,
                    market_price=signal.p_market,
                    implied_prob=signal.p_market,
                ),
                size=bet,
                entry_price=signal.p_market,
                strategy="tail",
                p_model=signal.p_model,
                edge=signal.edge,
            )
        )
        remaining -= bet

    return positions
