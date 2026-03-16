from __future__ import annotations

from wedge.market.models import Position
from wedge.strategy.kelly import fractional_kelly
from wedge.strategy.models import EdgeSignal


def evaluate_ladder(
    signals: list[EdgeSignal],
    budget: float,
    edge_threshold: float = 0.05,
    kelly_fraction: float = 0.10,  # Reduced from 0.15
    max_bet: float = 50.0,         # Reduced from 100
    max_bet_pct: float = 0.03,     # Reduced from 0.05
) -> list[Position]:
    """Select ladder positions: center-region buckets with range edge > threshold."""
    ladder_signals = [s for s in signals if s.edge > edge_threshold]
    if not ladder_signals:
        return []

    # Sort by edge descending to prioritize best opportunities
    ladder_signals.sort(key=lambda s: s.edge, reverse=True)

    positions: list[Position] = []
    remaining = budget

    for signal in ladder_signals:
        result = fractional_kelly(
            p_model=signal.p_model,
            market_price=signal.p_market,
            bankroll=remaining,
            fraction=kelly_fraction,
            max_bet=max_bet,
            max_bet_pct=max_bet_pct,
        )
        bet = result.bet_size
        if bet <= 0:
            continue
        if bet > remaining:
            break

        from wedge.market.models import MarketBucket

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
                strategy="ladder",
                p_model=signal.p_model,
                edge=signal.edge,
            )
        )
        remaining -= bet

    return positions
