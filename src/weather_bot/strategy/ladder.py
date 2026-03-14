from __future__ import annotations

from weather_bot.market.models import Position
from weather_bot.strategy.kelly import fractional_kelly
from weather_bot.strategy.models import EdgeSignal


def evaluate_ladder(
    signals: list[EdgeSignal],
    budget: float,
    edge_threshold: float = 0.05,
    kelly_fraction: float = 0.15,
    max_bet: float = 100.0,
    max_bet_pct: float = 0.05,
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
        bet = fractional_kelly(
            p_model=signal.p_model,
            market_price=signal.p_market,
            bankroll=remaining,
            fraction=kelly_fraction,
            max_bet=max_bet,
            max_bet_pct=max_bet_pct,
        )
        if bet <= 0:
            continue
        if bet > remaining:
            break

        from weather_bot.market.models import MarketBucket

        positions.append(
            Position(
                bucket=MarketBucket(
                    token_id=signal.token_id,
                    city=signal.city,
                    date=signal.date,
                    temp_f=signal.temp_f,
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
