from __future__ import annotations

import math

_EPS = 1e-6


def fractional_kelly(
    p_model: float,
    market_price: float,
    bankroll: float,
    fraction: float = 0.15,
    max_bet: float = 100.0,
    max_bet_pct: float = 0.05,
) -> float:
    """Calculate fractional Kelly bet size with numerical safety guards."""
    if bankroll <= 0:
        return 0.0
    if not (_EPS < market_price < 1 - _EPS):
        return 0.0
    if p_model <= market_price:
        return 0.0

    b = (1.0 - market_price) / market_price  # odds
    if b < _EPS:  # pragma: no cover — guarded by market_price < 1-EPS above
        return 0.0

    q = 1.0 - p_model
    f_full = (p_model * b - q) / b  # full Kelly fraction

    if f_full <= 0 or not math.isfinite(f_full):  # pragma: no cover — edge>0 guarantees f_full>0
        return 0.0

    f_actual = f_full * fraction
    bet = f_actual * bankroll

    cap = min(max_bet, bankroll * max_bet_pct)
    bet = max(0.0, min(bet, cap))

    if not math.isfinite(bet):  # pragma: no cover — all inputs finite after guards
        return 0.0

    return bet
