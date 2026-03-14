from __future__ import annotations


def allocate(
    bankroll: float,
    ladder_pct: float = 0.70,
    tail_pct: float = 0.20,
) -> tuple[float, float, float]:
    """Split bankroll into (ladder_budget, tail_budget, cash_reserve)."""
    cash_pct = 1.0 - ladder_pct - tail_pct
    return (
        bankroll * ladder_pct,
        bankroll * tail_pct,
        bankroll * cash_pct,
    )
