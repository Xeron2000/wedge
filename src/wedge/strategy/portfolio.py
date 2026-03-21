from __future__ import annotations


def allocate(
    bankroll: float,
    ladder_pct: float = 0.90,
) -> tuple[float, float, float]:
    """Split bankroll into (ladder_budget, tail_budget, cash_reserve)."""
    cash_pct = 1.0 - ladder_pct
    return (
        bankroll * ladder_pct,
        0.0,
        bankroll * cash_pct,
    )
