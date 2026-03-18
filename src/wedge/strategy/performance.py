"""City-level forecast performance tracking.

Tracks per-city Brier scores over a rolling window.
Used to dynamically filter out cities where the model is underperforming.

Brier score: mean((p_model - outcome)^2)
- Perfect: 0.0
- Random: 0.25
- Threshold: 0.20 (configurable)
"""
from __future__ import annotations

from datetime import datetime, timezone

from wedge.db import Database
from wedge.log import get_logger

log = get_logger("strategy.performance")

_DEFAULT_WINDOW_DAYS = 30
_DEFAULT_MIN_SAMPLES = 5  # Need at least N settled trades to trust the score


async def update_city_performance(
    db: Database,
    city: str,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> float | None:
    """Recompute and persist per-city Brier score from settled trades.

    Args:
        db: Database instance
        city: City name
        window_days: Rolling window in days

    Returns:
        Updated Brier score, or None if insufficient data
    """
    from datetime import date, timedelta

    end_date = date.today()
    start_date = end_date - timedelta(days=window_days)

    trades = await db.get_settled_trades(start_date, end_date)
    city_trades = [t for t in trades if t["city"] == city]

    if len(city_trades) < _DEFAULT_MIN_SAMPLES:
        log.debug(
            "city_performance_insufficient_data",
            city=city,
            sample_count=len(city_trades),
            min_required=_DEFAULT_MIN_SAMPLES,
        )
        return None

    # Brier score: mean((p_model - outcome)^2)
    # outcome: 1.0 if trade won (pnl > 0), 0.0 if lost
    brier_sum = 0.0
    valid = 0
    for trade in city_trades:
        p_model = trade.get("p_model")
        pnl = trade.get("pnl")
        if p_model is None or pnl is None:
            continue
        outcome = 1.0 if pnl > 0 else 0.0
        brier_sum += (p_model - outcome) ** 2
        valid += 1

    if valid < _DEFAULT_MIN_SAMPLES:
        return None

    brier_score = brier_sum / valid

    await db.upsert_city_performance(
        city=city,
        window_days=window_days,
        brier_score=brier_score,
        sample_count=valid,
        updated_at=datetime.now(timezone.utc).isoformat(),
    )

    log.info(
        "city_performance_updated",
        city=city,
        brier_score=round(brier_score, 4),
        sample_count=valid,
        window_days=window_days,
    )
    return brier_score


async def get_city_filter(
    db: Database,
    cities: list[str],
    max_brier: float = 0.20,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> dict[str, bool]:
    """Get pass/fail filter for each city based on recent Brier score.

    Cities with insufficient history default to allowed (True).

    Args:
        db: Database instance
        cities: List of city names to check
        max_brier: Maximum acceptable Brier score (lower = better)
        window_days: Rolling window in days

    Returns:
        Dict mapping city -> allowed (True = trade, False = skip)
    """
    result: dict[str, bool] = {}
    for city in cities:
        score = await db.get_city_performance(city, window_days)
        if score is None:
            # No data yet — allow by default
            result[city] = True
            log.debug("city_filter_no_data", city=city, allowed=True)
        elif score > max_brier:
            result[city] = False
            log.info(
                "city_filter_blocked",
                city=city,
                brier_score=round(score, 4),
                max_brier=max_brier,
            )
        else:
            result[city] = True
            log.debug(
                "city_filter_allowed",
                city=city,
                brier_score=round(score, 4),
            )
    return result


async def update_all_city_performance(
    db: Database,
    cities: list[str],
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> dict[str, float]:
    """Update performance scores for all cities after settlement.

    Args:
        db: Database instance
        cities: List of city names
        window_days: Rolling window in days

    Returns:
        Dict mapping city -> brier_score (only cities with sufficient data)
    """
    results: dict[str, float] = {}
    for city in cities:
        score = await update_city_performance(db, city, window_days)
        if score is not None:
            results[city] = score
    return results
