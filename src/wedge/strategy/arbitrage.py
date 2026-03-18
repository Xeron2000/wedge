from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from wedge.market.models import MarketBucket
from wedge.log import get_logger

log = get_logger("strategy.arbitrage")

# If sum of all bucket prices < this threshold, flag as arbitrage opportunity
_ARBITRAGE_THRESHOLD = 0.95
# Minimum number of buckets required to consider arbitrage
_MIN_BUCKETS = 3


@dataclass
class ArbitrageSignal:
    city: str
    date: date
    buckets: list[MarketBucket]
    price_sum: float  # sum of all bucket market prices
    gap: float  # 1 - price_sum (potential profit)
    token_ids: list[str] = field(default_factory=list)

    @property
    def expected_profit_pct(self) -> float:
        """Minimum profit as fraction of total outlay if all buckets bought."""
        # Buy 1 share of each bucket: cost = price_sum, receive 1.0 on winner
        # Worst case: we hold N-1 losing positions but exactly one wins
        # Net: 1.0 - price_sum
        return self.gap

    def __str__(self) -> str:
        return (
            f"[ARBITRAGE] city={self.city} date={self.date} "
            f"buckets={len(self.buckets)} sum={self.price_sum:.4f} "
            f"gap={self.gap:.4f} ({self.gap * 100:.1f}% profit)"
        )


def detect_bucket_arbitrage(
    buckets: list[MarketBucket],
    threshold: float = _ARBITRAGE_THRESHOLD,
    min_buckets: int = _MIN_BUCKETS,
) -> ArbitrageSignal | None:
    """Detect cross-bucket arbitrage for a single city+date.

    When a Polymarket event has multiple temperature buckets (e.g., <60°F,
    60-65°F, 65-70°F, >70°F), exactly one bucket must resolve to YES.
    Therefore, the sum of all bucket prices must equal 1.0 in a perfect market.

    If sum < threshold (e.g., 0.95), buying all buckets guarantees a profit
    of (1.0 - sum) on the winning bucket minus the cost of losers.

    Args:
        buckets: All buckets for a single city+date combination.
        threshold: Trigger if price_sum < threshold (default 0.95 = 5% gap).
        min_buckets: Minimum buckets required (skip sparse markets).

    Returns:
        ArbitrageSignal if opportunity detected, else None.
    """
    if len(buckets) < min_buckets:
        return None

    # Validate all buckets are same city+date
    cities = {b.city for b in buckets}
    dates = {b.date for b in buckets}
    if len(cities) != 1 or len(dates) != 1:
        log.warning(
            "arbitrage_mixed_buckets",
            cities=list(cities),
            dates=[str(d) for d in dates],
        )
        return None

    price_sum = sum(b.market_price for b in buckets)
    gap = 1.0 - price_sum

    if price_sum < threshold:
        signal = ArbitrageSignal(
            city=buckets[0].city,
            date=buckets[0].date,
            buckets=buckets,
            price_sum=round(price_sum, 6),
            gap=round(gap, 6),
            token_ids=[b.token_id for b in buckets],
        )
        log.info(
            "arbitrage_detected",
            city=signal.city,
            date=str(signal.date),
            bucket_count=len(buckets),
            price_sum=signal.price_sum,
            gap=signal.gap,
            profit_pct=f"{signal.gap * 100:.1f}%",
        )
        return signal

    log.debug(
        "arbitrage_no_gap",
        city=buckets[0].city,
        date=str(buckets[0].date),
        price_sum=round(price_sum, 4),
        gap=round(gap, 4),
    )
    return None


def group_buckets_by_city_date(
    buckets: list[MarketBucket],
) -> dict[tuple[str, date], list[MarketBucket]]:
    """Group a flat list of buckets by (city, date) for arbitrage scanning."""
    groups: dict[tuple[str, date], list[MarketBucket]] = {}
    for bucket in buckets:
        key = (bucket.city, bucket.date)
        groups.setdefault(key, []).append(bucket)
    return groups


def scan_arbitrage(
    buckets: list[MarketBucket],
    threshold: float = _ARBITRAGE_THRESHOLD,
    min_buckets: int = _MIN_BUCKETS,
) -> list[ArbitrageSignal]:
    """Scan all buckets and return all detected arbitrage opportunities.

    Args:
        buckets: All market buckets across all cities/dates.
        threshold: Trigger threshold for price sum.
        min_buckets: Minimum buckets per city+date group.

    Returns:
        List of ArbitrageSignal, sorted by gap descending (best first).
    """
    groups = group_buckets_by_city_date(buckets)
    signals: list[ArbitrageSignal] = []

    for (city, dt), group in groups.items():
        signal = detect_bucket_arbitrage(group, threshold=threshold, min_buckets=min_buckets)
        if signal is not None:
            signals.append(signal)

    signals.sort(key=lambda s: s.gap, reverse=True)

    if signals:
        log.info(
            "arbitrage_scan_complete",
            total_groups=len(groups),
            opportunities=len(signals),
            best_gap=f"{signals[0].gap * 100:.1f}%" if signals else "0%",
        )
    else:
        log.debug("arbitrage_scan_complete", total_groups=len(groups), opportunities=0)

    return signals
