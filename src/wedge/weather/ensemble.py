from __future__ import annotations

import math
import re
from datetime import UTC, date, datetime

from wedge.weather.models import ForecastDistribution

_MEMBER_RE = re.compile(r"^temperature_2m_max_member\d+$")
_MIN_MEMBERS = 10


def parse_distribution(
    raw: dict, city: str, target_date: date
) -> ForecastDistribution | None:
    daily = raw.get("daily", {})
    times = daily.get("time", [])
    if not times:
        return None

    try:
        date_idx = times.index(target_date.isoformat())
    except ValueError:
        return None

    member_keys = [k for k in daily if _MEMBER_RE.match(k)]
    if not member_keys:
        return None

    temps: list[float] = []
    for key in member_keys:
        values = daily[key]
        if date_idx < len(values):
            v = values[date_idx]
            if v is not None and math.isfinite(v):
                temps.append(v)

    if len(temps) < _MIN_MEMBERS:
        return None

    buckets: dict[int, int] = {}
    for t in temps:
        bucket = round(t)
        buckets[bucket] = buckets.get(bucket, 0) + 1

    total = sum(buckets.values())
    prob_buckets = {k: v / total for k, v in buckets.items()}

    mean = sum(temps) / len(temps)
    variance = sum((t - mean) ** 2 for t in temps) / len(temps)
    spread = math.sqrt(variance)

    return ForecastDistribution(
        city=city,
        date=target_date,
        buckets=prob_buckets,
        ensemble_spread=spread,
        member_count=len(temps),
        updated_at=datetime.now(UTC),
    )
