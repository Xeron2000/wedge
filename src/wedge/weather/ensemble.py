from __future__ import annotations

import math
from datetime import UTC, date, datetime

from wedge.weather.models import ForecastDistribution

_MIN_MEMBERS = 10


def parse_distribution(
    raw: dict[str, object], city: str, target_date: date
) -> ForecastDistribution | None:
    if raw.get("source") != "noaa_gefs":
        return None
    if raw.get("target_date") != target_date.isoformat():
        return None

    member_temps = raw.get("member_temps_f")
    if not isinstance(member_temps, dict):
        return None

    temps: list[float] = []
    for value in member_temps.values():
        if isinstance(value, (int, float)) and math.isfinite(value):
            temps.append(float(value))

    if len(temps) < _MIN_MEMBERS:
        return None

    buckets: dict[int, int] = {}
    for temp in temps:
        bucket = round(temp)
        buckets[bucket] = buckets.get(bucket, 0) + 1

    total = sum(buckets.values())
    prob_buckets = {bucket: count / total for bucket, count in buckets.items()}

    mean = sum(temps) / len(temps)
    variance = sum((temp - mean) ** 2 for temp in temps) / len(temps)
    spread = math.sqrt(variance)

    run_time = raw.get("run_time")
    updated_at = datetime.now(UTC)
    if isinstance(run_time, str):
        try:
            updated_at = datetime.fromisoformat(run_time)
        except ValueError:
            updated_at = datetime.now(UTC)

    return ForecastDistribution(
        city=city,
        date=target_date,
        buckets=prob_buckets,
        ensemble_spread=spread,
        member_count=len(temps),
        updated_at=updated_at,
    )
