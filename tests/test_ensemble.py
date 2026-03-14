from __future__ import annotations

import math
from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from weather_bot.weather.ensemble import parse_distribution


def _make_raw(members: list[list[float]], dates: list[str]) -> dict:
    daily: dict = {"time": dates}
    for i, temps in enumerate(members, 1):
        daily[f"temperature_2m_max_member{i:02d}"] = temps
    return {"daily": daily}


class TestParseDistribution:
    def test_basic_30_members(self):
        temps = [[75.0 + i] for i in range(30)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 30
        assert abs(sum(result.buckets.values()) - 1.0) < 1e-9

    def test_dynamic_member_count(self):
        for n in [10, 15, 30, 51]:
            temps = [[80.0 + (i % 5)] for i in range(n)]
            raw = _make_raw(temps, ["2026-07-01"])
            result = parse_distribution(raw, "NYC", date(2026, 7, 1))
            assert result is not None
            assert result.member_count == n

    def test_below_min_members(self):
        temps = [[80.0] for _ in range(5)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is None

    def test_null_values_skipped(self):
        temps = [[80.0] for _ in range(15)]
        temps += [[None] for _ in range(5)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 15

    def test_nan_values_skipped(self):
        temps = [[80.0] for _ in range(15)]
        temps += [[float("nan")] for _ in range(5)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 15

    def test_date_not_found(self):
        raw = _make_raw([[80.0] for _ in range(30)], ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 15))
        assert result is None

    def test_half_boundary_rounding(self):
        temps = [[79.5] for _ in range(15)] + [[80.5] for _ in range(15)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        # round(79.5) = 80, round(80.5) = 80 (banker's rounding)
        # All should be accounted for
        assert abs(sum(result.buckets.values()) - 1.0) < 1e-9

    def test_empty_daily(self):
        result = parse_distribution({"daily": {}}, "NYC", date(2026, 7, 1))
        assert result is None

    def test_spread_calculation(self):
        temps = [[70.0] for _ in range(15)] + [[90.0] for _ in range(15)]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.ensemble_spread > 0


class TestEnsemblePBT:
    @given(
        member_temps=st.lists(
            st.floats(min_value=-50, max_value=150, allow_nan=False, allow_infinity=False),
            min_size=10,
            max_size=60,
        )
    )
    @settings(max_examples=200)
    def test_normalization_invariant(self, member_temps):
        temps = [[t] for t in member_temps]
        raw = _make_raw(temps, ["2026-07-01"])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        if result is not None:
            total = sum(result.buckets.values())
            assert abs(total - 1.0) < 1e-9
            assert all(v >= 0 for v in result.buckets.values())

    @given(
        member_temps=st.lists(
            st.floats(min_value=-50, max_value=150, allow_nan=False, allow_infinity=False),
            min_size=10,
            max_size=60,
        )
    )
    @settings(max_examples=100)
    def test_permutation_invariant(self, member_temps):
        import random

        temps1 = [[t] for t in member_temps]
        raw1 = _make_raw(temps1, ["2026-07-01"])
        result1 = parse_distribution(raw1, "NYC", date(2026, 7, 1))

        shuffled = member_temps.copy()
        random.shuffle(shuffled)
        temps2 = [[t] for t in shuffled]
        raw2 = _make_raw(temps2, ["2026-07-01"])
        result2 = parse_distribution(raw2, "NYC", date(2026, 7, 1))

        if result1 is not None and result2 is not None:
            assert result1.buckets == result2.buckets
