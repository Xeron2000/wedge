from __future__ import annotations

from datetime import date

from hypothesis import given, settings
from hypothesis import strategies as st

from wedge.weather.ensemble import parse_distribution


def _make_raw(member_temps: list[float], target_date: str = "2026-07-01") -> dict[str, object]:
    return {
        "source": "noaa_gefs",
        "city": "NYC",
        "target_date": target_date,
        "run_time": "2026-03-20T12:00:00+00:00",
        "member_temps_f": {f"m{i:02d}": temp for i, temp in enumerate(member_temps, 1)},
    }


class TestParseDistribution:
    def test_basic_30_members(self):
        raw = _make_raw([75.0 + i for i in range(30)])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 30
        assert abs(sum(result.buckets.values()) - 1.0) < 1e-9

    def test_dynamic_member_count(self):
        for n in [10, 15, 30, 51]:
            raw = _make_raw([80.0 + (i % 5) for i in range(n)])
            result = parse_distribution(raw, "NYC", date(2026, 7, 1))
            assert result is not None
            assert result.member_count == n

    def test_below_min_members(self):
        raw = _make_raw([80.0 for _ in range(5)])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is None

    def test_null_values_skipped(self):
        raw = {
            "source": "noaa_gefs",
            "city": "NYC",
            "target_date": "2026-07-01",
            "run_time": "2026-03-20T12:00:00+00:00",
            "member_temps_f": {
                **{f"m{i:02d}": 80.0 for i in range(1, 16)},
                **{f"m{i:02d}": None for i in range(16, 21)},
            },
        }
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 15

    def test_nan_values_skipped(self):
        raw = {
            "source": "noaa_gefs",
            "city": "NYC",
            "target_date": "2026-07-01",
            "run_time": "2026-03-20T12:00:00+00:00",
            "member_temps_f": {
                **{f"m{i:02d}": 80.0 for i in range(1, 16)},
                **{f"m{i:02d}": float("nan") for i in range(16, 21)},
            },
        }
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert result.member_count == 15

    def test_date_not_found(self):
        raw = _make_raw([80.0 for _ in range(30)], target_date="2026-07-01")
        result = parse_distribution(raw, "NYC", date(2026, 7, 15))
        assert result is None

    def test_half_boundary_rounding(self):
        raw = _make_raw([79.5 for _ in range(15)] + [80.5 for _ in range(15)])
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        assert result is not None
        assert abs(sum(result.buckets.values()) - 1.0) < 1e-9

    def test_empty_payload(self):
        result = parse_distribution({}, "NYC", date(2026, 7, 1))
        assert result is None

    def test_spread_calculation(self):
        raw = _make_raw([70.0 for _ in range(15)] + [90.0 for _ in range(15)])
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
        raw = _make_raw(member_temps)
        result = parse_distribution(raw, "NYC", date(2026, 7, 1))
        if result is not None:
            total = sum(result.buckets.values())
            assert abs(total - 1.0) < 1e-9
            assert all(value >= 0 for value in result.buckets.values())

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

        raw1 = _make_raw(member_temps)
        result1 = parse_distribution(raw1, "NYC", date(2026, 7, 1))

        shuffled = member_temps.copy()
        random.shuffle(shuffled)
        raw2 = _make_raw(shuffled)
        result2 = parse_distribution(raw2, "NYC", date(2026, 7, 1))

        if result1 is not None and result2 is not None:
            assert result1.buckets == result2.buckets
