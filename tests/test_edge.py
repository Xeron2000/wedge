from __future__ import annotations

from datetime import date

from weather_bot.market.models import MarketBucket
from weather_bot.strategy.edge import detect_edges
from weather_bot.weather.models import ForecastDistribution


def _forecast(buckets: dict[int, float]) -> ForecastDistribution:
    from datetime import UTC, datetime

    return ForecastDistribution(
        city="NYC",
        date=date(2026, 7, 1),
        buckets=buckets,
        ensemble_spread=2.0,
        member_count=30,
        updated_at=datetime.now(UTC),
    )


def _market(temp_f: int, price: float) -> MarketBucket:
    return MarketBucket(
        token_id=f"tok_{temp_f}",
        city="NYC",
        date=date(2026, 7, 1),
        temp_f=temp_f,
        market_price=price,
        implied_prob=price,
    )


class TestEdgeDetection:
    def test_positive_edge(self):
        forecast = _forecast({78: 0.25, 79: 0.30, 80: 0.20})
        markets = [_market(79, 0.20)]  # edge = 0.30 - 0.20 = 0.10
        signals = detect_edges(forecast, markets)
        assert len(signals) == 1
        assert signals[0].edge > 0.05

    def test_negative_edge_filtered(self):
        forecast = _forecast({78: 0.10})
        markets = [_market(78, 0.20)]  # edge = -0.10
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_zero_edge_filtered(self):
        forecast = _forecast({78: 0.20})
        markets = [_market(78, 0.20)]  # edge = 0
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_edge_below_threshold(self):
        forecast = _forecast({78: 0.22})
        markets = [_market(78, 0.20)]  # edge = 0.02 < 0.05
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_invalid_market_price_filtered(self):
        forecast = _forecast({78: 0.50})
        markets = [_market(78, 0.0), _market(78, 1.0)]
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_missing_temp_in_forecast(self):
        forecast = _forecast({78: 0.30})
        markets = [_market(90, 0.05)]  # temp 90 not in forecast → p_model = 0
        signals = detect_edges(forecast, markets)
        assert len(signals) == 0

    def test_multiple_edges(self):
        forecast = _forecast({77: 0.20, 78: 0.25, 79: 0.30})
        markets = [_market(77, 0.10), _market(78, 0.15), _market(79, 0.18)]
        signals = detect_edges(forecast, markets)
        assert len(signals) == 3
        assert all(s.edge > 0.05 for s in signals)
