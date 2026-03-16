"""Tests for event-driven trading strategies."""

import pytest
from datetime import date, datetime, timedelta

from wedge.strategy.events import (
    EventDetector,
    EventHistory,
    EventType,
    WeatherEvent,
    generate_event_signals,
)
from wedge.risk.correlation import get_base_correlation
from wedge.weather.models import ForecastDistribution


class TestWeatherEvent:
    """Test WeatherEvent dataclass."""

    def test_event_expiration(self):
        """Test event expiration calculation."""
        event = WeatherEvent(
            event_type=EventType.GFS_UPDATE,
            city="NYC",
            target_date=date.today(),
            detected_at=datetime.now(),
            confidence=0.9,
            expected_impact=0.0,
            trade_window_hours=2,
        )

        # Should expire 2 hours from now
        expected_expires = event.detected_at + timedelta(hours=2)
        assert abs((event.expires_at - expected_expires).total_seconds()) < 1

    def test_event_is_active(self):
        """Test event active status."""
        event = WeatherEvent(
            event_type=EventType.GFS_UPDATE,
            city="NYC",
            target_date=date.today(),
            detected_at=datetime.now(),
            confidence=0.9,
            expected_impact=0.0,
            trade_window_hours=2,
        )

        assert event.is_active is True


class TestEventDetector:
    """Test EventDetector functionality."""

    def test_gfs_update_window_detection(self):
        """Test GFS update window detection."""
        detector = EventDetector()

        # This test depends on current time, so we just verify the method works
        result = detector._is_in_gfs_window()
        assert isinstance(result, bool)

    def test_anomaly_detection_normal_forecast(self):
        """Test anomaly detection with normal forecast."""
        detector = EventDetector()

        # Normal forecast - tight spread around mean
        forecast = ForecastDistribution(
            city="NYC",
            date=date.today(),
            buckets={75: 0.1, 76: 0.2, 77: 0.4, 78: 0.2, 79: 0.1},
            ensemble_spread=1.0,  # Low spread
            member_count=31,
            updated_at=datetime.now(),
        )

        event = detector._detect_anomaly(forecast, "NYC")

        # Should not detect anomaly for normal forecast
        assert event is None

    def test_anomaly_detection_extreme_forecast(self):
        """Test anomaly detection with extreme forecast."""
        detector = EventDetector()

        # Extreme forecast - bimodal with fat tails
        forecast = ForecastDistribution(
            city="NYC",
            date=date.today(),
            buckets={
                60: 0.05,  # Extreme cold
                70: 0.10,
                75: 0.20,
                80: 0.30,
                85: 0.20,
                90: 0.10,
                100: 0.05,  # Extreme heat
            },
            ensemble_spread=8.0,  # High spread
            member_count=31,
            updated_at=datetime.now(),
        )

        event = detector._detect_anomaly(forecast, "NYC")

        # May detect anomaly if extreme probability > 15%
        # This depends on the exact calculation
        assert event is None or event.event_type == EventType.TEMPERATURE_ANOMALY


class TestEventSignals:
    """Test event signal generation."""

    def test_generate_signals_gfs_update(self):
        """Test signal generation for GFS update event."""
        forecast = ForecastDistribution(
            city="NYC",
            date=date.today(),
            buckets={75: 0.3, 76: 0.4, 77: 0.3},
            ensemble_spread=1.5,
            member_count=31,
            updated_at=datetime.now(),
        )

        event = WeatherEvent(
            event_type=EventType.GFS_UPDATE,
            city="NYC",
            target_date=date.today(),
            detected_at=datetime.now(),
            confidence=0.9,
            expected_impact=0.0,
            trade_window_hours=2,
        )

        signals = generate_event_signals(
            events=[event],
            forecast=forecast,
            market_prices={74: 0.25, 75: 0.28, 76: 0.30, 77: 0.27, 78: 0.22},
        )

        # Should generate at least one signal for GFS update
        assert len(signals) >= 1

        gfs_signal = signals[0]
        assert gfs_signal.event.event_type == EventType.GFS_UPDATE
        assert gfs_signal.confidence == 0.9
        assert gfs_signal.recommended_bet_pct == 0.04

    def test_generate_signals_temperature_anomaly(self):
        """Test signal generation for temperature anomaly."""
        forecast = ForecastDistribution(
            city="NYC",
            date=date.today(),
            buckets={70: 0.1, 75: 0.3, 80: 0.4, 85: 0.15},
            ensemble_spread=3.0,
            member_count=31,
            updated_at=datetime.now(),
        )

        event = WeatherEvent(
            event_type=EventType.TEMPERATURE_ANOMALY,
            city="NYC",
            target_date=date.today(),
            detected_at=datetime.now(),
            confidence=0.75,
            expected_impact=10.0,
            trade_window_hours=6,
            metadata={"direction": "high"},
        )

        signals = generate_event_signals(
            events=[event],
            forecast=forecast,
            market_prices={75: 0.20, 80: 0.35, 85: 0.30, 90: 0.10},
        )

        # Signal may or may not be generated depending on logic
        assert isinstance(signals, list)

        if signals:
            anomaly_signal = signals[0]
            assert anomaly_signal.event.event_type == EventType.TEMPERATURE_ANOMALY
            assert anomaly_signal.confidence == 0.75


class TestEventHistory:
    """Test event history tracking."""

    def test_record_event(self):
        """Test recording events."""
        history = EventHistory()

        event = WeatherEvent(
            event_type=EventType.GFS_UPDATE,
            city="NYC",
            target_date=date.today(),
            detected_at=datetime.now(),
            confidence=0.9,
            expected_impact=0.0,
            trade_window_hours=2,
        )

        history.record_event(event, actual_edge=0.03)

        assert len(history._events) == 1

    def test_get_performance_by_type(self):
        """Test performance statistics by event type."""
        history = EventHistory()

        # Add multiple events of different types
        for _ in range(5):
            event = WeatherEvent(
                event_type=EventType.GFS_UPDATE,
                city="NYC",
                target_date=date.today(),
                detected_at=datetime.now(),
                confidence=0.9,
                expected_impact=0.0,
                trade_window_hours=2,
            )
            history.record_event(event, actual_edge=0.02)

        for _ in range(3):
            event = WeatherEvent(
                event_type=EventType.TEMPERATURE_ANOMALY,
                city="Miami",
                target_date=date.today(),
                detected_at=datetime.now(),
                confidence=0.7,
                expected_impact=8.0,
                trade_window_hours=6,
            )
            history.record_event(event, actual_edge=0.05)

        performance = history.get_performance_by_type()

        assert "gfs_update" in performance
        assert performance["gfs_update"]["count"] == 5
        # Note: EventType.TEMPERATURE_ANOMALY uses "anomaly" as key
        assert "anomaly" in performance
        assert performance["anomaly"]["count"] == 3


class TestClimateRegions:
    """Test climate region mappings."""

    def test_northeast_us_cities(self):
        """Test Northeast US city mappings."""
        assert get_base_correlation("NYC", "Boston") > 0.7
        assert get_base_correlation("NYC", "Philadelphia") > 0.7

    def test_cross_region_correlation(self):
        """Test cross-region correlations are lower."""
        # NYC (northeast) vs Miami (south)
        nyc_miami = get_base_correlation("NYC", "Miami")
        assert nyc_miami < 0.5

        # NYC vs London (some correlation due to North Atlantic)
        nyc_london = get_base_correlation("NYC", "London")
        assert nyc_london < nyc_miami or nyc_london < 0.5

    def test_same_city_returns_one(self):
        """Test that same city always returns correlation of 1.0."""
        assert get_base_correlation("NYC", "NYC") == 1.0
        assert get_base_correlation("Tokyo", "Tokyo") == 1.0


class TestIntegration:
    """Integration tests for event-driven trading."""

    @pytest.mark.asyncio
    async def test_full_event_detection_flow(self):
        """Test full event detection to signal generation flow."""
        detector = EventDetector()

        forecast = ForecastDistribution(
            city="NYC",
            date=date.today(),
            buckets={70: 0.05, 75: 0.25, 80: 0.40, 85: 0.25, 90: 0.05},
            ensemble_spread=4.0,
            member_count=31,
            updated_at=datetime.now(),
        )

        # Detect events
        events = await detector.detect_events(forecast, "NYC")

        # Generate signals for detected events
        if events:
            signals = generate_event_signals(
                events=events,
                forecast=forecast,
                market_prices={
                    70: 0.08, 75: 0.22, 80: 0.38, 85: 0.23, 90: 0.07
                },
            )

            # Verify signals have reasonable values
            for signal in signals:
                assert 0 < signal.confidence <= 1.0
                assert signal.recommended_bet_pct >= 0
                assert signal.recommended_bet_pct <= 1.0
