"""Event-driven trading strategies for weather markets.

Detects and trades on:
1. GFS model update windows (4x daily)
2. NOAA weather alerts/warnings
3. Temperature anomalies (extreme events)
4. Historical record breaks
5. Liquidity events (weekend/holiday thinning)
"""

from __future__ import annotations

import httpx
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Literal

from wedge.log import get_logger
from wedge.weather.models import ForecastDistribution

log = get_logger("strategy.events")


class EventType(Enum):
    """Types of tradable weather events."""
    GFS_UPDATE = "gfs_update"           # GFS model refresh
    WEATHER_ALERT = "weather_alert"     # NOAA warning/advisory
    TEMPERATURE_ANOMALY = "anomaly"     # Extreme temperature deviation
    RECORD_BREAK = "record_break"       # Historical record broken
    LIQUIDITY_THIN = "liquidity_thin"   # Low liquidity event


class AlertSeverity(Enum):
    """NOAA weather alert severity levels."""
    EXTREME = "extreme"    # Warning - immediate threat
    SEVERE = "severe"      # Watch - possible severe weather
    MODERATE = "moderate"  # Advisory - nuisance impact
    MINOR = "minor"        # Statement - informational


@dataclass
class WeatherEvent:
    """Represents a tradable weather event."""
    event_type: EventType
    city: str
    target_date: date
    detected_at: datetime
    confidence: float  # 0-1 confidence in event
    expected_impact: float  # Expected temperature impact (°F)
    trade_window_hours: int  # How long the edge persists
    metadata: dict | None = None

    @property
    def expires_at(self) -> datetime:
        """Event expiration time."""
        return self.detected_at + timedelta(hours=self.trade_window_hours)

    @property
    def is_active(self) -> bool:
        """Check if event is still active."""
        return datetime.now() < self.expires_at


@dataclass
class NOAAAlert:
    """NOAA weather alert."""
    alert_type: str  # Warning, Watch, Advisory
    severity: AlertSeverity
    headline: str
    description: str
    effective: datetime
    expires: datetime
    areas: list[str]  # Affected areas
    temperature_impact: float | None = None  # Expected temp impact


class EventDetector:
    """Detects tradable weather events.

    Usage:
        detector = EventDetector()

        # Check for events
        events = await detector.detect_events(forecast, city_config)

        # Get NOAA alerts for city
        alerts = await detector.get_noaa_alerts("NYC")

        # Check if GFS just updated
        if await detector.is_gfs_update_window():
            print("GFS update window - expect higher edge!")
    """

    # GFS update times (UTC)
    GFS_UPDATE_HOURS = [0, 6, 12, 18]  # 00Z, 06Z, 12Z, 18Z
    GFS_DELAY_HOURS = 4  # Data available ~4h after model run
    UPDATE_WINDOW_HOURS = 2  # Edge window after data release

    # Anomaly thresholds
    ANOMALY_THRESHOLD_STD = 2.5  # 2.5σ = anomaly
    EXTREME_ANOMALY_STD = 3.5    # 3.5σ = extreme anomaly

    # Record break lookback
    RECORD_LOOKBACK_YEARS = 10

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        self._client = http_client
        self._noaa_base_url = "https://api.weather.gov"

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def detect_events(
        self,
        forecast: ForecastDistribution,
        city: str,
    ) -> list[WeatherEvent]:
        """Detect all tradable events for a forecast.

        Args:
            forecast: Weather forecast distribution
            city: City name

        Returns:
            List of detected events
        """
        events = []

        # Check for GFS update window
        if self._is_in_gfs_window():
            events.append(WeatherEvent(
                event_type=EventType.GFS_UPDATE,
                city=city,
                target_date=forecast.date,
                detected_at=datetime.now(),
                confidence=0.9,
                expected_impact=0.0,  # Informational only
                trade_window_hours=self.UPDATE_WINDOW_HOURS,
                metadata={"forecast_date": forecast.date},
            ))

        # Check for temperature anomalies
        anomaly_event = self._detect_anomaly(forecast, city)
        if anomaly_event:
            events.append(anomaly_event)

        # Check for NOAA alerts (async, may fail)
        try:
            alerts = await self.get_noaa_alerts(city)
            for alert in alerts:
                if alert.severity in (AlertSeverity.EXTREME, AlertSeverity.SEVERE):
                    events.append(WeatherEvent(
                        event_type=EventType.WEATHER_ALERT,
                        city=city,
                        target_date=forecast.date,
                        detected_at=datetime.now(),
                        confidence=0.95 if alert.severity == AlertSeverity.EXTREME else 0.8,
                        expected_impact=alert.temperature_impact or 5.0,
                        trade_window_hours=6 if alert.severity == AlertSeverity.EXTREME else 12,
                        metadata={"alert": alert.headline},
                    ))
        except Exception as e:
            log.warning("noaa_alert_check_failed", city=city, error=str(e))

        return events

    def _is_in_gfs_window(self) -> bool:
        """Check if currently in GFS update edge window."""
        now = datetime.now(UTC)
        current_hour = now.hour

        for update_hour in self.GFS_UPDATE_HOURS:
            # Data available ~4h after model run
            data_available_hour = (update_hour + self.GFS_DELAY_HOURS) % 24

            # Check if within edge window
            hours_since_update = (current_hour - data_available_hour) % 24
            if hours_since_update <= self.UPDATE_WINDOW_HOURS:
                return True

        return False

    async def is_gfs_update_window(self) -> bool:
        """Public method to check GFS update window."""
        return self._is_in_gfs_window()

    def _detect_anomaly(
        self,
        forecast: ForecastDistribution,
        city: str,
    ) -> WeatherEvent | None:
        """Detect temperature anomalies in forecast.

        Uses ensemble spread to identify extreme deviations.
        """
        if not forecast.buckets:
            return None

        # Calculate mean and std from ensemble
        temps = list(forecast.buckets.keys())
        probs = list(forecast.buckets.values())

        mean_temp = sum(t * p for t, p in zip(temps, probs))

        # Historical average for this date (simplified - should use actual climatology)
        # Assume "normal" temperature is around the forecast mean for this calculation
        normal_temp = mean_temp  # Simplified baseline

        # Check for extreme tails
        extreme_high = mean_temp + self.ANOMALY_THRESHOLD_STD * forecast.ensemble_spread
        extreme_low = mean_temp - self.ANOMALY_THRESHOLD_STD * forecast.ensemble_spread

        # Calculate probability of extreme temps
        prob_extreme_high = sum(
            p for t, p in zip(temps, probs) if t >= extreme_high
        )
        prob_extreme_low = sum(
            p for t, p in zip(temps, probs) if t <= extreme_low
        )

        # Detect anomaly if ensemble shows significant extreme probability
        max_prob = max(prob_extreme_high, prob_extreme_low)

        if max_prob >= 0.15:  # 15%+ chance of extreme = anomaly
            impact = forecast.ensemble_spread * self.ANOMALY_THRESHOLD_STD

            return WeatherEvent(
                event_type=EventType.TEMPERATURE_ANOMALY,
                city=city,
                target_date=forecast.date,
                detected_at=datetime.now(),
                confidence=min(1.0, max_prob / 0.30),  # Scale confidence
                expected_impact=impact,
                trade_window_hours=12 if max_prob < 0.25 else 6,
                metadata={
                    "mean_temp": mean_temp,
                    "spread": forecast.ensemble_spread,
                    "extreme_prob": max_prob,
                    "direction": "high" if prob_extreme_high > prob_extreme_low else "low",
                },
            )

        return None

    async def get_noaa_alerts(self, city: str) -> list[NOAAAlert]:
        """Fetch NOAA weather alerts for a city.

        Args:
            city: City name (e.g., "NYC", "Miami")

        Returns:
            List of active alerts
        """
        # Map cities to NOAA zones (simplified - should use actual zone IDs)
        city_zones = {
            "NYC": "NYZ072",
            "Miami": "FLZ173",
            "Chicago": "ILZ014",
            "LA": "CAZ041",
            "Boston": "MAZ015",
        }

        zone = city_zones.get(city)
        if not zone:
            return []

        try:
            async with self.client:
                response = await self.client.get(
                    f"{self._noaa_base_url}/alerts/active/zone/{zone}",
                    headers={"User-Agent": "wedge-bot/1.0"},
                )
                response.raise_for_status()
                data = response.json()
        except Exception as e:
            log.warning("noaa_api_failed", city=city, error=str(e))
            return []

        alerts = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})

            # Parse severity
            severity_str = props.get("severity", "minor").lower()
            severity_map = {
                "extreme": AlertSeverity.EXTREME,
                "severe": AlertSeverity.SEVERE,
                "moderate": AlertSeverity.MODERATE,
            }
            severity = severity_map.get(severity_str, AlertSeverity.MINOR)

            # Parse effective/expire times
            effective = props.get("effective")
            expires = props.get("ends") or props.get("expires")

            try:
                effective_dt = datetime.fromisoformat(effective.replace("Z", "+00:00")) if effective else datetime.now()
                expires_dt = datetime.fromisoformat(expires.replace("Z", "+00:00")) if expires else datetime.now()
            except (ValueError, AttributeError):
                effective_dt = datetime.now()
                expires_dt = datetime.now() + timedelta(hours=6)

            # Estimate temperature impact based on alert type
            alert_type = props.get("event", "")
            temp_impact = self._estimate_temp_impact(alert_type)

            alerts.append(NOAAAlert(
                alert_type=alert_type,
                severity=severity,
                headline=props.get("headline", ""),
                description=props.get("description", "")[:500],  # Truncate
                effective=effective_dt,
                expires=expires_dt,
                areas=[zone],
                temperature_impact=temp_impact,
            ))

        return alerts

    def _estimate_temp_impact(self, alert_type: str) -> float:
        """Estimate temperature impact from alert type."""
        impact_map = {
            "Heat Advisory": 8.0,
            "Excessive Heat Warning": 12.0,
            "Heat Watch": 6.0,
            "Cold Wave Warning": -10.0,
            "Wind Chill Warning": -15.0,
            "Freeze Warning": -5.0,
            "Hard Freeze Warning": -10.0,
            "Winter Storm Warning": -8.0,
            "Blizzard Warning": -15.0,
        }
        return impact_map.get(alert_type, 5.0)  # Default moderate impact


@dataclass
class EventSignal:
    """Trading signal generated from an event."""
    event: WeatherEvent
    city: str
    target_date: date
    direction: Literal["long", "short"]  # Long = bet on event, Short = bet against
    confidence: float
    expected_edge: float  # Estimated edge from event
    recommended_bet_pct: float  # Recommended bet as % of bankroll


def generate_event_signals(
    events: list[WeatherEvent],
    forecast: ForecastDistribution,
    market_prices: dict[int, float],  # temp_f → market price
) -> list[EventSignal]:
    """Generate trading signals from detected events.

    Args:
        events: Detected weather events
        forecast: Weather forecast
        market_prices: Current market prices by temperature

    Returns:
        List of trading signals
    """
    signals = []

    for event in events:
        if not event.is_active:
            continue

        if event.event_type == EventType.GFS_UPDATE:
            # GFS update = higher confidence in model
            # Increase Kelly fraction for all signals
            signals.append(EventSignal(
                event=event,
                city=event.city,
                target_date=event.target_date,
                direction="long",  # Trust model more
                confidence=0.9,
                expected_edge=0.02,  # 2% edge boost from fresher data
                recommended_bet_pct=0.04,  # 4% of bankroll max
            ))

        elif event.event_type == EventType.TEMPERATURE_ANOMALY:
            # Anomaly = market may underreact to extreme
            metadata = event.metadata or {}
            direction = metadata.get("direction", "high")

            # Calculate mean temperature from forecast distribution
            mean_temp = sum(t * p for t, p in forecast.buckets.items())

            # Find relevant market prices
            if direction == "high":
                # Look for high temp buckets
                relevant_temps = [t for t in market_prices.keys() if t > mean_temp]
            else:
                relevant_temps = [t for t in market_prices.keys() if t < mean_temp]

            if relevant_temps:
                signals.append(EventSignal(
                    event=event,
                    city=event.city,
                    target_date=event.target_date,
                    direction="long",
                    confidence=event.confidence,
                    expected_edge=event.confidence * 0.05,  # Scale edge with confidence
                    recommended_bet_pct=0.02 * event.confidence,
                ))

        elif event.event_type == EventType.WEATHER_ALERT:
            # Weather alerts = strong directional signal
            # Market often slow to price in breaking news
            signals.append(EventSignal(
                event=event,
                city=event.city,
                target_date=event.target_date,
                direction="long",
                confidence=min(0.95, event.confidence + 0.1),
                expected_edge=event.confidence * 0.08,  # Higher edge for alerts
                recommended_bet_pct=0.03 * event.confidence,
            ))

    return signals


class EventHistory:
    """Tracks historical event performance for calibration."""

    def __init__(self):
        self._events: list[tuple[WeatherEvent, float]] = []  # (event, actual_edge)

    def record_event(self, event: WeatherEvent, actual_edge: float) -> None:
        """Record an event and its actual edge."""
        self._events.append((event, actual_edge))

    def get_performance_by_type(self) -> dict[EventType, dict]:
        """Get performance statistics by event type."""
        by_type: dict[EventType, list[float]] = {}

        for event, edge in self._events:
            if event.event_type not in by_type:
                by_type[event.event_type] = []
            by_type[event.event_type].append(edge)

        results = {}
        for event_type, edges in by_type.items():
            if edges:
                import statistics
                results[event_type.value] = {
                    "count": len(edges),
                    "mean_edge": statistics.mean(edges),
                    "std_edge": statistics.stdev(edges) if len(edges) > 1 else 0.0,
                    "win_rate": sum(1 for e in edges if e > 0) / len(edges),
                }

        return results
