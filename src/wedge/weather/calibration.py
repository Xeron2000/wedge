"""Model probability calibration using Isotonic Regression.

Calibration aligns model-predicted probabilities with actual observed frequencies.
This is CRITICAL for accurate EV calculations.

Uses Isotonic Regression (non-parametric, monotonic) for robust calibration.
Stratified by city and season for climate-specific calibration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import TypedDict

import numpy as np
from sklearn.isotonic import IsotonicRegression


class Season(Enum):
    """Meteorological seasons (Northern Hemisphere)."""
    WINTER = "winter"  # Dec-Feb
    SPRING = "spring"  # Mar-May
    SUMMER = "summer"  # Jun-Aug
    FALL = "fall"  # Sep-Nov


def get_season(d: date, hemisphere: str = "north") -> Season:
    """Get meteorological season from date."""
    month = d.month
    if hemisphere == "south":
        # Reverse seasons for Southern Hemisphere
        month = (month + 6 - 1) % 12 + 1

    if month in (12, 1, 2):
        return Season.WINTER
    elif month in (3, 4, 5):
        return Season.SPRING
    elif month in (6, 7, 8):
        return Season.SUMMER
    else:
        return Season.FALL


@dataclass
class CalibrationRecord:
    """Single calibration data point."""
    p_raw: float  # Raw model probability
    outcome: int  # 1 if event occurred, 0 otherwise
    city: str
    season: Season
    date: date


@dataclass
class CalibrationCurve:
    """Fitted calibration curve for a city/season combination."""
    city: str
    season: Season
    ir_model: IsotonicRegression
    sample_count: int = 0
    brier_reliability: float = 0.0

    def calibrate(self, p_raw: float) -> float:
        """Apply calibration to raw probability."""
        if p_raw < 0 or p_raw > 1:
            return p_raw
        try:
            return float(self.ir_model.predict([p_raw])[0])
        except Exception:
            return p_raw  # Fallback to raw if model fails


class CalibrationState(TypedDict):
    """Serialized calibration state for persistence."""
    city: str
    season: str
    thresholds: list[float]  # Isotonic regression thresholds
    values: list[float]  # Isotonic regression values
    sample_count: int
    brier_reliability: float


class ProbabilityCalibrator:
    """Manages calibration curves for all city/season combinations.

    Usage:
        calibrator = ProbabilityCalibrator()

        # Add training data
        calibrator.add_record(0.65, outcome=1, city="NYC", season=Season.WINTER)

        # Fit models
        calibrator.fit_all()

        # Apply calibration
        p_calibrated = calibrator.calibrate(0.65, city="NYC", season=Season.WINTER)

        # Get Brier decomposition
        decomp = calibrator.get_brier_decomposition(city="NYC", season=Season.WINTER)
    """

    def __init__(self) -> None:
        self._records: list[CalibrationRecord] = []
        self._curves: dict[tuple[str, Season], CalibrationCurve] = {}

    def add_record(
        self,
        p_raw: float,
        outcome: int,
        city: str,
        season: Season,
        date: date,
    ) -> None:
        """Add a calibration data point."""
        self._records.append(CalibrationRecord(
            p_raw=p_raw,
            outcome=outcome,
            city=city,
            season=season,
            date=date,
        ))

    def fit_all(self) -> dict[str, int]:
        """Fit calibration curves for all city/season combinations.

        Returns dict mapping (city, season) -> sample count.
        """
        # Group records by city/season
        grouped: dict[tuple[str, Season], list[CalibrationRecord]] = {}
        for rec in self._records:
            key = (rec.city, rec.season)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(rec)

        # Fit Isotonic Regression for each group
        results = {}
        for (city, season), records in grouped.items():
            if len(records) < 10:  # Minimum samples for reliable fit
                continue

            # Sort by p_raw for Isotonic Regression
            sorted_recs = sorted(records, key=lambda r: r.p_raw)
            X = [r.p_raw for r in sorted_recs]
            y = [r.outcome for r in sorted_recs]

            # Fit Isotonic Regression
            ir = IsotonicRegression(out_of_bounds="clip")
            ir.fit(X, y)

            # Calculate Brier reliability (lower is better)
            predictions = ir.predict(X)
            reliability = np.mean((predictions - np.array(y)) ** 2)

            self._curves[(city, season)] = CalibrationCurve(
                city=city,
                season=season,
                ir_model=ir,
                sample_count=len(records),
                brier_reliability=reliability,
            )
            results[f"{city}_{season.value}"] = len(records)

        return results

    def calibrate(
        self,
        p_raw: float,
        city: str,
        season: Season,
    ) -> float:
        """Apply calibration to raw probability.

        Falls back to raw probability if no calibration curve exists.
        """
        key = (city, season)
        if key not in self._curves:
            return p_raw  # No calibration data, return raw

        return self._curves[key].calibrate(p_raw)

    def get_curve(self, city: str, season: Season) -> CalibrationCurve | None:
        """Get calibration curve for city/season."""
        return self._curves.get((city, season))

    def get_brier_decomposition(
        self,
        city: str,
        season: Season,
    ) -> dict[str, float] | None:
        """Get Brier score decomposition for a city/season.

        Returns:
            Dict with reliability, resolution, uncertainty components.
            None if insufficient data.
        """
        key = (city, season)
        if key not in self._curves:
            return None

        curve = self._curves[key]
        records = [r for r in self._records if r.city == city and r.season == season]

        if len(records) < 10:
            return None

        # Extract data
        p_raw = np.array([r.p_raw for r in records])
        outcomes = np.array([r.outcome for r in records])

        # Get calibrated predictions
        try:
            p_cal = curve.ir_model.predict(p_raw)
        except Exception:
            return None

        # Brier decomposition
        # Brier = Reliability - Resolution + Uncertainty
        base_rate = np.mean(outcomes)

        # Reliability: calibration accuracy (lower = better)
        reliability = np.mean((p_cal - outcomes) ** 2)

        # Resolution: ability to distinguish outcomes (higher = better)
        # Group by predicted probability buckets
        buckets = np.digitize(p_cal, bins=[0.2, 0.4, 0.6, 0.8])
        resolution = 0.0
        for b in range(5):
            mask = buckets == b
            if np.sum(mask) > 0:
                bucket_rate = np.mean(outcomes[mask])
                resolution += np.sum(mask) * (bucket_rate - base_rate) ** 2
        resolution /= len(outcomes)

        # Uncertainty: inherent unpredictability
        uncertainty = base_rate * (1 - base_rate)

        return {
            "reliability": float(reliability),
            "resolution": float(resolution),
            "uncertainty": float(uncertainty),
            "brier_score": float(reliability - resolution + uncertainty),
        }

    def to_state(self) -> list[CalibrationState]:
        """Serialize calibration curves for storage."""
        state = []
        for (city, season), curve in self._curves.items():
            # Handle different sklearn versions
            thresholds = getattr(curve.ir_model, 'X_thresholds_', None)
            values = getattr(curve.ir_model, 'y_thresholds_', None)
            if thresholds is None:
                thresholds = getattr(curve.ir_model, 'thresholds_', [])
            if values is None:
                values = getattr(curve.ir_model, 'y_', [])

            state.append({
                "city": city,
                "season": season.value,
                "thresholds": thresholds.tolist() if hasattr(thresholds, 'tolist') else list(thresholds),
                "values": values.tolist() if hasattr(values, 'tolist') else list(values),
                "sample_count": curve.sample_count,
                "brier_reliability": curve.brier_reliability,
            })
        return state

    def from_state(self, state: list[CalibrationState]) -> None:
        """Restore calibration curves from serialized state."""
        for item in state:
            season = Season(item["season"])
            key = (item["city"], season)

            ir = IsotonicRegression(out_of_bounds="clip")
            # Handle different sklearn versions
            if hasattr(ir, 'X_thresholds_'):
                ir.X_thresholds_ = np.array(item["thresholds"])
                ir.y_thresholds_ = np.array(item["values"])
            else:
                ir.thresholds_ = np.array(item["thresholds"])
                ir.y_ = np.array(item["values"])

            self._curves[key] = CalibrationCurve(
                city=item["city"],
                season=season,
                ir_model=ir,
                sample_count=item["sample_count"],
                brier_reliability=item["brier_reliability"],
            )


# Global calibrator instance (lazy initialized)
_calibrator: ProbabilityCalibrator | None = None


def get_calibrator() -> ProbabilityCalibrator:
    """Get global calibrator instance."""
    global _calibrator
    if _calibrator is None:
        _calibrator = ProbabilityCalibrator()
    return _calibrator


def apply_calibration(
    p_raw: float,
    city: str,
    date: date,
    hemisphere: str = "north",
) -> float:
    """Apply calibration to raw model probability.

    Convenience function using global calibrator.
    """
    season = get_season(date, hemisphere)
    return get_calibrator().calibrate(p_raw, city, season)


def add_calibration_record(
    p_raw: float,
    outcome: int,
    city: str,
    date: date,
    hemisphere: str = "north",
) -> None:
    """Add calibration record to global calibrator."""
    season = get_season(date, hemisphere)
    get_calibrator().add_record(p_raw, outcome, city, season, date)
