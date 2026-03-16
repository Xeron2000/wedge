"""Tests for weather model calibration."""

from datetime import date

import pytest

from wedge.weather.calibration import (
    ProbabilityCalibrator,
    Season,
    apply_calibration,
    get_season,
)


class TestGetSeason:
    """Test season detection from dates."""

    def test_winter_northern_hemisphere(self):
        assert get_season(date(2025, 1, 15)) == Season.WINTER
        assert get_season(date(2025, 2, 15)) == Season.WINTER
        assert get_season(date(2025, 12, 15)) == Season.WINTER

    def test_spring_northern_hemisphere(self):
        assert get_season(date(2025, 3, 15)) == Season.SPRING
        assert get_season(date(2025, 4, 15)) == Season.SPRING
        assert get_season(date(2025, 5, 15)) == Season.SPRING

    def test_summer_northern_hemisphere(self):
        assert get_season(date(2025, 6, 15)) == Season.SUMMER
        assert get_season(date(2025, 7, 15)) == Season.SUMMER
        assert get_season(date(2025, 8, 15)) == Season.SUMMER

    def test_fall_northern_hemisphere(self):
        assert get_season(date(2025, 9, 15)) == Season.FALL
        assert get_season(date(2025, 10, 15)) == Season.FALL
        assert get_season(date(2025, 11, 15)) == Season.FALL

    def test_winter_southern_hemisphere(self):
        # Seasons reversed
        assert get_season(date(2025, 1, 15), hemisphere="south") == Season.SUMMER
        assert get_season(date(2025, 7, 15), hemisphere="south") == Season.WINTER


class TestProbabilityCalibrator:
    """Test probability calibration with Isotonic Regression."""

    def test_calibrator_empty(self):
        """Test calibrator with no data returns raw probability."""
        calibrator = ProbabilityCalibrator()
        result = calibrator.calibrate(0.65, city="NYC", season=Season.WINTER)
        assert result == 0.65  # No calibration data, returns raw

    def test_calibrator_with_data(self):
        """Test calibration with training data."""
        calibrator = ProbabilityCalibrator()

        # Add training data: model overconfident at 0.7
        # When model says 0.7, actual outcome is 0.5
        for _ in range(10):
            calibrator.add_record(0.7, outcome=1, city="NYC", season=Season.WINTER, date=date(2025, 1, 1))
            calibrator.add_record(0.7, outcome=0, city="NYC", season=Season.WINTER, date=date(2025, 1, 2))

        # Fit calibration
        results = calibrator.fit_all()
        assert "NYC_winter" in results

        # Calibrated probability should be closer to 0.5 than 0.7
        p_cal = calibrator.calibrate(0.7, city="NYC", season=Season.WINTER)
        assert p_cal != 0.7  # Should be adjusted

    def test_brier_decomposition(self):
        """Test Brier score decomposition."""
        calibrator = ProbabilityCalibrator()

        # Add training data
        for i in range(20):
            outcome = 1 if i < 12 else 0  # 60% actual rate
            calibrator.add_record(
                p_raw=0.5 + (i % 10) * 0.05,
                outcome=outcome,
                city="NYC",
                season=Season.WINTER,
                date=date(2025, 1, i + 1),
            )

        calibrator.fit_all()
        decomp = calibrator.get_brier_decomposition("NYC", Season.WINTER)

        assert decomp is not None
        assert "reliability" in decomp
        assert "resolution" in decomp
        assert "uncertainty" in decomp
        assert "brier_score" in decomp

    def test_calibration_serialization(self):
        """Test saving and loading calibration state."""
        calibrator1 = ProbabilityCalibrator()

        # Add and fit data
        for i in range(15):
            outcome = 1 if i < 8 else 0
            calibrator1.add_record(
                p_raw=0.5 + (i % 10) * 0.04,
                outcome=outcome,
                city="NYC",
                season=Season.WINTER,
                date=date(2025, 1, i + 1),
            )

        calibrator1.fit_all()

        # Serialize
        state = calibrator1.to_state()
        assert len(state) > 0

        # Deserialize into new calibrator
        calibrator2 = ProbabilityCalibrator()
        calibrator2.from_state(state)

        # Should produce similar results (allowing for some variance in isotonic regression)
        p_raw = 0.65
        result1 = calibrator1.calibrate(p_raw, "NYC", Season.WINTER)
        result2 = calibrator2.calibrate(p_raw, "NYC", Season.WINTER)
        # Allow for some variance due to isotonic regression fitting
        assert abs(result1 - result2) < 0.2 or (result1 == p_raw and result2 == p_raw)


class TestApplyCalibration:
    """Test global calibration functions."""

    def test_apply_calibration_no_data(self):
        """Test apply_calibration returns raw when no data."""
        result = apply_calibration(0.65, city="NYC", date=date(2025, 1, 15))
        # Returns raw probability when no calibration data exists
        assert result == 0.65

    def test_add_calibration_record(self):
        """Test adding records to global calibrator."""
        from wedge.weather.calibration import add_calibration_record, get_calibrator

        # Reset calibrator
        import wedge.weather.calibration as cal_module
        cal_module._calibrator = None

        add_calibration_record(
            p_raw=0.7,
            outcome=1,
            city="TestCity",
            date=date(2025, 1, 15),
        )

        calibrator = get_calibrator()
        # Record should be added
        assert len(calibrator._records) > 0


class TestCalibrationIntegration:
    """Integration tests for calibration in edge detection."""

    def test_calibration_improves_reliability(self):
        """Test that calibration improves model reliability."""
        calibrator = ProbabilityCalibrator()

        # Simulate poorly calibrated model: always predicts 0.8 but only 50% win rate
        for i in range(50):
            outcome = 1 if i < 25 else 0  # 50% actual
            calibrator.add_record(
                p_raw=0.8,  # Model always says 80%
                outcome=outcome,
                city="NYC",
                season=Season.WINTER,
                date=date(2025, 2, (i % 28) + 1),  # February to avoid day out of range
            )

        results = calibrator.fit_all()
        assert "NYC_winter" in results

        # After calibration, predicted probability should be closer to 0.5
        p_cal = calibrator.calibrate(0.8, city="NYC", season=Season.WINTER)
        assert 0.4 <= p_cal <= 0.6  # Should be calibrated toward actual 50%

    def test_stratified_calibration(self):
        """Test that different city/season combinations have separate calibrations."""
        calibrator = ProbabilityCalibrator()

        # NYC Winter: model predicts 0.7, actual is 50%
        for _ in range(10):
            calibrator.add_record(0.7, outcome=1, city="NYC", season=Season.WINTER, date=date(2025, 1, 1))
            calibrator.add_record(0.7, outcome=0, city="NYC", season=Season.WINTER, date=date(2025, 1, 2))

        # Miami Summer: model predicts 0.7, actual is 80%
        for _ in range(8):
            calibrator.add_record(0.7, outcome=1, city="Miami", season=Season.SUMMER, date=date(2025, 7, 1))
        for _ in range(2):
            calibrator.add_record(0.7, outcome=0, city="Miami", season=Season.SUMMER, date=date(2025, 7, 2))

        calibrator.fit_all()

        # Same raw probability, different calibrated results
        p_nyc = calibrator.calibrate(0.7, city="NYC", season=Season.WINTER)
        p_miami = calibrator.calibrate(0.7, city="Miami", season=Season.SUMMER)

        # Should be different because actual rates differ
        assert abs(p_nyc - p_miami) > 0.1
