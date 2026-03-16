"""Portfolio correlation tracking and risk exposure calculation.

Tracks temperature correlations between cities and calculates portfolio-level
risk exposure using Value-at-Risk (VaR) methodology.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np

from wedge.log import get_logger
from wedge.market.models import Position

log = get_logger("risk.correlation")


# Climate region mapping for correlation estimation
# Cities in same region have higher correlation
CLIMATE_REGIONS = {
    # Northeast US
    "NYC": "northeast_us",
    "Boston": "northeast_us",
    "Philadelphia": "northeast_us",
    "Washington DC": "northeast_us",

    # South US
    "Miami": "south_us",
    "Atlanta": "south_us",
    "Dallas": "south_us",
    "Houston": "south_us",

    # Midwest US
    "Chicago": "midwest_us",
    "Detroit": "midwest_us",
    "Minneapolis": "midwest_us",

    # West US
    "LA": "west_us",
    "San Francisco": "west_us",
    "Seattle": "west_us",
    "Denver": "west_us",
    "Phoenix": "west_us",

    # Europe
    "London": "uk_nw_europe",
    "Paris": "nw_europe",
    "Berlin": "central_europe",

    # Asia
    "Seoul": "east_asia",
    "Shanghai": "east_asia",
    "Tokyo": "east_asia",

    # Oceania
    "Wellington": "oceania",
    "Sydney": "oceania",
}

# Base correlation matrix (based on historical temperature correlations)
# These are approximate values - should be calibrated with actual data
BASE_CORRELATIONS = {
    # Same region = high correlation
    ("northeast_us", "northeast_us"): 0.85,
    ("south_us", "south_us"): 0.80,
    ("midwest_us", "midwest_us"): 0.85,
    ("west_us", "west_us"): 0.70,  # West US more diverse climate
    ("uk_nw_europe", "uk_nw_europe"): 0.75,
    ("east_asia", "east_asia"): 0.70,
    ("oceania", "oceania"): 0.80,

    # Cross-region correlations
    ("northeast_us", "midwest_us"): 0.60,
    ("northeast_us", "south_us"): 0.30,
    ("northeast_us", "west_us"): 0.10,
    ("northeast_us", "uk_nw_europe"): 0.40,  # North Atlantic connection

    ("south_us", "midwest_us"): 0.50,
    ("south_us", "west_us"): 0.40,
    ("south_us", "east_asia"): 0.20,

    ("midwest_us", "west_us"): 0.20,
    ("midwest_us", "uk_nw_europe"): 0.30,

    ("west_us", "east_asia"): 0.30,  # Pacific connection

    ("uk_nw_europe", "central_europe"): 0.60,
    ("uk_nw_europe", "east_asia"): 0.10,

    ("east_asia", "oceania"): 0.15,

    # Default for unspecified pairs
    ("default", "default"): 0.05,  # Slight positive correlation (global climate)
}


@dataclass
class CorrelationMatrix:
    """Correlation matrix for portfolio cities."""
    cities: list[str]
    matrix: np.ndarray  # N x N correlation matrix

    def get_correlation(self, city1: str, city2: str) -> float:
        """Get correlation between two cities."""
        if city1 == city2:
            return 1.0

        try:
            i = self.cities.index(city1)
            j = self.cities.index(city2)
            return float(self.matrix[i, j])
        except (ValueError, IndexError):
            return 0.0

    def get_portfolio_variance(self, exposures: dict[str, float]) -> float:
        """Calculate portfolio variance given city exposures.

        Uses the formula: σ²_p = w'Σw
        where w = exposure vector, Σ = covariance matrix

        Args:
            exposures: Map of city → dollar exposure

        Returns:
            Portfolio variance (in dollars squared)
        """
        # Build exposure vector
        n = len(self.cities)
        w = np.zeros(n)
        for i, city in enumerate(self.cities):
            w[i] = exposures.get(city, 0.0)

        # Convert correlation to covariance
        # Cov(i,j) = Corr(i,j) * σ_i * σ_j
        # For simplicity, assume σ_i = 1 for all i (standardized)
        cov_matrix = self.matrix.copy()

        # Portfolio variance = w' * Σ * w
        variance = float(w.T @ cov_matrix @ w)
        return max(0.0, variance)  # Ensure non-negative


@dataclass
class RiskExposure:
    """Portfolio risk exposure metrics."""
    total_exposure: float  # Total dollar exposure
    var_95: float  # 95% Value-at-Risk
    var_99: float  # 99% Value-at-Risk
    max_single_city: float  # Largest single city exposure
    max_single_city_pct: float  # Largest exposure as % of total
    correlation_adjusted_exposure: float  # Exposure adjusted for correlations
    diversification_ratio: float  # 1.0 = fully diversified, 0.0 = no diversification


def get_city_region(city: str) -> str | None:
    """Get climate region for a city."""
    return CLIMATE_REGIONS.get(city)


def get_base_correlation(city1: str, city2: str) -> float:
    """Get base correlation between two cities based on climate regions."""
    if city1 == city2:
        return 1.0

    region1 = get_city_region(city1)
    region2 = get_city_region(city2)

    if region1 is None or region2 is None:
        return BASE_CORRELATIONS.get(("default", "default"), 0.05)

    # Try exact match first
    key = (region1, region2)
    if key in BASE_CORRELATIONS:
        return BASE_CORRELATIONS[key]

    # Try reverse
    key = (region2, region1)
    if key in BASE_CORRELATIONS:
        return BASE_CORRELATIONS[key]

    # Default
    return BASE_CORRELATIONS.get(("default", "default"), 0.05)


def build_correlation_matrix(cities: list[str]) -> CorrelationMatrix:
    """Build correlation matrix for a list of cities.

    Args:
        cities: List of city names

    Returns:
        CorrelationMatrix with pairwise correlations
    """
    n = len(cities)
    matrix = np.eye(n)  # Start with identity (1.0 on diagonal)

    for i in range(n):
        for j in range(i + 1, n):
            corr = get_base_correlation(cities[i], cities[j])
            matrix[i, j] = corr
            matrix[j, i] = corr  # Symmetric

    # Ensure positive semi-definite (required for valid covariance)
    # Use nearest PSD matrix if needed
    matrix = _nearest_positive_semidefinite(matrix)

    return CorrelationMatrix(cities=cities, matrix=matrix)


def _nearest_positive_semidefinite(matrix: np.ndarray) -> np.ndarray:
    """Find nearest positive semi-definite matrix.

    Uses Higham's algorithm to find the nearest correlation matrix
    that is positive semi-definite.
    """
    # Make symmetric
    B = (matrix + matrix.T) / 2

    # Eigenvalue decomposition
    eigenvalues, eigenvectors = np.linalg.eigh(B)

    # Set negative eigenvalues to zero
    eigenvalues = np.maximum(eigenvalues, 0)

    # Reconstruct
    matrix_psd = eigenvectors @ np.diag(eigenvalues) @ eigenvectors.T

    # Ensure diagonal is 1.0 (correlation matrix property)
    d = np.sqrt(np.diag(matrix_psd))
    if np.all(d > 0):
        matrix_psd = matrix_psd / np.outer(d, d)

    return matrix_psd


def calculate_portfolio_exposure(
    positions: list[Position],
    correlation_matrix: CorrelationMatrix | None = None,
) -> RiskExposure:
    """Calculate portfolio risk exposure.

    Args:
        positions: List of current positions
        correlation_matrix: Optional pre-computed correlation matrix

    Returns:
        RiskExposure with detailed metrics
    """
    if not positions:
        return RiskExposure(
            total_exposure=0.0,
            var_95=0.0,
            var_99=0.0,
            max_single_city=0.0,
            max_single_city_pct=0.0,
            correlation_adjusted_exposure=0.0,
            diversification_ratio=1.0,
        )

    # Build exposure map by city
    exposures: dict[str, float] = {}
    for pos in positions:
        city = pos.bucket.city
        exposures[city] = exposures.get(city, 0.0) + pos.size

    total_exposure = sum(exposures.values())

    # Find max single city exposure
    max_single_city = max(exposures.values()) if exposures else 0.0
    max_single_city_pct = max_single_city / total_exposure if total_exposure > 0 else 0.0

    # Build or use correlation matrix
    if correlation_matrix is None:
        cities = list(exposures.keys())
        correlation_matrix = build_correlation_matrix(cities)

    # Calculate portfolio variance
    # Assume each position has σ = 1 (standardized for binary options)
    # In reality, tail positions have higher volatility
    city_list = list(exposures.keys())

    # Filter exposures to only include cities in matrix
    filtered_exposures = {c: exposures[c] for c in city_list if c in correlation_matrix.cities}

    # Recalculate with filtered cities if needed
    if len(filtered_exposures) != len(exposures):
        correlation_matrix = build_correlation_matrix(city_list)

    variance = correlation_matrix.get_portfolio_variance(filtered_exposures)
    std_dev = np.sqrt(variance)

    # Value-at-Risk (parametric)
    # VaR = μ - z * σ, assume μ = 0 for short-term
    var_95 = 1.645 * std_dev  # 95% confidence
    var_99 = 2.326 * std_dev  # 99% confidence

    # Correlation-adjusted exposure
    # If perfectly correlated: adjusted = total
    # If uncorrelated: adjusted = sqrt(sum of squares)
    uncorrelated_exposure = np.sqrt(sum(e ** 2 for e in exposures.values()))
    correlation_adjusted_exposure = std_dev  # This is the correlation-adjusted exposure

    # Diversification ratio
    # 1.0 = fully diversified (uncorrelated)
    # 0.0 = no diversification (perfectly correlated)
    if total_exposure > 0:
        diversification_ratio = 1.0 - (correlation_adjusted_exposure / total_exposure)
    else:
        diversification_ratio = 1.0

    return RiskExposure(
        total_exposure=total_exposure,
        var_95=var_95,
        var_99=var_99,
        max_single_city=max_single_city,
        max_single_city_pct=max_single_city_pct,
        correlation_adjusted_exposure=correlation_adjusted_exposure,
        diversification_ratio=max(0.0, min(1.0, diversification_ratio)),
    )


def check_correlation_limit(
    positions: list[Position],
    new_city: str,
    max_correlated_exposure: float = 500.0,
    max_correlation: float = 0.7,
) -> bool:
    """Check if adding a new position would exceed correlated exposure limit.

    Args:
        positions: Existing positions
        new_city: City of the new position
        max_correlated_exposure: Maximum exposure to highly correlated positions
        max_correlation: Correlation threshold

    Returns:
        True if position is allowed, False if would exceed limit
    """
    # Get existing exposures
    exposures: dict[str, float] = {}
    for pos in positions:
        exposures[pos.bucket.city] = exposures.get(pos.bucket.city, 0.0) + pos.size

    # Find cities highly correlated with new_city
    correlated_exposure = 0.0
    for city, exposure in exposures.items():
        corr = get_base_correlation(city, new_city)
        if corr >= max_correlation:
            correlated_exposure += exposure

    # Check limit
    if correlated_exposure >= max_correlated_exposure:
        log.warning(
            "correlation_limit_exceeded",
            new_city=new_city,
            correlated_exposure=correlated_exposure,
            limit=max_correlated_exposure,
        )
        return False

    return True


def get_diversification_score(positions: list[Position]) -> float:
    """Calculate portfolio diversification score (0-100).

    Higher score = better diversified.

    Scoring:
    - Number of cities (max 25 points)
    - Regional diversity (max 25 points)
    - Correlation-adjusted exposure (max 25 points)
    - Single-city concentration (max 25 points)
    """
    if not positions:
        return 100.0  # No positions = no risk

    # Count unique cities
    cities = set(pos.bucket.city for pos in positions)
    city_score = min(25, len(cities) * 5)  # 5 cities = max score

    # Count unique regions
    regions = set(get_city_region(c) for c in cities if get_city_region(c))
    region_score = min(25, len(regions) * 8)  # 3+ regions = max score

    # Correlation-adjusted exposure
    exposure = calculate_portfolio_exposure(positions)
    corr_score = 25 * exposure.diversification_ratio

    # Single-city concentration penalty
    if exposure.total_exposure > 0:
        concentration = exposure.max_single_city / exposure.total_exposure
        concentration_score = 25 * (1.0 - concentration)
    else:
        concentration_score = 25

    total_score = city_score + region_score + corr_score + concentration_score
    return round(total_score, 1)
