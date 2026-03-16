"""Risk management module.

Provides circuit breakers, correlation tracking, and portfolio risk analysis.
"""

from wedge.risk.circuit_breaker import (
    BreakerLevel,
    BreakerState,
    CircuitBreaker,
    CircuitBreakerConfig,
    RiskMetrics,
    get_risk_metrics,
)
from wedge.risk.correlation import (
    CorrelationMatrix,
    RiskExposure,
    build_correlation_matrix,
    calculate_portfolio_exposure,
    check_correlation_limit,
    get_diversification_score,
)

__all__ = [
    # Circuit breaker
    "BreakerLevel",
    "BreakerState",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "RiskMetrics",
    "get_risk_metrics",
    # Correlation
    "CorrelationMatrix",
    "RiskExposure",
    "build_correlation_matrix",
    "calculate_portfolio_exposure",
    "check_correlation_limit",
    "get_diversification_score",
]
