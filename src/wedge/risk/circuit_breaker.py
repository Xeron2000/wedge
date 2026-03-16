"""Circuit breaker system for risk management.

Multi-layer circuit breakers to prevent catastrophic losses:
1. Daily loss limit
2. Weekly loss limit
3. Maximum drawdown
4. Consecutive losses
5. Brier score degradation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Literal

from wedge.log import get_logger

log = get_logger("risk.circuit_breaker")


class BreakerLevel(Enum):
    """Circuit breaker severity levels."""
    GREEN = "green"      # Normal operation
    YELLOW = "yellow"    # Warning - approaching limits
    RED = "red"          # Halted - limit hit


@dataclass
class BreakerState:
    """Current state of a single circuit breaker."""
    level: BreakerLevel
    current_value: float
    limit_value: float
    utilization: float  # current / limit
    triggered_at: datetime | None = None
    message: str = ""


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breakers."""
    # Loss limits (as % of bankroll)
    daily_loss_limit: float = 0.05      # 5% daily loss → halt
    weekly_loss_limit: float = 0.10     # 10% weekly loss → halt
    monthly_loss_limit: float = 0.15    # 15% monthly loss → halt

    # Drawdown limit
    max_drawdown: float = 0.15          # 15% peak-to-trough drawdown → halt

    # Consecutive losses
    max_consecutive_losses: int = 5     # 5 consecutive losses → halt

    # Brier score threshold
    brier_score_threshold: float = 0.25  # Brier > 0.25 → halt

    # Warning thresholds (when to trigger YELLOW)
    warning_threshold: float = 0.80     # 80% of limit → warning


class CircuitBreaker:
    """Multi-layer circuit breaker system.

    Usage:
        breaker = CircuitBreaker(initial_bankroll=1000)

        # After each trade
        breaker.record_trade(pnl=-50, p_model=0.65, outcome=0)

        # Check if trading is allowed
        if not breaker.can_trade():
            print("Trading halted!")
            print(breaker.get_halt_reason())

        # Get current state
        state = breaker.get_state()
    """

    def __init__(self, config: CircuitBreakerConfig | None = None):
        self.config = config or CircuitBreakerConfig()
        self._initial_bankroll = 0.0
        self._peak_bankroll = 0.0
        self._current_bankroll = 0.0

        # Daily tracking
        self._daily_pnl = 0.0
        self._last_reset_date: date | None = None

        # Weekly tracking
        self._weekly_pnl = 0.0
        self._week_start: date | None = None

        # Consecutive losses
        self._consecutive_losses = 0

        # Brier score tracking
        self._brier_predictions: list[tuple[float, int]] = []  # (p_model, outcome)

        # Halt state
        self._halted = False
        self._halt_reason: str = ""
        self._halted_at: datetime | None = None

    def initialize(self, bankroll: float) -> None:
        """Initialize with starting bankroll."""
        self._initial_bankroll = bankroll
        self._current_bankroll = bankroll
        self._peak_bankroll = bankroll
        log.info("circuit_breaker_initialized", bankroll=bankroll)

    def update_bankroll(self, bankroll: float) -> None:
        """Update current bankroll and check drawdown."""
        self._current_bankroll = bankroll

        # Update peak if new high
        if bankroll > self._peak_bankroll:
            self._peak_bankroll = bankroll

        self._check_drawdown()

    def record_trade(
        self,
        pnl: float,
        p_model: float | None = None,
        outcome: int | None = None,
    ) -> None:
        """Record a trade and update all limits."""
        now = datetime.now()
        today = now.date()

        # Reset daily/weekly counters if new period
        self._reset_if_new_period(today)

        # Update P&L
        self._daily_pnl += pnl
        self._weekly_pnl += pnl

        # Update consecutive losses
        if pnl < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        # Update Brier score tracking
        if p_model is not None and outcome is not None:
            self._brier_predictions.append((p_model, outcome))

        # Check all breakers
        self._check_daily_loss()
        self._check_weekly_loss()
        self._check_consecutive_losses()
        self._check_brier_score()

        # Log if halted
        if self._halted:
            log.warning("trading_halted", reason=self._halt_reason, daily_pnl=self._daily_pnl)

    def can_trade(self) -> bool:
        """Check if trading is allowed."""
        if self._halted:
            return False

        # Run all checks
        self._check_daily_loss()
        self._check_weekly_loss()
        self._check_drawdown()
        self._check_consecutive_losses()
        self._check_brier_score()

        return not self._halted

    def get_halt_reason(self) -> str | None:
        """Get reason for trading halt."""
        return self._halt_reason if self._halted else None

    def get_state(self) -> dict[str, BreakerState]:
        """Get current state of all circuit breakers."""
        states = {}

        # Daily loss
        daily_util = abs(self._daily_pnl) / (self._initial_bankroll * self.config.daily_loss_limit) if self._initial_bankroll > 0 else 0
        states["daily_loss"] = BreakerState(
            level=self._get_level(daily_util),
            current_value=self._daily_pnl,
            limit_value=self._initial_bankroll * self.config.daily_loss_limit,
            utilization=min(daily_util, 1.0),
            message=f"Daily P&L: ${self._daily_pnl:.2f}"
        )

        # Weekly loss
        weekly_util = abs(self._weekly_pnl) / (self._initial_bankroll * self.config.weekly_loss_limit) if self._initial_bankroll > 0 else 0
        states["weekly_loss"] = BreakerState(
            level=self._get_level(weekly_util),
            current_value=self._weekly_pnl,
            limit_value=self._initial_bankroll * self.config.weekly_loss_limit,
            utilization=min(weekly_util, 1.0),
            message=f"Weekly P&L: ${self._weekly_pnl:.2f}"
        )

        # Drawdown
        drawdown = (self._peak_bankroll - self._current_bankroll) / self._peak_bankroll if self._peak_bankroll > 0 else 0
        drawdown_util = drawdown / self.config.max_drawdown if self.config.max_drawdown > 0 else 0
        states["drawdown"] = BreakerState(
            level=self._get_level(drawdown_util),
            current_value=drawdown,
            limit_value=self.config.max_drawdown,
            utilization=min(drawdown_util, 1.0),
            message=f"Drawdown: {drawdown*100:.1f}%"
        )

        # Consecutive losses
        consec_util = self._consecutive_losses / self.config.max_consecutive_losses
        states["consecutive_losses"] = BreakerState(
            level=self._get_level(consec_util),
            current_value=float(self._consecutive_losses),
            limit_value=float(self.config.max_consecutive_losses),
            utilization=min(consec_util, 1.0),
            message=f"Consecutive losses: {self._consecutive_losses}"
        )

        # Brier score
        brier = self._calculate_brier_score()
        brier_util = brier / self.config.brier_score_threshold if self.config.brier_score_threshold > 0 else 0
        states["brier_score"] = BreakerState(
            level=self._get_level(brier_util, invert=True),  # Lower is better
            current_value=brier,
            limit_value=self.config.brier_score_threshold,
            utilization=min(brier_util, 1.0),
            message=f"Brier score: {brier:.4f}"
        )

        return states

    def reset(self) -> None:
        """Manually reset circuit breaker (e.g., after review)."""
        self._halted = False
        self._halt_reason = ""
        self._halted_at = None
        log.info("circuit_breaker_reset")

    def _get_level(self, utilization: float, invert: bool = False) -> BreakerLevel:
        """Get breaker level from utilization."""
        if invert:
            # For metrics where lower is better (like Brier)
            if utilization <= 0.5:
                return BreakerLevel.GREEN
            elif utilization <= 0.8:
                return BreakerLevel.YELLOW
            else:
                return BreakerLevel.RED
        else:
            # For metrics where higher is worse (like losses)
            if utilization < self.config.warning_threshold:
                return BreakerLevel.GREEN
            elif utilization < 1.0:
                return BreakerLevel.YELLOW
            else:
                return BreakerLevel.RED

    def _reset_if_new_period(self, today: date) -> None:
        """Reset daily/weekly counters if new period."""
        # Daily reset
        if self._last_reset_date is None or today > self._last_reset_date:
            self._daily_pnl = 0.0
            self._last_reset_date = today

        # Weekly reset (Monday)
        if self._week_start is None or today >= self._week_start + timedelta(days=7):
            self._weekly_pnl = 0.0
            self._week_start = today - timedelta(days=today.weekday())  # Last Monday

    def _check_daily_loss(self) -> None:
        """Check daily loss limit."""
        limit = self._initial_bankroll * self.config.daily_loss_limit
        if self._daily_pnl <= -limit:
            self._halt(f"Daily loss limit hit: ${self._daily_pnl:.2f} <= -${limit:.2f}")

    def _check_weekly_loss(self) -> None:
        """Check weekly loss limit."""
        limit = self._initial_bankroll * self.config.weekly_loss_limit
        if self._weekly_pnl <= -limit:
            self._halt(f"Weekly loss limit hit: ${self._weekly_pnl:.2f} <= -${limit:.2f}")

    def _check_drawdown(self) -> None:
        """Check maximum drawdown."""
        if self._peak_bankroll <= 0:
            return

        drawdown = (self._peak_bankroll - self._current_bankroll) / self._peak_bankroll
        if drawdown >= self.config.max_drawdown:
            self._halt(f"Max drawdown hit: {drawdown*100:.1f}% >= {self.config.max_drawdown*100:.1f}%")

    def _check_consecutive_losses(self) -> None:
        """Check consecutive losses."""
        if self._consecutive_losses >= self.config.max_consecutive_losses:
            self._halt(f"Consecutive losses: {self._consecutive_losses} >= {self.config.max_consecutive_losses}")

    def _check_brier_score(self) -> None:
        """Check Brier score degradation."""
        if len(self._brier_predictions) < 10:  # Need minimum samples
            return

        brier = self._calculate_brier_score()
        if brier >= self.config.brier_score_threshold:
            self._halt(f"Brier score too high: {brier:.4f} >= {self.config.brier_score_threshold}")

    def _calculate_brier_score(self) -> float:
        """Calculate Brier score from predictions."""
        if not self._brier_predictions:
            return 0.0

        sum_squared_error = sum(
            (p - o) ** 2 for p, o in self._brier_predictions
        )
        return sum_squared_error / len(self._brier_predictions)

    def _halt(self, reason: str) -> None:
        """Trigger trading halt."""
        self._halted = True
        self._halt_reason = reason
        self._halted_at = datetime.now()
        log.error("circuit_breaker_triggered", reason=reason)


@dataclass
class RiskMetrics:
    """Current risk metrics snapshot."""
    daily_pnl: float
    weekly_pnl: float
    current_bankroll: float
    peak_bankroll: float
    drawdown: float
    consecutive_losses: int
    brier_score: float | None
    can_trade: bool
    halt_reason: str | None


def get_risk_metrics(breaker: CircuitBreaker) -> RiskMetrics:
    """Get current risk metrics from circuit breaker."""
    state = breaker.get_state()

    drawdown = (breaker._peak_bankroll - breaker._current_bankroll) / breaker._peak_bankroll if breaker._peak_bankroll > 0 else 0
    brier = breaker._calculate_brier_score() if len(breaker._brier_predictions) >= 10 else None

    return RiskMetrics(
        daily_pnl=state["daily_loss"].current_value,
        weekly_pnl=state["weekly_loss"].current_value,
        current_bankroll=breaker._current_bankroll,
        peak_bankroll=breaker._peak_bankroll,
        drawdown=drawdown,
        consecutive_losses=state["consecutive_losses"].current_value,
        brier_score=brier,
        can_trade=breaker.can_trade(),
        halt_reason=breaker.get_halt_reason(),
    )
