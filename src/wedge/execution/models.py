from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel


class OrderRequest(BaseModel):
    run_id: str
    token_id: str
    city: str
    date: date
    temp_value: int  # Temperature value (same unit as market)
    temp_unit: str  # "F" or "C"
    strategy: Literal["ladder"]
    side: Literal["buy"] = "buy"
    limit_price: float
    size: float  # USD amount
    p_model: float = 0.0
    p_market: float = 0.0
    edge: float = 0.0


class OrderResult(BaseModel):
    success: bool
    order_id: str | None = None
    filled_price: float | None = None
    filled_size: float | None = None
    error: str | None = None


@dataclass
class PositionPnL:
    """Real-time P&L for a single position."""

    token_id: str
    city: str
    target_date: date
    temp_value: int  # Temperature value (same unit as market)
    temp_unit: str  # "F" or "C"
    strategy: str
    entry_price: float
    entry_size: float  # USD invested
    shares: float  # Number of shares (size / entry_price)
    current_price: float  # Current market price
    unrealized_pnl: float = field(init=False)
    unrealized_pnl_pct: float = field(init=False)
    opened_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        # For binary options: shares = size / entry_price
        # Current value = shares * current_price
        current_value = self.shares * self.current_price
        self.unrealized_pnl = current_value - self.entry_size
        self.unrealized_pnl_pct = (
            self.unrealized_pnl / self.entry_size if self.entry_size > 0 else 0.0
        )

    def update_price(self, price: float) -> None:
        """Update current price and recalculate P&L."""
        self.current_price = price
        current_value = self.shares * self.current_price
        self.unrealized_pnl = current_value - self.entry_size
        self.unrealized_pnl_pct = (
            self.unrealized_pnl / self.entry_size if self.entry_size > 0 else 0.0
        )


@dataclass
class PortfolioPnL:
    """Real-time portfolio P&L tracking."""

    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    total_invested: float = 0.0
    positions: dict[str, PositionPnL] = field(default_factory=dict)  # token_id → PositionPnL
    peak_value: float = 0.0
    trough_value: float = 0.0
    current_bankroll: float = 0.0
    initial_bankroll: float = 0.0

    @property
    def drawdown(self) -> float:
        """Current drawdown from peak."""
        if self.peak_value <= 0:
            return 0.0
        return (self.peak_value - self.current_bankroll) / self.peak_value

    @property
    def max_drawdown(self) -> float:
        """Maximum historical drawdown."""
        if self.peak_value <= 0:
            return 0.0
        return (self.peak_value - self.trough_value) / self.peak_value

    @property
    def roi(self) -> float:
        """Return on investment."""
        if self.total_invested <= 0:
            return 0.0
        return self.total_pnl / self.total_invested

    def add_position(self, position: PositionPnL) -> None:
        """Add a new position."""
        self.positions[position.token_id] = position
        self.total_invested += position.entry_size
        self._update_totals()

    def remove_position(self, token_id: str, settlement_price: float) -> float:
        """Remove a settled position and return realized P&L."""
        if token_id not in self.positions:
            return 0.0

        pos = self.positions[token_id]
        # For binary options: payout = shares * settlement_price (0 or 1)
        shares = pos.shares
        payout = shares * settlement_price
        realized = payout - pos.entry_size

        self.realized_pnl += realized
        del self.positions[token_id]
        self._update_totals()

        return realized

    def update_prices(self, prices: dict[str, float]) -> None:
        """Update prices for multiple positions.

        Args:
            prices: Map of token_id → current price
        """
        for token_id, price in prices.items():
            if token_id in self.positions:
                self.positions[token_id].update_price(price)

        self._update_totals()

    def _update_totals(self) -> None:
        """Recalculate total unrealized P&L."""
        self.unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.current_bankroll = self.initial_bankroll + self.total_pnl

        # Update peak/trough
        if self.current_bankroll > self.peak_value:
            self.peak_value = self.current_bankroll
        if self.current_bankroll < self.trough_value:
            self.trough_value = self.current_bankroll

    def get_summary(self) -> dict:
        """Get P&L summary."""
        return {
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "total_invested": self.total_invested,
            "roi": self.roi * 100,  # As percentage
            "current_bankroll": self.current_bankroll,
            "peak_bankroll": self.peak_value,
            "drawdown": self.drawdown * 100,  # As percentage
            "max_drawdown": self.max_drawdown * 100,
            "open_positions": len(self.positions),
        }
