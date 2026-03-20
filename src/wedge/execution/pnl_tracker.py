"""Real-time P&L tracking module.

Provides:
- Position-level P&L tracking
- Portfolio-level P&L aggregation
- Drawdown monitoring
- Risk metrics calculation
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from wedge.execution.models import PortfolioPnL, PositionPnL
from wedge.log import get_logger
from wedge.market.models import Position

log = get_logger("execution.pnl")


@dataclass
class PnLSnapshot:
    """Point-in-time P&L snapshot."""

    timestamp: datetime
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    bankroll: float
    drawdown: float
    open_positions: int
    daily_pnl: float = 0.0


class PnLTracker:
    """Real-time P&L tracking for weather trading.

    Usage:
        tracker = PnLTracker(initial_bankroll=1000)

        # Add a position when order fills
        tracker.add_position(token_id="abc123", entry_price=0.30, size=50, shares=166.67)

        # Update prices (call periodically with latest market prices)
        await tracker.update_prices({"abc123": 0.35, "def456": 0.20})

        # Get current P&L
        pnl = tracker.get_pnl()
        print(f"Total P&L: ${pnl.total_pnl:.2f}")

        # Get snapshot for monitoring
        snapshot = tracker.get_snapshot()
    """

    def __init__(self, initial_bankroll: float):
        self._portfolio = PortfolioPnL(
            initial_bankroll=initial_bankroll,
            current_bankroll=initial_bankroll,
            peak_value=initial_bankroll,
        )
        self._snapshots: list[PnLSnapshot] = []
        self._daily_pnl: dict[str, float] = {}  # date → P&L

    @property
    def portfolio(self) -> PortfolioPnL:
        """Get portfolio P&L."""
        return self._portfolio

    def add_position(
        self,
        token_id: str,
        city: str,
        target_date: str,
        temp_value: int,
        temp_unit: str,
        strategy: str,
        entry_price: float,
        size: float,
    ) -> PositionPnL | None:
        """Add a new position.

        Args:
            token_id: Polymarket token ID
            city: City name
            target_date: Target date (ISO format)
            temp_value: Temperature value (same unit as market)
            temp_unit: "F" or "C"
            strategy: position strategy label
            entry_price: Entry price (0-1)
            size: USD amount invested
        """
        from datetime import date

        # For binary options: shares = size / entry_price
        shares = size / entry_price if entry_price > 0 else 0

        position = PositionPnL(
            token_id=token_id,
            city=city,
            target_date=(
                date.fromisoformat(target_date) if isinstance(target_date, str) else target_date
            ),
            temp_value=temp_value,
            temp_unit=temp_unit,
            strategy=strategy,
            entry_price=entry_price,
            entry_size=size,
            shares=shares,
            current_price=entry_price,
        )

        self._portfolio.add_position(position)
        log.info(
            "position_added",
            token_id=token_id,
            city=city,
            size=size,
            entry_price=entry_price,
            shares=shares,
        )
        return position

    def remove_position(
        self,
        token_id: str,
        settlement_price: float,
    ) -> float:
        """Remove a settled position.

        Args:
            token_id: Token ID to settle
            settlement_price: Final settlement price (0 or 1 for binary)

        Returns:
            Realized P&L from this position
        """
        realized = self._portfolio.remove_position(token_id, settlement_price)

        log.info(
            "position_settled",
            token_id=token_id,
            settlement_price=settlement_price,
            realized_pnl=realized,
        )

        return realized

    async def update_prices(self, prices: dict[str, float]) -> None:
        """Update prices for all positions.

        Args:
            prices: Map of token_id → current price
        """
        self._portfolio.update_prices(prices)

        # Log significant moves
        for token_id, price in prices.items():
            if token_id in self._portfolio.positions:
                pos = self._portfolio.positions[token_id]
                if abs(pos.unrealized_pnl_pct) > 0.2:  # 20%+ move
                    log.info(
                        "significant_price_move",
                        token_id=token_id,
                        price=price,
                        pnl=pos.unrealized_pnl,
                        pnl_pct=pos.unrealized_pnl_pct * 100,
                    )

    def get_pnl(self) -> PortfolioPnL:
        """Get current portfolio P&L."""
        return self._portfolio

    def get_snapshot(self) -> PnLSnapshot:
        """Take a P&L snapshot."""
        from datetime import date

        today = date.today().isoformat()
        daily_pnl = self._daily_pnl.get(today, 0.0)

        snapshot = PnLSnapshot(
            timestamp=datetime.now(),
            realized_pnl=self._portfolio.realized_pnl,
            unrealized_pnl=self._portfolio.unrealized_pnl,
            total_pnl=self._portfolio.total_pnl,
            bankroll=self._portfolio.current_bankroll,
            drawdown=self._portfolio.drawdown,
            open_positions=len(self._portfolio.positions),
            daily_pnl=daily_pnl,
        )

        self._snapshots.append(snapshot)
        return snapshot

    def record_daily_pnl(self, date_str: str, pnl: float) -> None:
        """Record daily P&L for tracking."""
        self._daily_pnl[date_str] = pnl

    def get_daily_pnl(self, date_str: str) -> float:
        """Get P&L for a specific date."""
        return self._daily_pnl.get(date_str, 0.0)

    def get_summary(self) -> dict:
        """Get comprehensive P&L summary."""
        summary = self._portfolio.get_summary()

        # Add daily P&L stats
        today = datetime.now().date().isoformat()
        summary["daily_pnl"] = self._daily_pnl.get(today, 0.0)

        # Add snapshot history count
        summary["snapshots_count"] = len(self._snapshots)

        return summary

    def get_positions_summary(self) -> list[dict]:
        """Get summary of all open positions."""
        positions = []
        for pos in self._portfolio.positions.values():
            positions.append(
                {
                    "token_id": pos.token_id,
                    "city": pos.city,
                    "target_date": str(pos.target_date),
                    "temp_value": pos.temp_value,
                    "temp_unit": pos.temp_unit,
                    "strategy": pos.strategy,
                    "entry_price": pos.entry_price,
                    "current_price": pos.current_price,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "unrealized_pnl_pct": pos.unrealized_pnl_pct * 100,
                }
            )
        return positions


async def sync_positions_with_market(
    tracker: PnLTracker,
    market_positions: list[Position],
    prices: dict[str, float],
) -> None:
    """Sync tracker positions with latest market data.

    Args:
        tracker: P&L tracker
        market_positions: Current positions from Polymarket
        prices: Current market prices
    """
    # Update prices
    await tracker.update_prices(prices)

    # Check for positions that should be settled
    for market_pos in market_positions:
        # Check if position is settled (past target date)
        from datetime import date

        if market_pos.bucket.date < date.today():
            # Position should be settled
            # This would need actual settlement price from Polymarket
            log.info(
                "position_past_settlement_date",
                token_id=market_pos.bucket.token_id,
                date=market_pos.bucket.date,
            )


def calculate_sharpe_ratio(
    daily_returns: list[float],
    risk_free_rate: float = 0.05,
) -> float:
    """Calculate Sharpe ratio from daily returns.

    Args:
        daily_returns: List of daily returns (as decimals, e.g., 0.05 = 5%)
        risk_free_rate: Annual risk-free rate

    Returns:
        Annualized Sharpe ratio
    """
    import statistics

    if not daily_returns or len(daily_returns) < 2:
        return 0.0

    mean_return = statistics.mean(daily_returns)
    std_return = statistics.stdev(daily_returns)

    if std_return == 0:
        return 0.0

    # Annualize
    annual_return = mean_return * 252  # Trading days
    annual_std = std_return * (252**0.5)

    sharpe = (annual_return - risk_free_rate) / annual_std
    return sharpe


def calculate_sortino_ratio(
    daily_returns: list[float],
    risk_free_rate: float = 0.05,
) -> float:
    """Calculate Sortino ratio (downside deviation only).

    Args:
        daily_returns: List of daily returns
        risk_free_rate: Annual risk-free rate

    Returns:
        Annualized Sortino ratio
    """
    import statistics

    if not daily_returns or len(daily_returns) < 2:
        return 0.0

    mean_return = statistics.mean(daily_returns)

    # Calculate downside deviation (only negative returns)
    negative_returns = [daily_return for daily_return in daily_returns if daily_return < 0]
    if not negative_returns:
        return float("inf")

    downside_std = (
        statistics.stdev(negative_returns)
        if len(negative_returns) > 1
        else abs(negative_returns[0])
    )

    if downside_std == 0:
        return 0.0

    # Annualize
    annual_return = mean_return * 252
    annual_downside = downside_std * (252**0.5)

    sortino = (annual_return - risk_free_rate) / annual_downside
    return sortino
