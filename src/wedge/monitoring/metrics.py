from __future__ import annotations

from wedge.config import Settings
from wedge.db import Database
from wedge.log import get_logger

log = get_logger("monitoring")


async def show_stats(settings: Settings, days: int = 30) -> None:
    """Display trading statistics."""
    db = Database(settings.db_path)
    await db.connect()

    try:
        brier = await db.get_brier_score(days)
        pnl = await db.get_pnl_summary(days)

        # Get latest balance snapshot for unrealized P&L
        latest_balance = await db.get_last_balance_snapshot()

        log.info("=== Weather Edge Bot Stats ===")
        log.info(
            "pnl_summary",
            period=f"{days} days",
            total_trades=pnl["total_trades"],
            wins=pnl["wins"],
            win_rate=f"{pnl['win_rate']:.1%}" if pnl["total_trades"] > 0 else "N/A",
            total_pnl=f"${pnl['total_pnl']:.2f}",
        )

        if latest_balance:
            balance, unrealized_pnl = latest_balance
            total_value = balance + unrealized_pnl
            log.info(
                "portfolio",
                cash=f"${balance:.2f}",
                unrealized_pnl=f"${unrealized_pnl:.2f}",
                total_value=f"${total_value:.2f}",
            )

        if brier is not None:
            status = "GOOD" if brier < 0.20 else "OK" if brier < 0.25 else "PAUSED"
            log.info("brier_score", score=f"{brier:.4f}", status=status)
        else:
            log.info("brier_score", score="N/A (no settled forecasts)")

        if pnl.get("best_trade") is not None:
            log.info(
                "trade_range",
                best=f"${pnl['best_trade']:.2f}",
                worst=f"${pnl['worst_trade']:.2f}",
            )
    finally:
        await db.close()
