from __future__ import annotations

from datetime import date

from wedge.config import Settings
from wedge.db import Database
from wedge.log import get_logger

log = get_logger("backtest")


async def run_backtest(settings: Settings, start_date: date, end_date: date) -> dict:
    """Run backtest on historical settled trades.

    Analyzes actual performance vs model predictions to validate strategy.
    """
    db = Database(settings.db_path)
    await db.connect()

    try:
        # Get all settled trades in date range
        trades = await db.get_settled_trades(start_date, end_date)

        if not trades:
            log.warning("no_settled_trades", start=str(start_date), end=str(end_date))
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "brier_score": None,
                "roi": 0.0,
            }

        # Calculate metrics
        total_trades = len(trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        win_rate = wins / total_trades if total_trades > 0 else 0.0
        total_pnl = sum(t["pnl"] for t in trades)
        total_invested = sum(t["size"] for t in trades)
        roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0.0

        # Calculate Brier score
        brier_sum = 0.0
        brier_count = 0
        for t in trades:
            if t["p_model"] is not None and t["outcome"] is not None:
                # outcome: 1 if won, 0 if lost
                outcome = 1 if t["pnl"] > 0 else 0
                brier_sum += (t["p_model"] - outcome) ** 2
                brier_count += 1

        brier_score = brier_sum / brier_count if brier_count > 0 else None

        # Strategy breakdown
        ladder_trades = [t for t in trades if t["strategy"] == "ladder"]
        ladder_pnl = sum(t["pnl"] for t in ladder_trades)

        result = {
            "total_trades": total_trades,
            "wins": wins,
            "losses": total_trades - wins,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "total_invested": total_invested,
            "roi": roi,
            "brier_score": brier_score,
            "ladder": {
                "trades": len(ladder_trades),
                "pnl": ladder_pnl,
                "win_rate": (
                    sum(1 for trade in ladder_trades if trade["pnl"] > 0) / len(ladder_trades)
                    if ladder_trades
                    else 0.0
                ),
            },
        }

        log.info("backtest_complete", **result)
        return result

    finally:
        await db.close()
