from __future__ import annotations

from datetime import date, datetime, timedelta

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
        tail_trades = [t for t in trades if t["strategy"] == "tail"]

        ladder_pnl = sum(t["pnl"] for t in ladder_trades)
        tail_pnl = sum(t["pnl"] for t in tail_trades)

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
                "win_rate": sum(1 for t in ladder_trades if t["pnl"] > 0) / len(ladder_trades) if ladder_trades else 0.0,
            },
            "tail": {
                "trades": len(tail_trades),
                "pnl": tail_pnl,
                "win_rate": sum(1 for t in tail_trades if t["pnl"] > 0) / len(tail_trades) if tail_trades else 0.0,
            },
        }

        log.info("backtest_complete", **result)
        return result

    finally:
        await db.close()


async def validate_model_calibration(settings: Settings, days: int = 30) -> dict:
    """Validate model calibration by comparing predicted vs actual outcomes.

    Groups trades by predicted probability buckets and checks if actual
    win rates match predictions.
    """
    db = Database(settings.db_path)
    await db.connect()

    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        trades = await db.get_settled_trades(start_date, end_date)

        if not trades:
            log.warning("no_trades_for_calibration", days=days)
            return {}

        # Group by probability buckets
        buckets = {
            "0-20%": [],
            "20-40%": [],
            "40-60%": [],
            "60-80%": [],
            "80-100%": [],
        }

        for t in trades:
            if t["p_model"] is None or t["outcome"] is None:
                continue

            p = t["p_model"]
            outcome = 1 if t["pnl"] > 0 else 0

            if p < 0.2:
                buckets["0-20%"].append(outcome)
            elif p < 0.4:
                buckets["20-40%"].append(outcome)
            elif p < 0.6:
                buckets["40-60%"].append(outcome)
            elif p < 0.8:
                buckets["60-80%"].append(outcome)
            else:
                buckets["80-100%"].append(outcome)

        # Calculate actual win rates per bucket
        calibration = {}
        for bucket_name, outcomes in buckets.items():
            if not outcomes:
                continue

            actual_rate = sum(outcomes) / len(outcomes)
            expected_rate = {
                "0-20%": 0.10,
                "20-40%": 0.30,
                "40-60%": 0.50,
                "60-80%": 0.70,
                "80-100%": 0.90,
            }[bucket_name]

            calibration[bucket_name] = {
                "count": len(outcomes),
                "expected": expected_rate,
                "actual": actual_rate,
                "error": abs(actual_rate - expected_rate),
            }

        log.info("calibration_check", buckets=calibration)
        return calibration

    finally:
        await db.close()
