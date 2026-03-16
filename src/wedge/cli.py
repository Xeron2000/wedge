from __future__ import annotations

import asyncio

import typer

from wedge.config import Settings
from wedge.config_manager import app as config_app
from wedge.log import setup_logging

app = typer.Typer(name="wedge", help="Weather prediction market trading bot")
app.add_typer(config_app, name="config")


@app.command()
def run(
    dry_run: bool = typer.Option(True, "--dry-run/--live", help="Run in simulation mode"),
    bankroll: float = typer.Option(None, "--bankroll", "-b", help="Starting bankroll"),
    max_bet: float = typer.Option(None, "--max-bet", help="Max bet per trade"),
    kelly: float = typer.Option(None, "--kelly", help="Kelly fraction (0-1)"),
    ladder_edge: float = typer.Option(None, "--ladder-edge", help="Ladder edge threshold"),
    tail_edge: float = typer.Option(None, "--tail-edge", help="Tail edge threshold"),
    telegram: bool = typer.Option(False, "--telegram", help="Enable Telegram bot"),
) -> None:
    """Start the 7x24 trading bot."""
    overrides = {
        "mode": "dry_run" if dry_run else "live",
    }
    if bankroll is not None:
        overrides["bankroll"] = bankroll
    if max_bet is not None:
        overrides["max_bet"] = max_bet
    if kelly is not None:
        overrides["kelly_fraction"] = kelly
    if ladder_edge is not None:
        overrides["ladder_edge"] = ladder_edge
    if tail_edge is not None:
        overrides["tail_edge"] = tail_edge

    settings = Settings.load(**overrides)
    setup_logging()

    from wedge.scheduler import run_scheduler

    asyncio.run(run_scheduler(settings, enable_telegram=telegram))


@app.command()
def scan(
    city: str = typer.Option("NYC", "--city", help="City to scan"),
) -> None:
    """Run a single scan for a city."""
    settings = Settings.load()
    setup_logging()

    from wedge.pipeline import run_single_scan

    asyncio.run(run_single_scan(settings, city))


@app.command()
def stats(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to show"),
) -> None:
    """Show P&L, Brier score, and trade statistics."""
    settings = Settings.load()
    setup_logging()

    from wedge.monitoring.metrics import show_stats

    asyncio.run(show_stats(settings, days))


@app.command()
def backtest(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to backtest"),
) -> None:
    """Run backtest on historical settled trades."""
    from datetime import datetime, timedelta

    settings = Settings.load()
    setup_logging()

    from wedge.backtest import run_backtest

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    asyncio.run(run_backtest(settings, start_date, end_date))


@app.command()
def calibration(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to analyze"),
) -> None:
    """Validate model calibration against actual outcomes."""
    settings = Settings.load()
    setup_logging()

    from wedge.backtest import validate_model_calibration

    asyncio.run(validate_model_calibration(settings, days))


if __name__ == "__main__":  # pragma: no cover
    app()
