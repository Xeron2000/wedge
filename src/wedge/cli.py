from __future__ import annotations

import asyncio

import typer

from wedge.config import Settings
from wedge.log import setup_logging

app = typer.Typer(name="wedge", help="Weather prediction market trading bot")


@app.command()
def run(
    dry_run: bool = typer.Option(True, "--dry-run/--live", help="Run in simulation mode"),
    bankroll: float = typer.Option(1000.0, "--bankroll", "-b", help="Starting bankroll"),
    max_bet: float = typer.Option(100.0, "--max-bet", help="Max bet per trade"),
    kelly: float = typer.Option(0.15, "--kelly", help="Kelly fraction (0-1)"),
    ladder_edge: float = typer.Option(0.05, "--ladder-edge", help="Ladder edge threshold"),
    tail_edge: float = typer.Option(0.08, "--tail-edge", help="Tail edge threshold"),
    telegram: bool = typer.Option(False, "--telegram", help="Enable Telegram bot"),
) -> None:
    """Start the 7x24 trading bot."""
    settings = Settings(
        mode="dry_run" if dry_run else "live",
        bankroll=bankroll,
        max_bet=max_bet,
        kelly_fraction=kelly,
        ladder_edge=ladder_edge,
        tail_edge=tail_edge,
    )
    setup_logging()

    from wedge.scheduler import run_scheduler

    asyncio.run(run_scheduler(settings, enable_telegram=telegram))


@app.command()
def scan(
    city: str = typer.Option("NYC", "--city", help="City to scan"),
) -> None:
    """Run a single scan for a city."""
    settings = Settings()
    setup_logging()

    from wedge.pipeline import run_single_scan

    asyncio.run(run_single_scan(settings, city))


@app.command()
def stats(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to show"),
) -> None:
    """Show P&L, Brier score, and trade statistics."""
    settings = Settings()
    setup_logging()

    from wedge.monitoring.metrics import show_stats

    asyncio.run(show_stats(settings, days))


if __name__ == "__main__":  # pragma: no cover
    app()
