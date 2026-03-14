from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from weather_bot.config import load_settings
from weather_bot.log import setup_logging

app = typer.Typer(name="wedge", help="Weather prediction market trading bot")


@app.command()
def run(
    dry_run: bool = typer.Option(True, "--dry-run/--live", help="Run in simulation mode"),
    config: Path | None = typer.Option(None, "--config", "-c", help="Path to config YAML"),
    bankroll: float | None = typer.Option(None, "--bankroll", "-b", help="Override bankroll"),
    telegram: bool = typer.Option(False, "--telegram", help="Enable Telegram bot"),
) -> None:
    """Start the 7x24 trading bot."""
    settings = load_settings(config)
    if bankroll is not None:
        settings = settings.model_copy(update={"bankroll": bankroll})
    settings = settings.model_copy(update={"mode": "dry_run" if dry_run else "live"})
    setup_logging()

    from weather_bot.scheduler import run_scheduler

    asyncio.run(run_scheduler(settings, enable_telegram=telegram))


@app.command()
def scan(
    city: str = typer.Option("NYC", "--city", help="City to scan"),
    config: Path | None = typer.Option(None, "--config", "-c"),
) -> None:
    """Run a single scan for a city."""
    settings = load_settings(config)
    setup_logging()

    from weather_bot.pipeline import run_single_scan

    asyncio.run(run_single_scan(settings, city))


@app.command()
def stats(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to show"),
    config: Path | None = typer.Option(None, "--config", "-c"),
) -> None:
    """Show P&L, Brier score, and trade statistics."""
    settings = load_settings(config)
    setup_logging()

    from weather_bot.monitoring.metrics import show_stats

    asyncio.run(show_stats(settings, days))


if __name__ == "__main__":
    app()
