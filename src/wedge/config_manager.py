"""Configuration file management."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import typer

from wedge.config import get_config_dir, get_config_path, get_data_dir, load_config_file

app = typer.Typer(name="config", help="Manage configuration")


def _write_config(data: dict[str, Any]) -> None:
    """Write config data to TOML file."""
    import tomli_w

    config_path = get_config_path()
    with open(config_path, "wb") as f:
        tomli_w.dump(data, f)


@app.command()
def init(force: bool = typer.Option(False, "--force", help="Overwrite existing config")) -> None:
    """Initialize config file with defaults."""
    config_path = get_config_path()

    if config_path.exists() and not force:
        typer.echo(f"Config already exists: {config_path}")
        typer.echo("Use --force to overwrite")
        raise typer.Exit(1)

    default_config = {
        "mode": "dry_run",
        "bankroll": 1000.0,
        "max_bet": 100.0,
        "kelly_fraction": 0.15,
        "ladder_edge": 0.05,
        "tail_edge": 0.08,
        "polymarket_private_key": "",
        "polymarket_api_key": "",
        "polymarket_api_secret": "",
        "telegram_token": "",
        "telegram_chat_id": "",
    }

    _write_config(default_config)
    typer.echo(f"✓ Config initialized: {config_path}")


@app.command()
def set(key: str, value: str) -> None:
    """Set a config value."""
    config_data = load_config_file()

    # Type conversion
    if value.lower() in ("true", "false"):
        typed_value: Any = value.lower() == "true"
    elif value.replace(".", "", 1).isdigit():
        typed_value = float(value) if "." in value else int(value)
    else:
        typed_value = value

    config_data[key] = typed_value
    _write_config(config_data)
    typer.echo(f"✓ Set {key} = {typed_value}")


@app.command()
def get(key: str) -> None:
    """Get a config value."""
    config_data = load_config_file()

    if key not in config_data:
        typer.echo(f"Key not found: {key}", err=True)
        raise typer.Exit(1)

    typer.echo(config_data[key])


@app.command()
def show() -> None:
    """Show all config values."""
    config_data = load_config_file()

    if not config_data:
        typer.echo("No config file found. Run 'wedge config init' first.")
        raise typer.Exit(1)

    typer.echo(f"Config: {get_config_path()}\n")
    for key, value in sorted(config_data.items()):
        # Mask sensitive values
        if "key" in key.lower() or "token" in key.lower() or "secret" in key.lower():
            display_value = "***" if value else "(not set)"
        else:
            display_value = value
        typer.echo(f"{key:25} = {display_value}")


@app.command()
def path() -> None:
    """Show config and data paths."""
    typer.echo(f"Config dir:  {get_config_dir()}")
    typer.echo(f"Config file: {get_config_path()}")
    typer.echo(f"Data dir:    {get_data_dir()}")
