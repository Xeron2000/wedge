from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_config_dir() -> Path:
    """Get XDG config directory."""
    config_dir = Path.home() / ".config" / "wedge"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_data_dir() -> Path:
    """Get XDG data directory."""
    data_dir = Path.home() / ".local" / "share" / "wedge"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_cache_dir() -> Path:
    """Get XDG cache directory."""
    cache_dir = Path.home() / ".cache" / "wedge"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_config_path() -> Path:
    """Get config file path."""
    return get_config_dir() / "config.toml"


def load_config_file() -> dict[str, Any]:
    """Load config from TOML file if exists."""
    config_path = get_config_path()
    if not config_path.exists():
        return {}

    with open(config_path, "rb") as f:
        return tomllib.load(f)


class CityConfig(BaseModel):
    name: str
    lat: float
    lon: float
    timezone: str = "UTC"
    station: str = ""  # ICAO airport code (e.g. KLGA)


# CRITICAL: Coordinates MUST match the airport weather stations
# Polymarket resolves on. Using city center coords causes 3-8°F error.
# High liquidity markets only (>$25K daily volume)
DEFAULT_CITIES = [
    CityConfig(name="Seoul", lat=37.4602, lon=126.4407, timezone="Asia/Seoul", station="RKSI"),
    CityConfig(name="London", lat=51.4700, lon=-0.4543, timezone="Europe/London", station="EGLL"),
    CityConfig(name="NYC", lat=40.7772, lon=-73.8726, timezone="America/New_York", station="KLGA"),
    CityConfig(name="Shanghai", lat=31.1434, lon=121.8052, timezone="Asia/Shanghai", station="ZSSS"),
    CityConfig(name="Miami", lat=25.7959, lon=-80.2870, timezone="America/New_York", station="KMIA"),
    CityConfig(name="Wellington", lat=-41.3272, lon=174.8050, timezone="Pacific/Auckland", station="NZWN"),
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WEDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    mode: str = "dry_run"
    bankroll: float = 1000.0
    max_bet: float = 100.0
    kelly_fraction: float = 0.15
    max_bet_pct: float = 0.05
    db_path: str = Field(default_factory=lambda: str(get_data_dir() / "wedge.db"))

    ladder_edge: float = 0.08  # Increased from 0.05 to account for model calibration error
    ladder_alloc: float = 0.70
    tail_edge: float = 0.12  # Increased from 0.08 for higher confidence threshold
    tail_odds: float = 10.0
    tail_alloc: float = 0.20

    brier_threshold: float = 0.25  # Pause trading if weekly Brier score exceeds this
    offsets_utc: list[str] = Field(
        default_factory=lambda: ["04:30", "10:30", "16:30", "22:30"]
    )

    cities: list[CityConfig] = Field(default_factory=lambda: list(DEFAULT_CITIES))

    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""

    telegram_token: str = ""
    telegram_chat_id: str = ""

    @classmethod
    def load(cls, **overrides: Any) -> Settings:
        """Load settings from config file, env vars, and overrides.

        Priority: overrides > env vars > config file > defaults
        """
        config_data = load_config_file()
        # Merge config file with overrides
        merged = {**config_data, **overrides}
        return cls(**merged)
