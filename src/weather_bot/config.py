from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CityConfig(BaseModel):
    name: str
    lat: float
    lon: float
    timezone: str = "UTC"
    station: str = ""  # ICAO airport code (e.g. KLGA)


class LadderConfig(BaseModel):
    edge_threshold: float = 0.05
    allocation: float = 0.70


class TailConfig(BaseModel):
    edge_threshold: float = 0.08
    min_odds: float = 10.0
    allocation: float = 0.20


class StrategyConfig(BaseModel):
    ladder: LadderConfig = Field(default_factory=LadderConfig)
    tail: TailConfig = Field(default_factory=TailConfig)
    cash_reserve: float = 0.10


class SchedulerConfig(BaseModel):
    offsets_utc: list[str] = Field(default_factory=lambda: ["04:30", "10:30", "16:30", "22:30"])


class MonitoringConfig(BaseModel):
    brier_threshold: float = 0.25
    webhook_url: str | None = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WEATHER_BOT_")

    mode: str = "dry_run"
    bankroll: float = 1000.0
    max_bet: float = 100.0
    kelly_fraction: float = 0.15
    max_bet_pct: float = 0.05
    db_path: str = "weather_bot.db"

    # CRITICAL: Coordinates MUST match the airport weather stations
    # Polymarket resolves on. Using city center coords causes 3-8°F error.
    cities: list[CityConfig] = Field(default_factory=lambda: [
        CityConfig(name="NYC", lat=40.7772, lon=-73.8726, timezone="America/New_York", station="KLGA"),
        CityConfig(name="Chicago", lat=41.9742, lon=-87.9073, timezone="America/Chicago", station="KORD"),
        CityConfig(name="Miami", lat=25.7959, lon=-80.2870, timezone="America/New_York", station="KMIA"),
        CityConfig(name="Dallas", lat=32.8471, lon=-96.8518, timezone="America/Chicago", station="KDAL"),
        CityConfig(name="Seattle", lat=47.4502, lon=-122.3088, timezone="America/Los_Angeles", station="KSEA"),
        CityConfig(name="Atlanta", lat=33.6407, lon=-84.4277, timezone="America/New_York", station="KATL"),
    ])

    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""

    telegram_token: str = ""
    telegram_chat_id: str = ""


_SENSITIVE_KEYS = {
    "polymarket_private_key", "polymarket_api_key", "polymarket_api_secret",
    "telegram_token",
}


def load_settings(config_path: Path | None = None) -> Settings:
    overrides: dict = {}
    if config_path and config_path.exists():
        with open(config_path) as f:
            overrides = yaml.safe_load(f) or {}
        # Reject sensitive fields in YAML — must use env vars only
        leaked = _SENSITIVE_KEYS & set(overrides)
        if leaked:
            raise ValueError(
                f"Sensitive fields found in config file: {leaked}. "
                "Use WEATHER_BOT_* environment variables instead."
            )
    return Settings(**overrides)
