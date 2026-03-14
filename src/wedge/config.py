from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CityConfig(BaseModel):
    name: str
    lat: float
    lon: float
    timezone: str = "UTC"
    station: str = ""  # ICAO airport code (e.g. KLGA)


# CRITICAL: Coordinates MUST match the airport weather stations
# Polymarket resolves on. Using city center coords causes 3-8°F error.
DEFAULT_CITIES = [
    CityConfig(name="NYC", lat=40.7772, lon=-73.8726, timezone="America/New_York", station="KLGA"),
    CityConfig(name="Chicago", lat=41.9742, lon=-87.9073, timezone="America/Chicago", station="KORD"),
    CityConfig(name="Miami", lat=25.7959, lon=-80.2870, timezone="America/New_York", station="KMIA"),
    CityConfig(name="Dallas", lat=32.8471, lon=-96.8518, timezone="America/Chicago", station="KDAL"),
    CityConfig(name="Seattle", lat=47.4502, lon=-122.3088, timezone="America/Los_Angeles", station="KSEA"),
    CityConfig(name="Atlanta", lat=33.6407, lon=-84.4277, timezone="America/New_York", station="KATL"),
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WEDGE_")

    mode: str = "dry_run"
    bankroll: float = 1000.0
    max_bet: float = 100.0
    kelly_fraction: float = 0.15
    max_bet_pct: float = 0.05
    db_path: str = "wedge.db"

    ladder_edge: float = 0.05
    ladder_alloc: float = 0.70
    tail_edge: float = 0.08
    tail_odds: float = 10.0
    tail_alloc: float = 0.20

    brier_threshold: float = 0.25
    offsets_utc: list[str] = Field(
        default_factory=lambda: ["04:30", "10:30", "16:30", "22:30"]
    )

    cities: list[CityConfig] = Field(default_factory=lambda: list(DEFAULT_CITIES))

    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""

    telegram_token: str = ""
    telegram_chat_id: str = ""
