# wedge

Automated weather prediction market trading bot. Detects edges between GFS ensemble forecasts and Polymarket pricing, sizes positions with fractional Kelly criterion.

## Install

```bash
# From GitHub
uv tool install git+https://github.com/Xeron2000/wedge

# From source
git clone https://github.com/Xeron2000/wedge
cd wedge
uv sync
```

## Usage

```bash
wedge scan --city NYC            # Scan forecast distribution
wedge run --dry-run              # Simulated trading
wedge run --dry-run --telegram   # With Telegram notifications
wedge stats --days 30            # View P&L and Brier score
wedge run --live --bankroll 1000 # Live trading (requires Polymarket keys)
```

## Configuration

Copy `config.example.yaml` to `config.yaml`. Credentials use env vars only:

```bash
WEATHER_BOT_TELEGRAM_TOKEN=...
WEATHER_BOT_TELEGRAM_CHAT_ID=...
WEATHER_BOT_POLYMARKET_PRIVATE_KEY=...
```

## Tests

```bash
uv run pytest
```
