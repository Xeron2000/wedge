# wedge

Automated weather prediction market trading bot. Detects edges between GFS ensemble forecasts and Polymarket pricing, sizes positions with fractional Kelly criterion.

## Install

```bash
uv tool install git+https://github.com/Xeron2000/wedge
```

## Usage

```bash
wedge scan --city NYC
wedge run --dry-run
wedge run --dry-run --bankroll 500 --kelly 0.10
wedge run --dry-run --telegram
wedge run --live --bankroll 1000
wedge stats --days 30
```

## Environment Variables

All configuration via `WEDGE_*` env vars or CLI flags.

```bash
# Telegram (optional)
export WEDGE_TELEGRAM_TOKEN="..."
export WEDGE_TELEGRAM_CHAT_ID="..."

# Polymarket (required for --live)
export WEDGE_POLYMARKET_PRIVATE_KEY="..."
export WEDGE_POLYMARKET_API_KEY="..."
export WEDGE_POLYMARKET_API_SECRET="..."
```

## Tests

```bash
uv run pytest
```
