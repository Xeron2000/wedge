# wedge

Automated weather prediction market trading bot. Detects edges between GFS ensemble forecasts and Polymarket pricing, sizes positions with fractional Kelly criterion.

## Install

```bash
uv tool install git+https://github.com/Xeron2000/wedge
```

## Quick Start

```bash
# 1. Initialize config
wedge config init

# 2. Set API keys
wedge config set polymarket_private_key "0x..."
wedge config set polymarket_api_key "your-key"
wedge config set polymarket_api_secret "your-secret"

# 3. Run in dry-run mode
wedge run

# 4. Run in live mode
wedge run --live
```

## Configuration

Wedge uses a hierarchical config system:

**Priority**: CLI args > env vars > config file > defaults

**Locations**:
- Config: `~/.config/wedge/config.toml`
- Database: `~/.local/share/wedge/wedge.db`

### Config Commands

```bash
wedge config init              # Create config file
wedge config set <key> <value> # Set a value
wedge config show              # View all settings
wedge config path              # Show file locations
```

See [docs/CONFIG.md](docs/CONFIG.md) for full documentation.

## Usage

```bash
wedge scan --city NYC
wedge run --dry-run
wedge run --dry-run --bankroll 500 --kelly 0.10
wedge run --dry-run --bankroll 500 --kelly 0.10
wedge run --live --bankroll 1000
wedge stats --days 30
```

## Environment Variables

Config file is recommended, but env vars still work:

```bash
# Polymarket (required for --live)
export WEDGE_POLYMARKET_PRIVATE_KEY="..."
export WEDGE_POLYMARKET_API_KEY="..."
export WEDGE_POLYMARKET_API_SECRET="..."
```

## Tests

```bash
uv run pytest
```
