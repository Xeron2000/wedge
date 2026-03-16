# Configuration Management

## Overview

Wedge uses a hierarchical configuration system with the following priority (highest to lowest):

1. **CLI arguments** - Command-line flags override everything
2. **Environment variables** - Prefixed with `WEDGE_`
3. **Config file** - `~/.config/wedge/config.toml`
4. **Defaults** - Built-in default values

## File Locations

After `uv tool install wedge`, all data is stored in standard XDG directories:

```bash
Config:   ~/.config/wedge/config.toml
Database: ~/.local/share/wedge/wedge.db
Cache:    ~/.cache/wedge/
```

View paths:
```bash
wedge config path
```

## Quick Start

### 1. Initialize Config

```bash
wedge config init
```

This creates `~/.config/wedge/config.toml` with defaults.

### 2. Set API Keys

```bash
wedge config set polymarket_private_key "0x..."
wedge config set polymarket_api_key "your-api-key"
wedge config set polymarket_api_secret "your-secret"
wedge config set telegram_token "bot-token"
wedge config set telegram_chat_id "chat-id"
```

### 3. Adjust Parameters

```bash
wedge config set bankroll 5000
wedge config set max_bet 200
wedge config set kelly_fraction 0.2
wedge config set ladder_edge 0.06
```

### 4. View Config

```bash
wedge config show
```

Output:
```
Config: /home/user/.config/wedge/config.toml

bankroll                  = 5000
kelly_fraction            = 0.2
ladder_edge               = 0.06
max_bet                   = 200
mode                      = dry_run
polymarket_api_key        = ***
polymarket_api_secret     = ***
polymarket_private_key    = ***
tail_edge                 = 0.08
telegram_chat_id          = ***
telegram_token            = ***
```

## CLI Commands

### Config Management

```bash
wedge config init [--force]        # Initialize config file
wedge config set <key> <value>     # Set a config value
wedge config get <key>             # Get a config value
wedge config show                  # Show all config values
wedge config path                  # Show config and data paths
```

### Running the Bot

```bash
# Dry-run mode (default, uses config file)
wedge run

# Override specific parameters
wedge run --bankroll 10000 --max-bet 500

# Live mode (real trading)
wedge run --live

# With Telegram notifications
wedge run --telegram
```

### Other Commands

```bash
wedge scan --city NYC              # Run single scan
wedge stats --days 30              # Show statistics
```

## Configuration Options

### Trading Parameters

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"dry_run"` | Trading mode: `dry_run` or `live` |
| `bankroll` | float | `1000.0` | Starting bankroll ($) |
| `max_bet` | float | `100.0` | Maximum bet per trade ($) |
| `kelly_fraction` | float | `0.15` | Kelly fraction (0-1) |
| `max_bet_pct` | float | `0.05` | Max bet as % of bankroll |

### Strategy Parameters

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ladder_edge` | float | `0.05` | Ladder strategy edge threshold |
| `ladder_alloc` | float | `0.70` | Ladder allocation (70%) |
| `tail_edge` | float | `0.08` | Tail strategy edge threshold |
| `tail_odds` | float | `10.0` | Tail odds threshold |
| `tail_alloc` | float | `0.20` | Tail allocation (20%) |

### API Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `polymarket_private_key` | string | `""` | Ethereum private key |
| `polymarket_api_key` | string | `""` | Polymarket API key |
| `polymarket_api_secret` | string | `""` | Polymarket API secret |
| `telegram_token` | string | `""` | Telegram bot token |
| `telegram_chat_id` | string | `""` | Telegram chat ID |

## Environment Variables

All config options can be set via environment variables with `WEDGE_` prefix:

```bash
export WEDGE_MODE=live
export WEDGE_BANKROLL=5000
export WEDGE_POLYMARKET_PRIVATE_KEY="0x..."
export WEDGE_TELEGRAM_TOKEN="bot-token"

wedge run
```

## .env File Support

Create `.env` in your working directory:

```bash
WEDGE_MODE=dry_run
WEDGE_BANKROLL=5000
WEDGE_POLYMARKET_PRIVATE_KEY=0x...
WEDGE_POLYMARKET_API_KEY=your-key
WEDGE_POLYMARKET_API_SECRET=your-secret
WEDGE_TELEGRAM_TOKEN=bot-token
WEDGE_TELEGRAM_CHAT_ID=chat-id
```

## Migration from Old Setup

If you were using environment variables only:

```bash
# 1. Initialize config
wedge config init

# 2. Set your values
wedge config set polymarket_private_key "$WEDGE_POLYMARKET_PRIVATE_KEY"
wedge config set polymarket_api_key "$WEDGE_POLYMARKET_API_KEY"
wedge config set polymarket_api_secret "$WEDGE_POLYMARKET_API_SECRET"
wedge config set telegram_token "$WEDGE_TELEGRAM_TOKEN"
wedge config set telegram_chat_id "$WEDGE_TELEGRAM_CHAT_ID"

# 3. Remove environment variables (optional)
unset WEDGE_POLYMARKET_PRIVATE_KEY
unset WEDGE_POLYMARKET_API_KEY
# ... etc
```

## Security Notes

- Config file contains sensitive data (API keys, private keys)
- File permissions: `~/.config/wedge/config.toml` is readable only by you
- Never commit config files to git
- Use `wedge config show` to verify - sensitive values are masked with `***`

## Troubleshooting

### Config not found

```bash
$ wedge config show
No config file found. Run 'wedge config init' first.
```

Solution: Run `wedge config init`

### Database location changed

Old behavior: Database created in current directory (`./wedge.db`)
New behavior: Database in `~/.local/share/wedge/wedge.db`

Your data is now persistent regardless of where you run the command.

### Override not working

Check priority: CLI args > env vars > config file > defaults

Example:
```bash
# Config file has bankroll=5000
# But CLI override takes precedence:
wedge run --bankroll 10000  # Uses 10000, not 5000
```
