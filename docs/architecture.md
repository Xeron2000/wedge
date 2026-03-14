# Weather Edge Bot - Architecture Design

## Overview

7x24 自动化天气预测市场交易 CLI Bot。基于 GFS 集合预报概率 vs Polymarket 市场价格的 edge 检测，使用 Kelly 准则管理仓位。

## System Context

```
┌─────────────────────────────────────────────────────────┐
│                    weather-edge-bot                       │
│                                                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │
│  │ Scheduler │───▶│ Pipeline │───▶│ Executor         │   │
│  │ (APSched) │    │          │    │ (dry/live)       │   │
│  └──────────┘    │ Weather  │    └──────────────────┘   │
│                  │ → Edge   │                            │
│  ┌──────────┐   │ → Kelly  │    ┌──────────────────┐   │
│  │ CLI      │   │ → Signal │───▶│ SQLite            │   │
│  │ (Typer)  │   └──────────┘    │ (trades/metrics)  │   │
│  └──────────┘                    └──────────────────┘   │
└─────────┬────────────────┬──────────────────────────────┘
          │                │
          ▼                ▼
   ┌────────────┐   ┌──────────────┐
   │ Open-Meteo │   │ Polymarket   │
   │ (GFS data) │   │ (CLOB API)   │
   └────────────┘   └──────────────┘
```

## Components

### 1. CLI Layer (`cli.py`)

Typer app，单入口：

```
weather-bot run [--dry-run|--live] [--bankroll N] [--cities NYC,CHI,...]
weather-bot scan [--city NYC]          # 单次扫描
weather-bot stats [--days 7]           # P&L / Brier score
weather-bot backtest [--from DATE]     # 历史回测
```

`[project.scripts]` 注册为 `weather-bot`。

### 2. Scheduler (`scheduler.py`)

- APScheduler AsyncIOScheduler
- GFS 更新周期触发：04:30Z / 10:30Z / 16:30Z / 22:30Z（发布后 ~4.5h）
- 可配置偏移量
- 优雅关闭：SIGINT/SIGTERM 处理

### 3. Weather Module (`weather/`)

**`client.py`** — Open-Meteo HTTP 客户端
- `GET /v1/ensemble` 获取 GFS 31 成员预报
- 参数：latitude, longitude, daily temperature_2m_max
- httpx AsyncClient，超时 30s，重试 3 次

**`ensemble.py`** — 概率分布计算
- 31 成员 → 温度直方图（1°F 分辨率）
- 输出：`dict[int, float]` = {温度: 概率}
- 支持多天预报（1-7 天）

**`models.py`** — 数据模型
```python
class ForecastDistribution(BaseModel):
    city: str
    date: date
    buckets: dict[int, float]  # temp_f → probability
    ensemble_spread: float     # σ
    updated_at: datetime
```

### 4. Market Module (`market/`)

**`polymarket.py`** — Polymarket CLOB 客户端
- py-clob-client 封装
- 获取天气合约列表 + 当前价格
- 下单 / 取消单
- 需要：PRIVATE_KEY, API_KEY, API_SECRET

**`scanner.py`** — 合约扫描器
- 扫描指定城市的温度合约
- 解析温度档位 + 市场价格
- 输出：`list[MarketBucket]`

**`models.py`**
```python
class MarketBucket(BaseModel):
    token_id: str
    city: str
    date: date
    temp_f: int          # 温度档位
    market_price: float  # 0-1
    implied_prob: float  # = market_price

class Position(BaseModel):
    bucket: MarketBucket
    side: Literal["buy"]
    size: float          # 下注金额
    entry_price: float
    strategy: Literal["ladder", "tail"]
```

### 5. Strategy Module (`strategy/`)

**`edge.py`** — Edge 检测
```python
def detect_edge(
    forecast: ForecastDistribution,
    markets: list[MarketBucket],
) -> list[EdgeSignal]:
    # P_model - P_market > threshold → signal
```

**`kelly.py`** — Kelly 仓位
```python
def fractional_kelly(
    p_model: float,
    market_price: float,
    bankroll: float,
    fraction: float = 0.15,  # 15% fractional Kelly
    max_bet_pct: float = 0.05,  # 单笔上限 5% bankroll
) -> float:  # 下注金额
```

**`ladder.py`** — 层1 区间阶梯
- 买入 P_model 中心区域 ±1σ 的 3-5 个档位
- 触发：range edge > 5%
- 资金占比：70%

**`tail.py`** — 层2 尾部猎手
- ensemble 中 ≥3 成员预测极端温度
- 市场定价 2-8%，模型 >10%
- 触发：single edge > 8%，赔率 >10:1
- 资金占比：20%

**`portfolio.py`** — 组合管理
- 资金分配：ladder 70% / tail 20% / cash 10%
- 每日重平衡
- Brier score > 0.25 → 熔断暂停

### 6. Execution Module (`execution/`)

**接口**
```python
class Executor(Protocol):
    async def place_order(self, position: Position) -> OrderResult: ...
    async def get_positions(self) -> list[Position]: ...
    async def get_balance(self) -> float: ...
```

**`dry_run.py`** — DryRunExecutor
- 内存 + SQLite 模拟
- 完整交易记录
- 结算时按实际温度结算
- 与 live 共享所有策略逻辑

**`live.py`** — LiveExecutor
- 调用 Polymarket CLOB API
- 订单状态跟踪
- 错误处理 + 重试

### 7. Monitoring (`monitoring/`)

**`metrics.py`**
- Brier score：持续追踪预测准确度
- P&L：每笔 / 每日 / 累计
- Drawdown：最大回撤
- Win rate / Edge realized

**`notify.py`**
- structlog 结构化日志（默认 stdout）
- Webhook 通知（可选，POST JSON）

### 8. Storage (`db.py`)

SQLite via aiosqlite：

```sql
-- 交易记录
CREATE TABLE trades (
    id INTEGER PRIMARY KEY,
    city TEXT,
    date TEXT,
    temp_f INTEGER,
    strategy TEXT,        -- 'ladder' | 'tail'
    entry_price REAL,
    size REAL,
    p_model REAL,
    p_market REAL,
    edge REAL,
    settled INTEGER DEFAULT 0,
    outcome REAL,         -- 0 or 1
    pnl REAL,
    created_at TEXT
);

-- 预测记录（Brier score 计算）
CREATE TABLE forecasts (
    id INTEGER PRIMARY KEY,
    city TEXT,
    date TEXT,
    temp_f INTEGER,
    p_model REAL,
    actual_temp_f INTEGER,
    created_at TEXT
);

-- 资金快照
CREATE TABLE bankroll_snapshots (
    id INTEGER PRIMARY KEY,
    balance REAL,
    unrealized_pnl REAL,
    created_at TEXT
);
```

## Data Flow

```
1. Scheduler 触发（每 6h）
   │
2. Weather: Open-Meteo → 31 成员预报 → ForecastDistribution
   │
3. Market: Polymarket scan → list[MarketBucket]
   │
4. Strategy:
   ├── edge.detect_edge(forecast, markets) → list[EdgeSignal]
   ├── ladder.evaluate(signals, bankroll * 0.7) → list[Position]
   ├── tail.evaluate(signals, bankroll * 0.2) → list[Position]
   └── kelly.size(positions) → sized positions
   │
5. Execution:
   ├── dry_run: log + SQLite
   └── live: Polymarket CLOB API
   │
6. Monitoring: update metrics, log results
```

## Configuration

```yaml
# config.yaml
mode: dry_run  # dry_run | live

bankroll: 1000
max_bet: 100
kelly_fraction: 0.15

cities:
  - name: NYC
    lat: 40.7128
    lon: -74.0060
  - name: Chicago
    lat: 41.8781
    lon: -87.6298
  - name: Miami
    lat: 25.7617
    lon: -80.1918
  - name: LA
    lat: 34.0522
    lon: -118.2437
  - name: Denver
    lat: 39.7392
    lon: -104.9903

strategy:
  ladder:
    edge_threshold: 0.05
    allocation: 0.70
  tail:
    edge_threshold: 0.08
    min_odds: 10
    allocation: 0.20
  cash_reserve: 0.10

scheduler:
  # GFS 发布后 ~4.5h 触发
  offsets_utc: ["04:30", "10:30", "16:30", "22:30"]

monitoring:
  brier_threshold: 0.25  # > 0.25 暂停交易
  webhook_url: null       # optional

# 环境变量覆盖（敏感信息）
# POLYMARKET_PRIVATE_KEY
# POLYMARKET_API_KEY
# POLYMARKET_API_SECRET
```

## Error Handling

| 场景 | 策略 |
|------|------|
| Open-Meteo API 超时 | 重试 3 次，指数退避，跳过本轮 |
| Polymarket API 失败 | 重试 2 次，记录未执行信号 |
| 数据不完整 | 跳过该城市，不强行交易 |
| Brier > 0.25 | 熔断，停止交易，通知 |
| 进程崩溃 | Systemd/supervisor 自动重启 |
| SQLite 写入失败 | 内存 buffer，延迟写入 |

## Security

- 私钥仅通过环境变量注入
- SQLite 文件权限 600
- 无 Web UI，纯 CLI
- 日志脱敏：不打印私钥 / API secret

## Project Structure

```
weather-edge-bot/
├── pyproject.toml
├── config.example.yaml
├── src/
│   └── weather_bot/
│       ├── __init__.py
│       ├── cli.py              # Typer CLI 入口
│       ├── config.py           # Pydantic Settings
│       ├── db.py               # SQLite 管理
│       ├── scheduler.py        # APScheduler
│       ├── weather/
│       │   ├── __init__.py
│       │   ├── client.py       # Open-Meteo 客户端
│       │   ├── ensemble.py     # 概率分布计算
│       │   └── models.py
│       ├── market/
│       │   ├── __init__.py
│       │   ├── polymarket.py   # Polymarket CLOB
│       │   ├── scanner.py      # 合约扫描
│       │   └── models.py
│       ├── strategy/
│       │   ├── __init__.py
│       │   ├── edge.py         # Edge 检测
│       │   ├── kelly.py        # Kelly 仓位
│       │   ├── ladder.py       # 区间阶梯
│       │   ├── tail.py         # 尾部猎手
│       │   └── portfolio.py    # 组合管理
│       ├── execution/
│       │   ├── __init__.py
│       │   ├── executor.py     # Protocol 接口
│       │   ├── dry_run.py      # 模拟执行
│       │   └── live.py         # 实盘执行
│       └── monitoring/
│           ├── __init__.py
│           ├── metrics.py      # Brier/P&L/Drawdown
│           └── notify.py       # 日志 + Webhook
└── tests/
    ├── __init__.py
    ├── test_ensemble.py
    ├── test_edge.py
    ├── test_kelly.py
    ├── test_ladder.py
    ├── test_tail.py
    └── test_dry_run.py
```

## Dependencies

```toml
[project]
dependencies = [
    "httpx>=0.27",
    "apscheduler>=3.10",
    "py-clob-client>=0.1",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "pyyaml>=6.0",
    "aiosqlite>=0.20",
    "typer>=0.12",
    "structlog>=24.0",
]

[project.scripts]
weather-bot = "weather_bot.cli:app"
```

## Non-Functional Requirements

| 指标 | 目标 |
|------|------|
| 可用性 | 7x24，进程监控自动重启 |
| 延迟 | 每轮扫描 < 30s |
| 资源 | < 100MB RAM，< 1% CPU |
| 存储 | SQLite < 100MB/年 |
| 可观测 | 结构化日志，Brier score 趋势 |
