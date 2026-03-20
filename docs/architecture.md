# Weather Edge Bot 架构设计

## 概览

Wedge 现在被刻意收缩为一条单一主线：

1. 直接抓取 NOAA GEFS 集合预报
2. 构造某城市本地日最高温的概率分布
3. 将模型概率与 Polymarket 价格对比
4. 仅使用 **Ladder-only** 策略，并用 fractional Kelly 定仓
5. 管理提前退出与结算

整个仓库现在都围绕这一条套利逻辑组织。

## 系统上下文

```text
┌─────────────────────────────────────────────────────────┐
│                        wedge                            │
│                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │
│  │ Scheduler │───▶│ Pipeline │───▶│ Executor         │   │
│  │ (APSched) │    │          │    │ (dry/live)       │   │
│  └──────────┘    │ NOAA     │    └──────────────────┘   │
│                  │ → Edge   │                            │
│  ┌──────────┐    │ → Kelly  │    ┌──────────────────┐   │
│  │ CLI      │    │ → Ladder │───▶│ SQLite            │   │
│  │ (Typer)  │    └──────────┘    │ (trades/metrics)  │   │
│  └──────────┘                     └──────────────────┘   │
└─────────┬────────────────┬──────────────────────────────┘
          │                │
          ▼                ▼
   ┌────────────┐   ┌──────────────┐
   │ NOAA GEFS  │   │ Polymarket   │
   │ (NOMADS)   │   │ (CLOB API)   │
   └────────────┘   └──────────────┘
```

## 组件

### 1. CLI 层（`cli.py`）

可用命令：

```bash
wedge run
wedge scan --city NYC
wedge stats --days 30
wedge backtest --days 30
```

### 2. 调度层（`scheduler.py`）

- 使用 APScheduler `AsyncIOScheduler`
- 按配置的 UTC offset 在 GEFS cycle 之后触发
- 启动时立即执行一次 pipeline
- 每天执行一次 settlement job
- 不再包含 telegram 编排
- 不再包含 arbitrage 辅助扫描

### 3. 天气模块（`weather/`）

#### `client.py`

- 从 NOAA NOMADS grib filter 拉取 GEFS member 文件
- 提取 `2 m above ground` 的 `TMP`
- 将 3 小时步长预报聚合成 **每个 member 的本地日最高温**
- 输出 NOAA-native 结构化 payload

#### `ensemble.py`

- 将 member 的日最高温转换成 `ForecastDistribution`
- 以 1°F 为粒度做桶化
- 根据 member 值计算 ensemble spread

#### `models.py`

```python
class ForecastDistribution(BaseModel):
    city: str
    date: date
    buckets: dict[int, float]
    ensemble_spread: float
    member_count: int
    updated_at: datetime
```

### 4. 市场模块（`market/`）

#### `polymarket.py`

- 封装 Polymarket CLOB 访问
- 处理 live 模式认证与市场交互

#### `scanner.py`

- 扫描指定 city/date 的天气市场
- 解析市场 bucket 与价格

#### `models.py`

```python
class MarketBucket(BaseModel):
    token_id: str
    city: str
    date: date
    temp_value: int
    temp_unit: str
    market_price: float
    implied_prob: float

class Position(BaseModel):
    bucket: MarketBucket
    side: Literal["buy"]
    size: float
    entry_price: float
    strategy: Literal["ladder"]
```

### 5. 策略模块（`strategy/`）

#### `edge.py`

- 比较 `p_model` 与 `p_market`
- 只保留正 EV 且超过阈值的机会

#### `kelly.py`

- 使用 fractional Kelly 对 binary options 定仓
- 结合 ensemble spread 做不确定性折扣

#### `ladder.py`

- 把 edge signals 转成 ladder positions
- 当前唯一运行中的交易策略

#### `portfolio.py`

- 预算分配默认偏重 ladder，并保留现金缓冲
- runtime 不再使用 tail allocation

### 6. 执行模块（`execution/`）

- `dry_run.py`：纸上交易 + DB 记录
- `live.py`：真实下单

### 7. 监控模块（`monitoring/`）

- `metrics.py`：P&L、Brier score、汇总统计
- `notify.py`：仅保留 stdout 格式化辅助

### 8. 存储模块（`db.py`）

SQLite 当前仅保留：

- runs
- trades
- forecasts
- bankroll snapshots

## 数据流

```text
1. Scheduler 触发
   │
2. NOAA GEFS 抓取 → member daily maxima
   │
3. ensemble.parse_distribution → ForecastDistribution
   │
4. Polymarket scan → list[MarketBucket]
   │
5. edge.detect_edges → list[EdgeSignal]
   │
6. ladder.evaluate_ladder → list[Position]
   │
7. executor.place_order
   │
8. monitoring + settlement
```

## 当前明确不做的事情

以下内容不属于当前产品定义：

- tail strategy runtime
- cross-bucket arbitrage runtime
- telegram bot runtime
- live decision loop 中的在线 calibration
- 以 Open-Meteo ensemble 作为预测主源
