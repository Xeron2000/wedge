# 最终版 NOAA Ladder-Only 设计文档

## 目标

Wedge 被明确收缩为单一生产级 thesis：

> 使用 direct NOAA GEFS ensemble data 估计某城市本地日最高温的概率分布，将该分布与 Polymarket 温度 bucket 定价做比较，并且只在 expected value 为正时执行 ladder entries。

## 策略定义

### 预测 edge

- 数据源：**direct NOAA NOMADS GEFS**
- 解析粒度：先聚合成 member 的本地日最高温，再构造成整体温度分布
- 比较对象：Polymarket 天气 bucket 价格
- 触发条件：正 EV 且达到 ladder 阈值

### 持仓构造

- 策略：**ladder only**
- 定仓：fractional Kelly，并结合 ensemble spread 做不确定性折扣
- 执行：通过现有 dry-run / live executor 进行 buy-only 的 binary option 进场

### 风控与控制

- 全局 Brier threshold 暂停
- max bet 与 max bet percentage 限制
- 基于概率变化的提前退出规则
- 每日 settlement 流程与 archive observed temperatures

## Runtime 架构

### 1. Scheduler

在配置好的 UTC offsets 上运行主 pipeline，并每日运行一次 settlement job。
不再存在 arbitrage side scheduler，也不存在 telegram 进程。

### 2. Weather ingest

`src/wedge/weather/client.py`

- 从 NOAA NOMADS 拉取 GEFS member 的 GRIB subset
- 在与机场对齐的坐标上提取 2m 温度
- 把 forecast steps 聚合成每个 member 的 daily maximum
- 输出 NOAA-native 结构化 payload

`src/wedge/weather/ensemble.py`

- 把 member daily maxima 转换为 `ForecastDistribution`
- 计算 bucket probabilities 和 ensemble spread

### 3. Market scan

`src/wedge/market/scanner.py`

- 加载指定 city/date 的 Polymarket 天气市场
- 构造标准化 market bucket models

### 4. Edge + ladder selection

`src/wedge/strategy/edge.py`

- 扣除 fee / slippage 之后计算 EV
- 只在 EV 和 threshold 都为正时生成 edge signal

`src/wedge/strategy/ladder.py`

- 将 edge signals 转换为 ladder positions
- 使用 fractional Kelly 输出定仓结果

### 5. Execution + persistence

`src/wedge/execution/*`

- dry run 与 live execution 共享同一条策略路径

`src/wedge/db.py`

- 仅保留 runs、forecasts、trades、bankroll snapshots
- legacy arbitrage / city-performance schema 已从 runtime 设计中移除

## 明确不做的内容

以下能力已经被有意移出设计：

- tail strategy runtime
- cross-bucket arbitrage runtime
- telegram bot / notifier runtime
- live decision loop 中的在线 calibration
- Open-Meteo ensemble 作为预测主源
- city-level 动态 performance filtering

## 仍然保留的辅助能力

以下能力被保留，是因为它们对运行有帮助，但并不构成 edge 本身：

- Open-Meteo archive 作为 settlement observations 来源
- dry-run synthetic market generation，用于离线/测试场景
- backtest summary command，用于查看历史 settled trades

## 为什么采用这个设计

这个设计遵循 KISS 与 Occam's razor：

- 一个预测源
- 一个执行策略
- 一条决策闭环
- 一个清晰可解释的赚钱逻辑

仓库中的每个保留部分，都应该直接支撑这条闭环；否则就应该被删除。
