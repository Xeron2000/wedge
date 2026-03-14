# Trading Strategy Overview

> 来源：1.md（市场概览）+ 2.md（数学推导）

## Core Thesis

天气预测市场存在系统性定价偏差：GFS 集合预报比市场参与者更准确地量化温度概率分布。利用这个信息差赚钱。

## Edge Sources（按可靠性排序）

### 1. 模型更新时间差（最硬）
- GFS 每 6h 更新：00Z / 06Z / 12Z / 18Z
- 数据延迟 ~4h，发布后 30min 内 edge 最大
- 每天 4 个交易窗口

### 2. 散户认知偏差（最稳定）
- 锚定效应：用昨天温度预测明天
- 忽视 ensemble spread：只看点预测
- 近因偏差：连续几天同温度 → 认为不变

### 3. 做市商定价偏差（最大但不稳定）
- 各档位价格之和 ≠ 100%
- 尾部被系统性错误定价
- 做市商用正态分布，天气常是偏态

## Dual-Layer Strategy

### Layer 1: Range Ladder（70% 资金）
- 买入模型预测概率中心区域 ±1σ 的 3-5 个温度档位
- 触发条件：range edge > 5%
- Kelly：15% fractional
- 预期：~30 次/月，低波动稳定收益

### Layer 2: Tail Hunter（20% 资金）
- 买入极端温度档位（market price 2-8%）
- 触发条件：ensemble ≥3 成员预测 + edge > 8% + 赔率 > 10:1
- Kelly：1-2% bankroll per bet
- 预期：~5 次/月，高方差高回报

### Layer 3: Cash Reserve（10%）
- 应对 drawdown 和突发机会

## Position Sizing (Kelly Criterion)

```
b = ($1 - cost) / cost              # 赔率
f* = (p × b - q) / b               # 满 Kelly
f_actual = f* × 0.15               # 15% 分数 Kelly
bet = min(f_actual × bankroll, $100) # 单笔上限
```

Ed Thorp 原则：永远用分数 Kelly，满 Kelly 的波动会杀死你。

## Example: NYC 3-Day High Temp

| Temp(°F) | Members | P_model | Market | P_market |
|----------|---------|---------|--------|----------|
| ≤76      | 3       | 9.7%    | $0.11  | 11.0%    |
| 77       | 4       | 12.9%   | $0.10  | 10.0%    |
| 78       | 7       | 22.6%   | $0.18  | 18.0%    |
| 79       | 8       | 25.8%   | $0.22  | 22.0%    |
| 80       | 5       | 16.1%   | $0.18  | 18.0%    |
| 81       | 3       | 9.7%    | $0.10  | 10.0%    |
| ≥82      | 1       | 3.2%    | $0.06  | 6.0%     |

Ladder: 买入 77-80°F
- 总成本 = $0.68
- P_model = 77.4%，P_market = 68.0%
- Edge = 9.4%
- EV = +$0.094 / 组

## Risk Controls

| 控制 | 阈值 | 动作 |
|------|------|------|
| Brier Score | > 0.25 | 暂停交易 |
| 单笔上限 | $100 | 硬限制 |
| 单笔 bankroll % | 5% | Kelly 上限 |
| 日亏损 | > 10% bankroll | 暂停当日 |
| 流动性 | < $50 可用量 | 跳过该合约 |

## Revenue Projection

$1000 本金，保守估算（理论值打 5 折）：
- 月收益 ~$100-150
- 年化 ~150-300%
- 资金天花板 ~$5000-10000（流动性限制）
