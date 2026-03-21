# 策略

## 核心逻辑

```
NOAA GEFS → 概率分布 → Polymarket 比较 → EV 计算 → 仓位
```

1. **预测**：直接抓 NOAA NOMADS GEFS，以机场对齐坐标提取 2m 温度
2. **分布**：聚合成 member 本地日最高温，构造成温度 bucket 概率分布
3. **比较**：对比模型概率 `p_model` 与市场价格 `p_market`
4. **决策**：计算扣 fee/slippage 后的 EV，只在 EV > 0 时行动

## EV 公式

```
EV = p_model × (1 - fee) × odds - (1 - p_model) - slippage
odds = (1 - p_market) / p_market
```

默认 `fee = 2%`，slippage 根据成交量估算。

## Ladder 策略

只在市场明显低估的温度区间建仓：
- `ladder_edge = 0.08`（至少 8% 概率偏差才入候选集）
- 按 edge 从高到低排序，优先吃最错价的 bucket
- `ladder_alloc = 0.90`（可用资金比例）

## Kelly 仓位

使用 fractional Kelly 控制仓位：
- `kelly_fraction = 0.15`
- `max_bet` / `max_bet_pct` 硬上限
- `ensemble_spread` 不确定性折扣
- `fat-tail discount`

## 退出规则

**止损**：`p_model < entry_price × 0.5`，默认 `exit_loss_factor = 0.5`

**优势消失**：`p_model >= entry_price` 但 `EV <= 0`，市场已修复，提前离场

**临近结算**：距离结算过近时不频繁操作，避免被最后噪音洗出

## 结算流程

1. 每日运行 settlement job
2. 从 Weather Underground 获取历史日最高温
3. 完成 settled PnL 计算
4. Archive observed temperatures

## 风控措施

- Brier threshold 全局暂停
- max bet 与 max bet percentage 限制
- 预报反向或优势消失时提前退出

## 已移除

Tail 策略、cross-bucket arbitrage、telegram runtime、live 在线 calibration 均已从架构中移除。
