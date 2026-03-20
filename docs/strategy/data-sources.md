# 数据源说明

## 主预测源：NOAA NOMADS GEFS

Wedge 现在使用 **direct NOAA GEFS ensemble data** 作为主预测源。
机器人会从 NOAA NOMADS 抓取 GEFS member GRIB 文件，在与机场对齐的坐标上提取 2m 温度，并基于 ensemble members 构造某个本地自然日的最高温分布。

### 端点模式

```text
https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl
```

### Wedge 实际拉取的内容

- 数据集：`gefs_atmos_0p25s`
- 变量：`TMP`
- 层级：`2 m above ground`
- Members：`c00`, `p01` ... `p30`
- 预报步长：3 小时
- Wedge 最终使用的结果：**每个 member 的本地日最高温（°F）**

### 为什么要 direct NOAA

- 去掉 Open-Meteo 这层中间包装
- 让策略与 thesis 保持一致：**直接吃 GEFS 的 repricing edge**
- 完全掌控 cycle 选择、member 处理与 daily-max 构造逻辑

### 更新时间窗口

GEFS 运行时间：

- 00Z
- 06Z
- 12Z
- 18Z

Wedge 会在这些 cycle 理论上已经出现在 NOMADS 之后的时间点调度扫描。

## 结算观测源：Open-Meteo Archive

Wedge 仍然使用 Open-Meteo Archive 获取结算时所需的历史观测日最高温。
这属于结算便利性，不属于策略 edge 本身。

## 交易平台：Polymarket

### Client

```text
py-clob-client
```

### 关键能力

- 发现天气市场
- 读取 bucket 当前价格
- 下限价单
- 跟踪持仓

## 城市坐标

城市坐标必须与 Polymarket 结算依赖的机场/气象站尽量一致。
机场坐标比 city center 更重要。

当前默认城市定义见 `src/wedge/config.py`。

## 明确不再属于主架构的内容

以下内容已经不再属于当前主产品设计：

- Open-Meteo ensemble 作为主预测源
- Tail 策略 runtime
- Cross-bucket arbitrage runtime
- Telegram runtime
- Live decision loop 中的在线 calibration
