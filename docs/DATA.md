# 数据源

## NOAA GEFS（预测）

直接从 NOAA NOMADS 获取 GEFS ensemble 数据，不依赖第三方聚合层。

**端点**：`https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl`

**拉取内容**：
- 数据集：`gefs_atmos_0p25s`
- 变量：`TMP`
- 层级：`2 m above ground`
- Members：`c00`, `p01` ... `p30`（31 个成员）
- 预报步长：3 小时

**核心处理**：将目标自然日内多个 3 小时 step 聚合成每个 member 的本地日最高温（°F），然后构造成整体温度分布。

**运行周期**：00Z / 06Z / 12Z / 18Z

## Weather Underground（结算）

使用 IBM Weather Company API 获取历史日最高温作为 Polymarket 结算依据。Polymarket 所有温度市场均以 Wunderground 站点数据为准。

## METAR（验证）

aviationweather.gov METAR 作为实时验证层（比 Wunderground 快数小时），不用于正式结算。

## 城市坐标

坐标必须与 Polymarket 结算依赖的机场/气象站尽量一致，机场坐标优先于市中心。配置见 `src/wedge/config.py`。

## 已移除

Open-Meteo 已从主架构移除，不再使用。
