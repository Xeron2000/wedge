# Data Sources

## Primary: Open-Meteo Ensemble API

免费，无需 API key，支持 GFS 51 成员集合预报。

### Endpoint
```
GET https://ensemble-api.open-meteo.com/v1/ensemble
```

### Parameters
| Param | Value | Notes |
|-------|-------|-------|
| latitude | 40.7128 | 城市坐标 |
| longitude | -74.0060 | |
| daily | temperature_2m_max | 日最高温 |
| models | gfs_seamless | GFS 集合 |
| forecast_days | 7 | 预报天数 |
| temperature_unit | fahrenheit | Kalshi/Polymarket 用°F |

### Response Structure
```json
{
  "daily": {
    "temperature_2m_max_member01": [78.2, 79.1, ...],
    "temperature_2m_max_member02": [77.8, 80.3, ...],
    ...
    "temperature_2m_max_member31": [79.5, 78.7, ...]
  }
}
```

### Update Frequency
- GFS 00Z → ~04:00 UTC available
- GFS 06Z → ~10:00 UTC available
- GFS 12Z → ~16:00 UTC available
- GFS 18Z → ~22:00 UTC available

## Trading Platform: Polymarket

### API: py-clob-client
```
pip install py-clob-client
```

### Key Operations
- 获取天气市场列表
- 获取订单簿（bid/ask）
- 创建限价单
- 查询持仓

### Authentication
- Ethereum 私钥签名
- API Key + Secret

## Cities (KXHIGH aligned)

| City | Lat | Lon |
|------|-----|-----|
| New York | 40.7128 | -74.0060 |
| Chicago | 41.8781 | -87.6298 |
| Miami | 25.7617 | -80.1918 |
| Los Angeles | 34.0522 | -118.2437 |
| Denver | 39.7392 | -104.9903 |

## Cross-Validation (Future)

- ECMWF IFS：最准，9km 分辨率，需付费
- HRRR：美国短期，3km，适合 1-2 天预报
- 可作为 edge 增强，不在 MVP 范围
