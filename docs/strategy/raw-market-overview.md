基于深度搜索，我整理了推特天气预测市场交易员的完整策略：

  顶级交易员

  neobrother（Polymarket）
  - 盈利：$20,000+
  - 交易次数：2,373次
  - 策略：温度阶梯套利
  - 特点：高度自动化，专注天气市场

  Hans323（Polymarket）
  - 单笔最大盈利：$1.11M（投入$92,632）
  - 策略：黑天鹅猎手，重注低概率事件（2%-8%）
  - 风格：Taleb杠铃策略

  核心盈利逻辑

  温度阶梯策略（neobrother）：
  - 在同一城市同时买入多个温度档位（如29°C、30°C、31°C、32°C、33°C）
  - 每个仓位成本极低（0.2¢-15¢）
  - 只要实际温度落在区间内，单个高赔率仓位（如811%回报）覆盖所有损失

  赚谁的钱：
  1. 市场定价滞后 - 天气模型更新快于市场反应
  2. 散户认知差 - 不懂气象模型的普通玩家
  3. 流动性提供者 - 做市商的定价偏差

  数据源（全免费）

  气象模型：
  - ECMWF IFS（最准，9km分辨率）
  - NOAA GFS（31成员集合预报，免费）
  - HRRR（美国短期，3km分辨率）
  - Open-Meteo API（免费，无需API key）

  交易平台：
  - Kalshi（KXHIGH系列：纽约/芝加哥/迈阿密/洛杉矶/丹佛）
  - Polymarket（更多城市选择）

  自动化参与方案

  开源机器人：
  # GitHub: suislanchez/polymarket-kalshi-weather-bot
  git clone https://github.com/suislanchez/polymarket-kalshi-weather-bot
  cd polymarket-kalshi-weather-bot
  pip install -r requirements.txt
  uvicorn backend.api.main:app --reload --port 8000

  核心功能：
  - 每5分钟扫描Kalshi/Polymarket天气市场
  - 使用31成员GFS集合预报计算概率
  - 当edge > 8%时自动下单
  - Kelly准则仓位管理（15% fractional Kelly）
  - 单笔上限$100

  平仓策略

  自动平仓触发：
  1. 价格收敛：结算临近时，预测准确的仓位价格从$0.65涨到$0.95+，提前获利
  2. 止损：单笔最大损失=买入成本（如$0.7买入，最多亏$0.7）
  3. Kelly调整：根据bankroll动态调整仓位，最多5%资金

  无需盯盘：
  - 机器人24/7运行
  - APScheduler定时任务
  - Webhook通知（可选）

  立即参与步骤

  1. 注册账户：
    - Kalshi（需KYC，美国用户）
    - Polymarket（链上钱包，全球可用）
  2. 部署机器人：
  # 克隆开源项目
  git clone https://github.com/suislanchez/polymarket-kalshi-weather-bot

  # 配置API密钥（可选，模拟模式无需）
  # Kalshi API key
  # Polymarket私钥

  # 启动
  uvicorn backend.api.main:app --reload
  3. 模拟模式测试：
    - 先用paper trading验证策略
    - 观察Brier score（预测准确度）
    - 确认edge detection有效
  4. 小资金实盘：
    - 初始$500-1000
    - 单笔上限$50-100
    - 运行1-2周验证

  风险提示

  - neobrother可能接入专业气象API（比Open-Meteo更准）
  - 需要理解集合预报的概率分布
  - 市场流动性有限，大资金难以进出
  - 天气模型也有误差，不是100%准确

  关键：这是数据驱动的量化策略，不是赌博。成功的核心是比市场更准确地理解气象模型。
