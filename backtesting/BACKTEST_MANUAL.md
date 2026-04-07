# 回测系统使用手册

> 适用版本：当前 `ibkrtrd` 项目 / 2026-04  
> 入口文件：`backtesting/run_backtest.py`

---

## 目录

1. [快速开始](#1-快速开始)
2. [完整命令行参数](#2-完整命令行参数)
3. [策略详解](#3-策略详解)
4. [仓位管理](#4-仓位管理)
5. [止损机制](#5-止损机制)
6. [信号确认过滤器](#6-信号确认过滤器)
7. [典型用法示例](#7-典型用法示例)
8. [输出文件说明](#8-输出文件说明)
9. [作为 Python 函数调用](#9-作为-python-函数调用)
10. [策略参数速查表](#10-策略参数速查表)

---

## 1. 快速开始

```bash
# 进入项目根目录
cd C:\Users\secur\Desktop\Stock\ibkrtrd

# 基础运行（单股，默认多因子策略）
PYTHONIOENCODING=utf-8 python -m backtesting.run_backtest --ticker 0700.HK

# 带图表
PYTHONIOENCODING=utf-8 python -m backtesting.run_backtest --ticker 0700.HK --plot

# 批量运行所有持仓股票
PYTHONIOENCODING=utf-8 python -m backtesting.run_backtest --all
```

> **Windows 注意**：`PYTHONIOENCODING=utf-8` 前缀可避免中文乱码。

---

## 2. 完整命令行参数

```
python -m backtesting.run_backtest [OPTIONS]
```

### 2.1 目标标的

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--ticker TICKER` | str | — | 单只股票代码，如 `0700.HK` |
| `--all` | flag | False | 对所有可用股票批量运行 |

> `--ticker` 和 `--all` 二选一。

---

### 2.2 时间范围

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--start DATE` | str | `2015-01-01` | 回测起始日期（含热身期） |
| `--end DATE` | str | 最新可用数据 | 回测结束日期 |

> 实际第一笔交易约在 `start` + `warmup_days` 个交易日之后。

---

### 2.3 资金与仓位

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--capital FLOAT` | float | `1000000.0` | 初始资金（HKD） |
| `--fraction FLOAT` | float | `0.25` | 该股最大总仓位占初始资金的比例（0~1） |
| `--max-tranches INT` | int | `3` | 最大分批建仓次数（等分模式） |
| `--board-lot INT` | int | 自动检测 | 每手股数；省略时从持仓 CSV 或内置表自动读取 |
| `--pyramid` | flag | False | 启用金字塔建仓（自动按可承受手数分配权重） |

**金字塔权重自动计算规则（`--pyramid` 启用时）：**

| 可承受总手数 | 分批数 | 各批权重 |
|------------|--------|---------|
| ≥ 10 手 | 4 批 | 10% → 20% → 30% → 40% |
| ≥ 5 手 | 3 批 | 20% → 30% → 50% |
| ≥ 3 手 | 2 批 | 40% → 60% |
| < 3 手 | 1 批 | 100% |

---

### 2.4 策略选择

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--strategy NAME` | str | `multifactor_risk` | 策略名称（见下方列表） |

可用策略名：

| 策略名 | 类型 | 适用场景 |
|--------|------|---------|
| `multifactor_risk` | 综合/默认 | 所有股票，主策略 |
| `technical_momentum` | 技术动量 | 趋势明显的市场 |
| `composite` | 双重确认 | 希望减少假信号 |
| `custom` | 自定义规则 | 灵活配置（需代码传参） |
| `valuation_reversion` | 保守型 | 蓝筹股（腾讯、中海油等） |
| `dual_momentum` | 中性型 | 全部股票，规避长期阴跌 |
| `atr_trend` | 激进型 | 高波动成长股（泡泡玛特等） |

---

### 2.5 买卖阈值（multifactor_risk / composite 策略）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--buy-threshold FLOAT` | float | `0.05` | 多因子长线风险 < 此值时买入（机会区） |
| `--sell-threshold FLOAT` | float | `0.95` | 多因子长线风险 > 此值时卖出（风险区） |

> 风险值范围 0~1；值越小代表越安全，越大代表风险越高。

---

### 2.6 估值回归策略专用参数（valuation_reversion）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--z-buy FLOAT` | float | `-1.5` | PE/PB Z-score < 此值时买入（历史极低区间） |
| `--z-sell FLOAT` | float | `1.5` | PE/PB Z-score > 此值时卖出（历史极高区间） |

> Z-score = (当前估值 − 3年均值) / 3年标准差；  
> -1.5 ≈ 历史最低的约 6.7% 时间段。

---

### 2.7 止损设置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--stop-loss FLOAT` | float | `-0.30` | 固定止损比例（负数），动态止损禁用时生效 |
| `--dynamic-stop` | flag | True | 启用 ATR 移动止损（默认开启） |
| `--no-dynamic-stop` | flag | — | 禁用动态止损，回退到 `--stop-loss` 固定值 |
| `--stock-type TYPE` | str | 自动检测 | `blue_chip` / `growth` / `high_volatility` |

**动态止损 ATR 倍数（按股票类型）：**

| 股票类型 | ATR 倍数 | 含义 | 示例股票 |
|---------|---------|------|---------|
| `blue_chip` | 3.0× | 宽容，减少频繁止损 | 腾讯 (0700)、中海油 (0883)、中升 (0881) |
| `growth` | 2.5× | 平衡 | 赤子城 (9992 之外)、XD (2400) |
| `high_volatility` | 2.0× | 较紧，快速锁定利润 | 泡泡玛特 (9992)、理想 (2015) |

> 止损线只上移，不下调；绝对底线为入场价 × (1 − 0.40)（最大亏损 -40%）。

---

### 2.8 调仓频率与热身期

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--freq FREQ` | str | `weekly` | `daily` / `weekly` / `monthly` |
| `--warmup INT` | int | `260` | 信号热身天数（约 1 年，热身期内不交易） |

> 推荐：`multifactor_risk` / `composite` 用 `weekly`；`dual_momentum` 用 `monthly`；`atr_trend` 用 `daily`。

---

### 2.9 信号确认过滤器

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--confirmation-weeks INT` | int | `0` | 0 = 禁用；N = 首次建仓需连续 N 个决策日出现买入信号 |

> 仅对空仓→首批建仓生效；后续加仓不需重新确认。  
> 注意：开启后交易次数会显著减少（降噪效果明显）。

---

### 2.10 其他

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--plot` | flag | False | 生成权益曲线 + 回撤图表（PNG 文件） |

---

## 3. 策略详解

### 3.1 多因子风险策略 `multifactor_risk`（推荐/默认）

**逻辑优先级（高→低）：**

1. 逐批检查固定止损 → `SELL_TRANCHE`（仅平该批，不影响其他）
2. 长线风险 > `sell_threshold` → `SELL`（清全部）
3. 长线风险 < `buy_threshold` + 短线未过热 → `BUY`（加一批）
4. 其他 → `HOLD`

**关键参数：**
- `buy_threshold`（默认 0.05）：长线风险极低，机会区
- `sell_threshold`（默认 0.95）：长线风险极高，风险区
- `short_term_filter`（默认 True）：买入时过滤短线超买
- `short_term_buy_max`（默认 0.80）：短线风险上限

---

### 3.2 技术动量策略 `technical_momentum`

**逻辑：**
- 买入：均线多头排列 + RSI 不超买 + 价格在 MA250 上方
- 卖出：均线空头排列 或 RSI 超买
- 特殊：RSI 超卖时可半仓反弹买入（`size_hint=0.5`）

---

### 3.3 复合策略 `composite`（双重确认）

**逻辑：**
- 买入：长线风险 < `buy_threshold` **且**（均线多头 **或** RSI 超卖）
- 卖出：长线风险 > `sell_threshold` **或** 均线空头

> 比 `multifactor_risk` 更保守，假信号更少但交易次数也更少。

---

### 3.4 估值均值回归策略 `valuation_reversion`（保守型）

**核心思想：** PE / PB 偏离历史均值时会均值回归；极度低估买入，极度高估卖出。

**逻辑：**
- 买入：avg(PE_zscore, PB_zscore) < `z_buy_threshold`（默认 -1.5）
- 卖出：avg Z-score > `z_sell_threshold`（默认 1.5）
- 附加过滤：RSI 超买时延迟买入，等待回调

**适用股票：** 有长期财报数据的蓝筹（腾讯、中海油、中升集团）

**推荐调仓频率：** `weekly`

---

### 3.5 双动量策略 `dual_momentum`（中性型）

**核心思想：** 绝对动量 > 无风险利率才持有；动量转负立即出场，切换现金。

**逻辑：**
- 买入：过去 12 个月回报 > 无风险利率（4%）
- 卖出：过去 12 个月回报 < 0%（绝对动量转负）
- 优势：能有效规避长期阴跌，如 2021–2022 港股熊市

**推荐调仓频率：** `monthly`

---

### 3.6 ATR 趋势跟踪策略 `atr_trend`（激进型）

**核心思想：** KAMA（自适应均线）判断趋势 + 成交量放大确认入场；配合 ATR 动态止损。

**逻辑：**
- 买入：价格 > KAMA + KAMA 趋势向上 + 成交量 > 20 日均量 1.5 倍
- 加仓：已有仓位 + KAMA 趋势延续（不要求放量）
- 卖出：价格 < KAMA + KAMA 趋势向下

**适用股票：** 高波动成长股（泡泡玛特 9992、理想汽车 2015）

**推荐调仓频率：** `daily`

---

## 4. 仓位管理

### 等分模式（默认）

每批建仓额 = `initial_capital × fixed_fraction / max_tranches`

```
例：100万 × 25% / 3 = 每批约 83,333 HKD
```

### 金字塔模式（`--pyramid`）

自动根据**当前股价下可承受总手数**决定分批数和权重（首批最小，逐批递增）。

```bash
# 启用金字塔建仓
--pyramid

# 配合最大分批数上限（金字塔模式下此参数作为上限参考）
--max-tranches 4
```

---

## 5. 止损机制

### 动态 ATR 移动止损（默认启用）

```
止损线 = max(
    peak_price - multiplier × current_ATR,   # ATR 移动止损
    entry_price × (1 - 0.40)                  # 绝对底线
)
```

- `peak_price`：持仓以来的最高价，只增不减
- 止损线随 peak_price 上移，形成"移动止损"
- 触发后：平掉该批次（`SELL_TRANCHE`），其他批次保持

### 固定止损（`--no-dynamic-stop`）

```bash
--no-dynamic-stop --stop-loss -0.25   # 浮亏超过 -25% 止损
```

---

## 6. 信号确认过滤器

```bash
--confirmation-weeks 3   # 首次建仓需连续 3 个决策日出现买入信号
```

**效果对比（腾讯 0700.HK，multifactor_risk）：**

| 模式 | 交易次数 | 总收益 |
|------|---------|--------|
| 无确认 | 37 次 | 27.97% |
| 3周确认 | 5 次 | 5.11% |

> 信号确认减少了噪音交易，但也会延误入场。根据交易风格选择。

---

## 7. 典型用法示例

### 7.1 基础单股回测

```bash
# 腾讯，默认参数
python -m backtesting.run_backtest --ticker 0700.HK

# 自定义时间范围
python -m backtesting.run_backtest --ticker 0700.HK --start 2018-01-01 --end 2024-12-31

# 生成图表
python -m backtesting.run_backtest --ticker 0700.HK --plot
```

### 7.2 策略选择

```bash
# 估值回归（适合腾讯/中海油等蓝筹）
python -m backtesting.run_backtest --ticker 0700.HK --strategy valuation_reversion

# 双动量（月频，规避熊市）
python -m backtesting.run_backtest --ticker 0700.HK --strategy dual_momentum --freq monthly

# ATR 趋势跟踪（适合泡泡玛特等高波动股）
python -m backtesting.run_backtest --ticker 9992.HK --strategy atr_trend --freq daily
```

### 7.3 金字塔建仓 + 宽松止损

```bash
# 中海油：蓝筹股，宽容止损，金字塔建仓
python -m backtesting.run_backtest --ticker 0883.HK \
    --strategy valuation_reversion \
    --pyramid \
    --stock-type blue_chip \
    --freq weekly

# 泡泡玛特：高波动，紧止损，趋势跟踪
python -m backtesting.run_backtest --ticker 9992.HK \
    --strategy atr_trend \
    --stock-type high_volatility \
    --freq daily \
    --plot
```

### 7.4 信号确认 + 估值策略

```bash
# 保守型：估值回归 + 3周确认期 + 金字塔
python -m backtesting.run_backtest --ticker 0700.HK \
    --strategy valuation_reversion \
    --confirmation-weeks 3 \
    --pyramid \
    --z-buy -2.0 \
    --z-sell 2.0
```

### 7.5 调整买卖阈值（multifactor_risk）

```bash
# 更保守的买入（更严格机会区）
python -m backtesting.run_backtest --ticker 0700.HK \
    --buy-threshold 0.03 --sell-threshold 0.90

# 更激进的买入
python -m backtesting.run_backtest --ticker 0700.HK \
    --buy-threshold 0.10 --sell-threshold 0.85
```

### 7.6 禁用动态止损（固定止损回测）

```bash
python -m backtesting.run_backtest --ticker 0700.HK \
    --no-dynamic-stop --stop-loss -0.25
```

### 7.7 批量运行所有股票

```bash
# 所有持仓股，默认策略
python -m backtesting.run_backtest --all

# 所有持仓股，ATR 趋势策略，日频
python -m backtesting.run_backtest --all --strategy atr_trend --freq daily

# 所有持仓股，估值回归，生成图表
python -m backtesting.run_backtest --all --strategy valuation_reversion --plot
```

---

## 8. 输出文件说明

输出目录：`data/output/backtest/bt_{TICKER}_{STRATEGY}/`

| 文件名 | 内容 |
|--------|------|
| `performance_summary.json` | 所有绩效指标（总收益、夏普、最大回撤、胜率等） |
| `trade_log.csv` | 每笔交易明细（日期、方向、价格、手数、盈亏、原因） |
| `equity_curve.csv` | 每日权益曲线（组合净值 + 回撤） |
| `equity_curve.png` | 权益曲线 + 回撤图（`--plot` 时生成） |

**关键绩效指标说明：**

| 指标 | 说明 |
|------|------|
| `total_return_pct` | 区间总收益率 (%) |
| `annualized_return_pct` | 年化收益率 (%) |
| `annualized_volatility_pct` | 年化波动率 (%) |
| `sharpe_ratio` | 夏普比率（风险调整收益）|
| `max_drawdown_pct` | 最大回撤 (%) |
| `max_drawdown_duration_days` | 最大回撤持续天数 |
| `calmar_ratio` | Calmar 比率（年化收益 / 最大回撤）|
| `win_rate_pct` | 胜率 (%) |
| `profit_factor` | 盈亏比（总盈利 / 总亏损）|
| `avg_holding_days` | 平均持仓天数 |
| `alpha_pct` | 相对恒生指数的超额收益 (%) |

---

## 9. 作为 Python 函数调用

```python
from backtesting.run_backtest import run_backtest

# 基础调用
metrics = run_backtest("0700.HK")

# 完整参数
metrics = run_backtest(
    ticker="0700.HK",
    strategy_name="multifactor_risk",   # 策略名
    start_date="2015-01-01",            # 起始日期
    end_date=None,                      # 截止日期（None=最新）
    initial_capital=1_000_000.0,        # 初始资金 HKD
    fixed_fraction=0.25,                # 最大总仓位比例
    max_tranches=3,                     # 最大分批数
    buy_threshold=0.05,                 # 买入阈值
    sell_threshold=0.95,                # 卖出阈值
    stop_loss_pct=-0.30,                # 固定止损（动态止损禁用时）
    short_term_filter=True,             # 短线过热过滤
    rebalance_freq="weekly",            # 调仓频率
    warmup_days=260,                    # 热身期（交易日数）
    plot=True,                          # 生成图表
    board_lot=None,                     # None=自动检测
    pyramid=False,                      # 金字塔建仓
    confirmation_weeks=0,               # 信号确认周数
    dynamic_stop=True,                  # ATR 动态止损
    stock_type=None,                    # None=自动分类
    z_buy=-1.5,                         # 估值回归买入 Z-score
    z_sell=1.5,                         # 估值回归卖出 Z-score
)

print(metrics["annualized_return_pct"])  # 年化收益率
print(metrics["sharpe_ratio"])           # 夏普比率
```

---

## 10. 策略参数速查表

| 策略 | 核心参数 | 推荐频率 | 适用股票 |
|------|---------|---------|---------|
| `multifactor_risk` | `buy_threshold=0.05, sell_threshold=0.95` | weekly | 全部 |
| `technical_momentum` | `stop_loss_pct=-0.20` | weekly | 趋势市 |
| `composite` | `buy_threshold=0.08, sell_threshold=0.90` | weekly | 全部 |
| `valuation_reversion` | `z_buy=-1.5, z_sell=1.5` | weekly | 蓝筹（需财报数据） |
| `dual_momentum` | `risk_free_rate=0.04, exit_threshold=0.0` | monthly | 全部 |
| `atr_trend` | `require_volume_breakout=True` | daily | 高波动成长 |

### 各策略已验证回测结果参考

| 股票 | 策略 | 年化收益 | 夏普 | 最大回撤 |
|------|------|---------|------|---------|
| 0700.HK 腾讯 | multifactor_risk (动态止损) | 27.97% | 1.27 | -6.52% |
| 0700.HK 腾讯 | multifactor_risk (固定止损) | 45.80% | 1.72 | -8.80% |
| 0700.HK 腾讯 | valuation_reversion | 26.77% | 1.04 | -5.86% |
| 0700.HK 腾讯 | dual_momentum (月频) | 40.96% | 3.34 | -7.71% |
| 9992.HK 泡泡玛特 | atr_trend (日频) | 51.73% | 3.41 | -4.25% |
| 0883.HK 中海油 | valuation_reversion + pyramid | — | — | 验证金字塔手数递增 ✓ |

---

*使用问题请检查 `data/output/backtest/` 下的 `trade_log.csv` 和 `performance_summary.json` 进行诊断。*
