# Portfolio Analysis Assistant (私人投顾)

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![IBKR API](https://img.shields.io/badge/IBKR-TWS%20API-red)](https://interactivebrokers.github.io/tws-api/)

## 项目简介

基于 Python 的轻量级个人量化投研系统，专为**港股周频交易者**设计。

**核心工作流**：连接 IBKR 自动获取持仓 → 批量拉取量价/财报/新闻数据 → 本地计算技术与基本面指标 → 组装 LLM 分析载荷 → 粘贴至 Claude/Gemini/Grok 生成中文研报。

---

## 主要功能

| 模块 | 功能 |
|------|------|
| **IBKR 连接** | 自动连接 TWS/Gateway，获取实时持仓快照、OHLCV K线 |
| **多源数据拉取** | yfinance (财报/基本面底线) + AkShare (东方财富覆盖) + 新闻舆情聚合 |
| **技术分析** | MA (10/20/30/60/120/250)、MACD、RSI、KDJ、BOLL（日/周/月三级别），自动判定多空排列，计算3年估值分位 |
| **基本面分析** | 财报三表清洗、Z-Score、DCF 估值、各类利润率计算 |
| **风险管理** | 账户级风控报告（胜率、赔率、最大回撤、止损位监测） |
| **LLM 载荷组装** | 将所有分析数据聚合为结构化 JSON + Prompt，供 Claude/Gemini/Grok 直读 |

---

## 项目结构

```
ibkrtrd/
├── main.py                    # 主程序入口，四阶段流水线调度器
├── config.py                  # 全局参数 (路径、API密钥、回溯年限等)
├── requirements.txt
├── user_notes.json            # 用户手动录入的个股备注、交易信息、摘抄文本
│
├── data_pull/                 # [第一层] 数据提取
│   ├── ibkr_api.py            # IBKR 持仓快照 + K线主引擎
│   ├── yfinance_api.py        # 财报/基本面底线 + K线备用引擎
│   ├── akshare_api.py         # 东方财富财报数据（覆盖 yfinance）
│   └── news_api.py            # 新闻与舆情拉取（东方财富 + Google News）
│
├── processors/                # [第二层] 数据处理与分析
│   ├── fundamental_calc.py    # 财报清洗、Z-Score、DCF、Margin 计算
│   ├── technical_calc.py      # K线重采样，技术指标主调度
│   ├── technical_indicators.py
│   ├── technical_financial.py
│   ├── technical_market.py
│   ├── technical_multifactor.py
│   ├── technical_risk.py
│   ├── technical_utils.py
│   ├── risk_calc.py           # 账户级风控报告
│   ├── sentiment_calc.py      # 情绪与新闻评分
│   ├── transaction_parser.py  # 交易流水组装
│   └── json_assembler.py      # 将各模块聚合为终极 LLM 载荷 JSON
│
├── llm_report/                # [第三层] 报告与 Prompt 生成
│   ├── prompt_template.py     # 系统提示词模板（中文深度研报格式）
│   └── report_generator.py    # 调用 Claude CLI 自动保存 Markdown 报告
│
└── data/
    ├── input/
    │   ├── ohlcv/             # 个股日K线 CSV（15年回溯）
    │   ├── financials/        # 财报三表 CSV（年报/季报）
    │   ├── portfolio/         # IBKR 持仓快照（按日）
    │   ├── transactions/      # 交易流水汇总
    │   └── sentiment/         # 沽空与情绪原始数据
    └── output/
        ├── latest/            # [核心] 最新单股 LLM 载荷 JSON（如 0700.HK_LLM_Payload.json）
        │   └── web_prompts_yyyymmdd/  # 切分后的小 JSON（减少粘贴 token）
        ├── final_reports/     # LLM 输出的 Markdown 研报
        └── _archive/          # 手动冷备份
```

---

## 快速开始

### 1. 环境依赖

- Python 3.12
- 已安装并登录 [IBKR TWS](https://www.interactivebrokers.com/en/trading/tws.php) 或 IB Gateway（端口 7496 实盘 / 7497 模拟）

```bash
pip install -r requirements.txt
```

### 2. 配置 `.env`

在项目根目录创建 `.env` 文件：

```env
IBKR_ACCOUNT_ID=your_account_id
IBKR_HOST=127.0.0.1
IBKR_PORT=7496
IBKR_CLIENT_ID=1

CLAUDE_API_KEY=your_claude_key
GEMINI_API_KEY=your_gemini_key
GROK_API_KEY=your_grok_key
```

### 3. 运行

```bash
python main.py
```

**四阶段流水线**：

1. **第零阶段** — 拉取恒生指数/科技指数大盘参照数据
2. **第一阶段** — IBKR 账户扫描，生成账户风控报告
3. **第二阶段** — 持仓标的逐个处理（OHLCV → 财报 → 新闻 → JSON 组装）
4. **第三阶段** — 将所有单股 JSON 聚合为终极 API Prompt

完成后，将 `data/output/latest/` 下的 JSON 或 Prompt 文本粘贴至 Claude/Gemini/Grok 网页端生成研报。

---

## 关键配置参数（config.py）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `LOOKBACK_YEARS` | 15 | K线历史回溯年限（覆盖完整牛熊周期） |
| `FINANCIAL_REPORT_YEARS` | 4 | 喂给 LLM 的年度财报数量 |
| `FINANCIAL_REPORT_QTERS` | 8 | 喂给 LLM 的季报数量 |
| `RISK_FREE_RATE` | 0.04 | 夏普比率无风险利率假设 |
| `INDEX_SYMBOLS` | `^HSI`, `3033.HK` | 大盘参照指数 |

---

## 免责声明

> 反正开发者也还没赚钱，图一乐。

本项目仅用于编程学习与个人辅助分析，不构成任何投资建议。金融市场存在风险，开发者不对因使用本项目产生的任何盈亏负责。
