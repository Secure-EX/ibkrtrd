# 📈 Portfolio Analysis Assistant (私人投顾)

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![IBKR API](https://img.shields.io/badge/IBKR-TWS%20API-red)](https://interactivebrokers.github.io/tws-api/)

## 📖 项目简介 (Introduction)

这是一个基于 Python 的轻量级个人量化投研系统，专为**周频交易者**设计。它通过 `ib_insync` 连接 Interactive Brokers (IBKR) 获取即时账户与持仓数据，结合 `pandas_ta` 进行本地技术面分析，并引入 Gemini / Qwen 3.5 / Deepseek 辅助解读（计划中）。

**核心目标**：自动化数据获取流程，提供客观的技术指标分析，并执行严格的交易纪律风控（止损/仓位管理）。

## ✨ 主要功能 (Features)

- **🔌 IBKR 无缝连接**：自动连接 TWS/Gateway，获取美股、港股实时行情。
- **🔬 基本面分析**：获取基本面估值 (PE/PB) 等。
- **📊 多维技术分析**：
  - 自动计算 MA (10/20/30/60/120/250)、MACD、RSI、KDJ、BOLL (周线/月线) 等。
  - **走势判定**：自动识别多头/空头排列。
  - **估值分位**：基于过去3年价格计算 Price Percentile。
- **💼 持仓透视**：一键导出当前持仓、平均成本、未实现盈亏，分析持仓健康度。
- **🚦 信号生成系统**：基于技术指标的评分机制，输出 Buy/Sell/Wait 建议。
- **📉 风险控制**：自动监测止损位，触发风控预警。

## 🛠️ 技术栈 (Tech Stack)

- **核心语言**: Python 3.12
- **交易接口**: `ib_insync` (这也是 TWS API 的最佳封装)
- **数据分析**: `pandas`, `pandas-ta`, `yfinance` (用于补充 PE/PB 数据)
- **未来计划**: `Streamlit` (可视化仪表盘), `Local LLM` (Qwen/DeepSeek 用于财报分析)

## 🚀 快速开始 (Quick Start)

### 0. 📂 项目结构 (Structure)
```
IBKR Portfolio Analysis Assistant/
├── README.md                      # 项目说明文档
├── .env                           # 存放敏感信息 (IBKR Account, Ports, Client ID, Gemini/Grok API Keys等)
├── .gitignore                     # 忽略数据文件夹和 .env 推送
├── main.py                        # 主程序入口 (流水线调度器)
├── config.py                      # 全局业务参数 (回溯年限设定、重试次数等)
├── requirements.txt               # 模组列表
│
├── data_pull/                     # Extract 数据提取层
│   ├── __init__.py
│   ├── ibkr_api.py                # 专职拉取持仓快照与交易流水
│   ├── yfinance_api.py            # 专职拉取财报、基本面画像
│   └── akshare_api.py             # 专职拉取 15年日线量价、沽空等
│
├── processors/                    # Transform 数据转换层
│   ├── __init__.py
│   ├── fundamental_calc.py        # 清洗财报，计算 Z-Score, DCF, 各种 Margin
│   ├── technical_calc.py          # 重采样周/月线，计算 MA, MACD, BOLL 等
│   ├── risk_calc.py               # 账户级风控 (计算系统的胜率、赔率、最大回撤)
│   └── json_assembler.py          # 负责把算好的各个模块组装成终极大 JSON
│
├── llm_report/                    # Load 数据加载与输出层
│   ├── __init__.py
│   ├── prompt_templates.py        # 存放调教 Gemini / Grok 的系统提示词模板
│   └── report_generator.py        # 发送 JSON 给 LLM 并保存返回的 markdown 报告
│
└── data/                          # 本地数据中心 (未来无缝迁移 Postgres)
    ├── input/                     # 纯粹的原始数据 - CSV
    │   ├── portfolio/             # 按 yyyymmdd 存放 IBKR 持仓
    │   ├── transactions/          # 按 yyyymmdd 存放 IBKR 交易流水
    │   ├── ohlcv/                 # Open-High-Low-Close-Volume 个股与大盘的原始日 K 线
    │   ├── financials/            # 原始财报三表 (年报 半年报 季报) 数据
    │   └── sentiment/             # 原始每日沽空与情绪数据
    │
    └── output/                    # 加工完毕的成品 - JSON
        ├── _archive/              # [重点] 滚动冷备份，按 yyyymmdd 命名，防止数据崩溃
        ├── latest/                # [重点] 永远只存全景更新的唯一真理 (如 0700_HK.json)，供大模型直读
        └── final_reports/         # Gemini 最终输出的中文投资分析报告
```

### 1. 🌏 环境要求 (Environment)
- 操作系统: Windows / macOS / Linux (本项目在 i7-6700 Windows 环境下优化测试)
- 已安装 [IBKR TWS (Trader Workstation)](https://www.interactivebrokers.com/en/trading/tws.php) 或 IB Gateway。

### 2. 🎁 主要安装依赖 (Major Dependency)
```bash
pip install ib_insync pandas pandas_ta yfinance akshare
```

### ⚠️ 免责声明 (Disclaimer)
本项目仅用于编程学习与辅助分析，不构成任何投资建议。金融市场有风险，自动化交易可能导致资金损失，请谨慎使用。开发者不对因使用本项目产生的任何盈亏负责。
