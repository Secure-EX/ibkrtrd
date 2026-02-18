# 📈 Portfolio Analysis Assistant (私人投顾)

[![Python](https://img.shields.io/badge/Python-3.14-blue)](https://www.python.org/)
[![IBKR API](https://img.shields.io/badge/IBKR-TWS%20API-red)](https://interactivebrokers.github.io/tws-api/)

## 📖 项目简介 (Introduction)

这是一个基于 Python 的轻量级个人量化投研系统，专为**周频交易者**设计。它通过 `ib_insync` 连接 Interactive Brokers (IBKR) 获取即时账户与持仓数据，结合 `pandas_ta` 进行本地技术面分析，并引入 Gemini / Qwen 3.5 / Deepseek 辅助解读（计划中）。

**核心目标**：自动化数据获取流程，提供客观的技术指标分析，并执行严格的交易纪律风控（止损/仓位管理）。

## ✨ 主要功能 (Features)

- **🔌 IBKR 无缝连接**：自动连接 TWS/Gateway，获取美股、港股实时行情。
- **🔬 基本面分析**：获取基本面估值 (PE/PB) 等。
- **📊 多维技术分析**：
  - 自动计算 MA (10/20/30/60/120/200)、MACD、RSI、KDJ、BOLL (周线/月线)。
  - **走势判定**：自动识别多头/空头排列。
  - **估值分位**：基于过去一年价格计算 Price Percentile。
- **💼 持仓透视**：一键导出当前持仓、平均成本、未实现盈亏，分析持仓健康度。
- **🚦 信号生成系统**：基于技术指标的评分机制，输出 Buy/Sell/Wait 建议。
- **📉 风险控制**：自动监测止损位，触发风控预警。

## 🛠️ 技术栈 (Tech Stack)

- **核心语言**: Python 3.14
- **交易接口**: `ib_insync` (这也是 TWS API 的最佳封装)
- **数据分析**: `pandas`, `pandas_ta`, `yfinance` (用于补充 PE/PB 数据)
- **未来计划**: `Streamlit` (可视化仪表盘), `Local LLM` (Qwen/DeepSeek 用于财报分析)

## 🚀 快速开始 (Quick Start)

### 0. 项目结构 (Structure)
```
IBKR Portfolio Analysis Assistant/
├── main.py                       # 主程序入口
├── config.py                     # (可选) 配置文件，存放账户ID或端口号
├── strategies/                   # 策略模块文件夹
│   ├── __init__.py
│   ├── fundamental_analysis      # 基本面分析
│   ├── technical_analysis.py     # 技术指标计算逻辑
│   ├── risk_analysis.py          # 风险分析 (胜率/赔率)
│   └── emotional_analysis.py     # 市场情绪分析
├── data/                         # (可选) 本地数据存储
│   └── market_data.csv           # 待升级为带分区的 Postgres SQL 数据库
├── requirements.txt              # 模组列表
└── README.md                     # 项目说明文档
```

### 1. 环境要求 (Environment)
- 操作系统: Windows / macOS / Linux (本项目在 i7-6700 Windows 环境下优化测试)
- 已安装 [IBKR TWS (Trader Workstation)](https://www.interactivebrokers.com/en/trading/tws.php) 或 IB Gateway。

### 2. 安装依赖 (Dependency)
```bash
pip install ib_insync pandas pandas_ta yfinance
```

### ⚠️ 免责声明 (Disclaimer)
本项目仅用于编程学习与辅助分析，不构成任何投资建议。金融市场有风险，自动化交易可能导致资金损失，请谨慎使用。开发者不对因使用本项目产生的任何盈亏负责。
