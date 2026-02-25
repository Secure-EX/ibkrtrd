import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# 加载 .env 文件中的敏感信息
load_dotenv()

# === 1. 基础路径配置 ===
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# === 2. 动态日期分区 (Partition Key) ===
# 获取今日日期，例如: 20260218
# 未来这对应数据库 partition key: values from ('2026-02-18')
TODAY_STR = datetime.now().strftime("%Y%m%d")
CURRENT_YEAR = datetime.now().strftime("%Y") # 用于交易流水按年滚动

# === 3. 定义子目录结构 ===

# 3.0 定义根输入输出路径
INPUT_ROOT = DATA_DIR / "input"
OUTPUT_ROOT = DATA_DIR / "output"

# 3.1 输入层 (Input: 生数据 CSV)
# 按日分区的流式数据 (每天生成新文件或覆写文件)
PORTFOLIO_DIR = INPUT_ROOT / "portfolio"                      # 持仓快照
TRANSACTIONS_DIR = INPUT_ROOT / "transactions"                # 交易流水

# 平铺覆盖的历史主数据 (直接覆写文件，无须按日建文件夹)
OHLCV_DIR = INPUT_ROOT / "ohlcv"                              # 历史日K线量价数据
FINANCIALS_DIR = INPUT_ROOT / "financials"                    # 财报三表数据
SENTIMENT_DIR = INPUT_ROOT / "sentiment"                      # 沽空与情绪数据

# 3.2 输出层 (Output: 熟数据 JSON 与分析报告)
ARCHIVE_DIR = OUTPUT_ROOT / "_archive"                        # 滚动冷备份，防止最新 JSON 损坏
LATEST_DIR = OUTPUT_ROOT / "latest"                           # [核心] 永远存放最新、最全的单股 JSON (如 0700_HK_yyyymmdd.json)
FINAL_REPORTS_DIR = OUTPUT_ROOT / "final_reports"             # LLM 生成的最终 Markdown 报告 (如 GEMINI_MODEL_ID_VERSION_yyyymmdd.md 或 GROK_MODEL_ID_VERSION_yyyymmdd.md)

# === 4. 自动创建所有目录 ===
# 将所有路径放入列表，批量创建
ALL_DIRS = [
    PORTFOLIO_DIR, TRANSACTIONS_DIR,
    OHLCV_DIR, FINANCIALS_DIR, SENTIMENT_DIR,
    ARCHIVE_DIR, LATEST_DIR, FINAL_REPORTS_DIR
]

for folder in ALL_DIRS:
    # parents=True: 如果父目录(比如 data/portfolio)不存在，也会一起创建
    # exist_ok=True: 如果文件夹已经存在(比如今天运行了两次)，不会报错
    folder.mkdir(parents=True, exist_ok=True)

# === 5. 账户与API配置 ===
# 优先从环境变量获取
ACCOUNT_ID = os.getenv("IBKR_ACCOUNT_ID")
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1") # 给个默认值兜底
IBKR_PORT = int(os.getenv("IBKR_PORT", 7496))   # 默认模拟交易端口 7497，实盘是 7496
CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", 1))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# GROK_API_KEY = os.getenv("GROK_API_KEY")

# === 6. 全局业务参数配置 ===
# 控制数据抓取的深度和逻辑
LOOKBACK_YEARS = 15  # 默认回溯 15 年的数据，以覆盖完整宏观牛熊周期
