import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# 加载 .env 文件
load_dotenv()

# === 1. 基础路径配置 ===
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# === 2. 动态日期分区 (Partition Key) ===
# 获取今日日期，例如: 20260218
# 未来这对应数据库 partition key: values from ('2026-02-18')
TODAY_STR = datetime.now().strftime("%Y%m%d")

# === 3. 定义子目录 (自动追加日期分区) ===
# 最终路径示例: .../data/portfolio/20260218/

# 3.0 定义根输入输出路径
INPUT_ROOT = DATA_DIR / "input"
OUTPUT_ROOT = DATA_DIR / "output"

# 3.1 输入内容 (Raw Data / Ingestion)
PORTFOLIO_DIR = INPUT_ROOT / "portfolio" / TODAY_STR       # 持仓快照
TRANSACTIONS_DIR = INPUT_ROOT / "transactions" / TODAY_STR # 交易流水
MARKET_DIR = INPUT_ROOT / "market" / TODAY_STR             # 大盘指数 (SPX, HSI) (OHLC)
STOCK_DIR = INPUT_ROOT / "stock" / TODAY_STR               # 个股 (OHLC)
FUNDAMENTAL_DIR = INPUT_ROOT / "fundamental" / TODAY_STR   # 财报/基本面
EMOTIONAL_DIR = INPUT_ROOT / "emotional" / TODAY_STR       # 情绪数据(新闻/舆情)

# 3.2 输出内容 (Processed Data / Analytics)
TECHNICAL_DIR = OUTPUT_ROOT / "technical" / TODAY_STR      # 技术指标计算结果
RISK_DIR = OUTPUT_ROOT / "risk" / TODAY_STR                # 胜率/赔率/风控分析
SUMMARY_DIR = OUTPUT_ROOT / "summary" / TODAY_STR          # 最终生成的周报/总结

# === 4. 自动创建所有目录 ===
# 将所有路径放入列表，批量创建
ALL_DIRS = [
    PORTFOLIO_DIR, TRANSACTIONS_DIR, MARKET_DIR, STOCK_DIR, FUNDAMENTAL_DIR, EMOTIONAL_DIR,
    TECHNICAL_DIR, RISK_DIR, SUMMARY_DIR
]

for folder in ALL_DIRS:
    # parents=True: 如果父目录(比如 data/portfolio)不存在，也会一起创建
    # exist_ok=True: 如果文件夹已经存在(比如今天运行了两次)，不会报错
    folder.mkdir(parents=True, exist_ok=True)

# === 5. 账户与API配置 ===
# 优先从环境变量获取
ACCOUNT_ID = os.getenv("IBKR_ACCOUNT_ID")
IBKR_HOST = "127.0.0.1"
IBKR_PORT = int(os.getenv("IBKR_PORT", 7496))    # 实盘 7496, 模拟盘 7497
CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", 1))  # 唯一 ID
