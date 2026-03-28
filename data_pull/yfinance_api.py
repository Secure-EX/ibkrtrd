import sys
import json
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import FINANCIALS_DIR, OHLCV_DIR, LOOKBACK_YEARS

# ==========================================
# Function 1: 拉 info.json + 三表 CSV (作为底线)
# ==========================================
def fetch_financials(ticker_symbol: str) -> bool:
    """
    通过 yfinance 拉取股票的财务报表（三大表）和基础画像。
    包含年度(Annual)和季度(Quarterly)数据。
    """
    print(f"🔄 开始抓取 {ticker_symbol} 的财务报表与基本面数据 (yfinance)...")

    # 注意：yfinance 认的港股代码就是 "0700.HK"，不需要像 AkShare 那样去转换
    ticker = yf.Ticker(ticker_symbol)

    # ==========================================
    # 1. 抓取基础画像 (Info) -> 保存为 JSON
    # ==========================================
    try:
        info = ticker.info
        info_file = FINANCIALS_DIR / f"{ticker_symbol}_info.json"

        # 将静态的字典信息落盘
        with open(info_file, 'w', encoding='utf-8') as f:
            json.dump(info, f, indent=4, ensure_ascii=False)
        print(f"  ✅ 基础画像 (Info) 已保存: {info_file.name}")
    except Exception as e:
        print(f"  ❌ 获取基础画像失败: {e}")

    # ==========================================
    # 2. 抓取三大财报 -> 转置并保存为 CSV
    # ==========================================
    # 映射字典：将 yfinance 的属性对象与我们要保存的文件名后缀对应起来
    financial_statements_map = {
        "annual_income": "financials",
        "quarterly_income": "quarterly_financials",
        "annual_balance": "balance_sheet",
        "quarterly_balance": "quarterly_balance_sheet",
        "annual_cashflow": "cashflow",
        "quarterly_cashflow": "quarterly_cashflow"
    }

    for name, attr_name in financial_statements_map.items():
        try:
            # 在这里（try块内部）才真正发起网络请求拉取数据
            df = getattr(ticker, attr_name)

            # yfinance 如果没有数据，可能会返回 None 或空的 DataFrame
            if df is None or df.empty:
                print(f"  ⚠️ {name} 数据为空，跳过。")
                continue

            # 矩阵转置 (Transpose)
            # 原本：列名是日期 (2025-12-31, 2024-12-31...)，行名是指标 (Total Revenue...)
            # 转置后：行变日期，列变指标，这才是量化数据该有的样子！
            df_transposed = df.T

            # 将转置后的索引 (原本的日期) 变成真实的数据列，并命名为 'Date'
            df_transposed.reset_index(inplace=True)
            df_transposed.rename(columns={'index': 'Date'}, inplace=True)

            # 清洗日期格式为标准的 YYYY-MM-DD
            df_transposed['Date'] = pd.to_datetime(df_transposed['Date']).dt.strftime('%Y-%m-%d')

            # 按日期时间线正向排序 (最老的数据在第一行，最新的在最后一行)
            df_transposed.sort_values('Date', ascending=True, inplace=True)

            # 落盘保存为 CSV
            file_path = FINANCIALS_DIR / f"{ticker_symbol}_{name}.csv"
            df_transposed.to_csv(file_path, index=False, encoding='utf-8')
            print(f"  ✅ 成功提取 {name}: {file_path.name} (共 {len(df_transposed)} 期)")

        except Exception as e:
            print(f"  ❌ 提取 {name} 时发生错误: {str(e)}")

    print(f"🎉 {ticker_symbol} 财务数据流水线执行完毕！")
    return True

# ==========================================
# Function 2: IBKR 的 OHLCV 备用引擎，如果 IBKR 挂了就用 yfinance
# ==========================================
def fallback_to_yfinance(ticker_symbol: str, years: int) -> bool:
    """
    备用引擎：当 AkShare 失败时，使用雅虎财经拉取数据，并拟合补全缺失字段。

    返回:
    bool: 拉取并保存是否成功
    """
    print(f"   🔄 [备用引擎] 正在唤醒 yfinance 接管 {ticker_symbol} 的量价拉取任务...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)

    try:
        ticker = yf.Ticker(ticker_symbol)
        # yfinance 对新股极其包容，比如理想汽车只有 5 年历史，要 15 年它也会平稳返回 5 年数据
        df = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

        if df.empty:
            print(f"   ❌ [备用引擎] yfinance 也未能获取到 {ticker_symbol} 的数据。")
            return False

        df.reset_index(inplace=True)

        # 时区处理：把带时区的 datetime 转换为干净的字符串 YYYY-MM-DD
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')

        # 核心替代逻辑：拟合 AkShare 特有的 Turnover_Value (成交额)
        # 采用典型价格 (Typical Price) 估算成交额 Turnover_approx = ((High + Low + Close) / 3) x Volume
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        df['Turnover_Value'] = typical_price * df['Volume']

        # 选取下游 technical_calc.py 强依赖的列
        columns_to_keep = [
            'Date',
            'Open',
            'Close',
            'High',
            'Low',
            'Volume',
            'Turnover_Value'
        ]
        df_clean = df[columns_to_keep].copy()

        df_clean.sort_values('Date', ascending=True, inplace=True)
        file_path = OHLCV_DIR / f"{ticker_symbol}_daily.csv"
        df_clean.to_csv(file_path, index=False, encoding='utf-8')

        print(f"   ✅ [备用引擎] 成功! {ticker_symbol} 量价数据已由 yfinance 存入 (共 {len(df_clean)} 条)")
        return True

    except Exception as e:
        print(f"   ❌ [备用引擎] 发生崩溃: {e}")
        return False

# ==========================================
# Function 3: 拉取大盘指数日 K 线 (yfinance 专属)
# ==========================================
def fetch_index_ohlcv(index_symbol: str, years: int = LOOKBACK_YEARS):
    """
    通过 yfinance 拉取指数日K线数据 (如恒生指数 ^HSI, 恒生科技 ^HSTECH)。
    支持增量更新：本地已有数据时只拉缺口部分。

    参数:
        index_symbol: yfinance 格式的指数代码 (如 "^HSI")
        years: 回溯年限
    """
    from datetime import datetime, timedelta

    # 将 ^HSI 转为文件安全的名称 HSI
    safe_name = index_symbol.replace('^', '').replace('.', '_')
    file_path = OHLCV_DIR / f"INDEX_{safe_name}_daily.csv"

    end_date = datetime.now()

    # 增量检测
    df_existing = None
    if file_path.exists():
        df_existing = pd.read_csv(file_path)
        last_date_str = df_existing['Date'].max()
        last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
        days_gap = (end_date - last_dt).days

        if days_gap <= 1:
            print(f"   ℹ️ {index_symbol} 指数数据已是最新，跳过拉取。")
            return True

        start_date = last_dt - timedelta(days=5)  # 小缓冲区防遗漏
        print(f"   📥 [增量模式] {index_symbol} 拉取最近 {days_gap} 天...")
    else:
        start_date = end_date - timedelta(days=years * 365)
        print(f"   📥 [全量模式] {index_symbol} 首次拉取过去 {years} 年...")

    try:
        ticker = yf.Ticker(index_symbol)
        df = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

        if df.empty:
            print(f"   ❌ yfinance 未能获取到 {index_symbol} 的数据。")
            return False

        df.reset_index(inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')

        # 指数没有真实成交额，用典型价格估算 (仅用于格式兼容，下游不依赖此值)
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        df['Turnover_Value'] = typical_price * df['Volume']

        columns_to_keep = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover_Value']
        df_new = df[columns_to_keep].copy()

        # 增量合并
        if df_existing is not None:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset='Date', keep='last', inplace=True)
        else:
            df_combined = df_new

        df_combined.sort_values('Date', ascending=True, inplace=True)
        df_combined.to_csv(file_path, index=False, encoding='utf-8')

        print(f"   ✅ {index_symbol} 指数日K线已保存 (共 {len(df_combined)} 条)")
        return True

    except Exception as e:
        print(f"   ❌ {index_symbol} 指数拉取失败: {e}")
        return False

# ==========================================
# 本地单例测试模块
# ==========================================
if __name__ == "__main__":
    test_ticker = "0700.HK"
    fetch_financials(test_ticker)
    # test_ticker = "3033.HK"
    # fetch_index_ohlcv(test_ticker, 15)
