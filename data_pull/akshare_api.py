import sys
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from pathlib import Path
from config import OHLCV_DIR, LOOKBACK_YEARS

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# ==========================================
# 核心拉取函数
# ==========================================

def fetch_hk_ohlcv(ticker_symbol: str, years: int = LOOKBACK_YEARS)-> bool:
    """
    通过 AkShare 拉取港股历史日 K 线数据 (包含成交量与成交额)，并保存为 CSV。

    参数:
    ticker_symbol (str): 股票代码，例如 "0700.HK"
    years (int): 回溯年限，默认 LOOKBACK_YEARS 年

    返回:
    bool: 拉取并保存是否成功
    """
    print(f"🔄 开始抓取 {ticker_symbol} 过去 {years} 年的量价数据...")

    # 1. 股票代码 5 位数预处理 (针对 AkShare 港股数据源)
    # "0700.HK" -> 提取 "0700" -> 补齐 5 位变成 "00700"
    base_symbol = ticker_symbol.split('.')[0] if '.' in ticker_symbol else ticker_symbol
    ak_symbol = base_symbol.zfill(5)

    # 2. 计算日期范围 (格式: YYYYMMDD)
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - timedelta(days=years * 365)

    start_date_str = start_date_obj.strftime("%Y%m%d")
    end_date_str = end_date_obj.strftime("%Y%m%d")

    try:
        # 3. 调用 AkShare 接口
        # period="daily" 代表日线
        # adjust="qfq" 代表前复权 (极其重要！技术分析必须用前复权价格，否则分红除权会导致均线断层)
        df = ak.stock_hk_hist(
            symbol=ak_symbol,
            period="daily",
            start_date=start_date_str,
            end_date=end_date_str,
            adjust="qfq"
        )

        if df is None or df.empty:
            print(f"❌ 未能获取到 {ticker_symbol} 的数据，API 返回为空。")
            return False

        # 4. 列名标准化清洗 (将中文列名映射为标准的英文列名，方便后续 Pandas 处理)
        rename_map = {
            '日期': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume',
            '成交额': 'Turnover_Value', # 成交额 (金额)
            '振幅': 'Amplitude',
            '涨跌幅': 'Pct_Chg',
            '涨跌额': 'Change',
            '换手率': 'Turnover_Rate'
        }
        df.rename(columns=rename_map, inplace=True)

        # 确保 Date 列是标准的 YYYY-MM-DD 格式
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        # 5. 直接使用 config 里的 OHLCV_DIR 落盘
        df.sort_values('Date', ascending=True, inplace=True)
        file_path = OHLCV_DIR / f"{ticker_symbol}_daily.csv"

        # index=False 保证不会把无意义的行号存入 CSV
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"✅ 成功! {ticker_symbol} 量价数据已存入: {file_path} (共 {len(df)} 条交易日)")
        return True

    except KeyError as e:
        print(f"❌ 数据解析失败，通常是因为触发了 API 频控或代码不存在。错误键: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ 抓取 {ticker_symbol} 数据时发生未知错误: {str(e)}")
        return False

# ==========================================
# 测试模块 (仅在该文件被直接运行时触发)
# ==========================================
if __name__ == "__main__":
    # 测试拉取腾讯控股 (0700.HK) 过去 15 年的数据
    test_ticker = "0700.HK"
    fetch_hk_ohlcv(test_ticker)
