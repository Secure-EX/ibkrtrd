import sys
import random
import pandas as pd
import akshare as ak
import yfinance as yf
import time
from datetime import datetime, timedelta
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import OHLCV_DIR, LOOKBACK_YEARS

# ==========================================
# 核心拉取函数
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

def fetch_hk_ohlcv(ticker_symbol: str, years: int = LOOKBACK_YEARS):
    """
    通过 AkShare 拉取港股历史日 K 线数据 (包含成交量与成交额)，并保存为 CSV。
    失败则自动降级到备用引擎。

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

    max_retries = 3
    akshare_success = False

    # 尝试主引擎
    for attempt in range(1, max_retries + 1):
        try:
            # 加上一个极其细微的随机延迟，防止并发请求被直接拉黑
            if attempt > 1:
                sleep_time = random.uniform(5, 10) # 随机休息 5 到 10 秒
                print(f"   ⚠️ 第 {attempt} 次尝试重新连接... (休眠 {sleep_time:.1f} 秒避开反爬)")
                time.sleep(sleep_time)

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
            print(f"   ✅ [主引擎] 成功! {ticker_symbol} 量价数据已由 AkShare 存入 {file_path} (共 {len(df)} 条交易日)")
            akshare_success = True
            break # 成功则跳出重试

        except Exception as e:
            err_msg = str(e)
            if "Connection aborted" in err_msg or "RemoteDisconnected" in err_msg or "timeout" in err_msg.lower():
                print(f"   🛑 网络被掐断: {err_msg.split('(')[0]}")
                if attempt == max_retries:
                    print(f"❌ {ticker_symbol} 连续 {max_retries} 次抓取失败，请检查网络或稍后再试。")
                continue # 继续下一次 for 循环重试
            else:
                # 如果是 KeyError 等数据结构错误，说明不是网络问题，直接抛出
                print(f"❌ 数据解析或其它致命错误: {err_msg}")

    # 降级判定：如果主引擎彻底阵亡，启动 C 计划
    if not akshare_success:
        return fallback_to_yfinance(ticker_symbol, years)

# ==========================================
# 测试模块 (仅在该文件被直接运行时触发)
# ==========================================
if __name__ == "__main__":
    # 测试拉取腾讯控股 (0700.HK) 过去 15 年的数据
    test_ticker = "0700.HK"
    fetch_hk_ohlcv(test_ticker)
