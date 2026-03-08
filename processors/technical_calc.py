import sys
import pandas as pd
import pandas_ta as ta
import numpy as np
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from config import OHLCV_DIR

# ==========================================
# 核心计算函数
# ==========================================

def _add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    内部辅助函数：为输入的 DataFrame 批量添加技术指标 (MA, MACD, RSI, KDJ, BOLL, VWAP)。
    依赖: pandas_ta 库
    """
    if df.empty or len(df) < 20:
        return df

    # 1. 均线系统 (MA: 5, 10, 20, 30, 60, 120, 250)
    ma_windows = [5, 10, 20, 30, 60, 120, 250]
    for w in ma_windows:
        # append=True 会直接把算好的列 (例如 SMA_5) 加进原 df
        df.ta.sma(length=w, append=True)

    # 2. 动能指标: MACD (12, 26, 9)
    # 自动生成列: MACD_12_26_9 (DIF), MACDs_12_26_9 (DEA), MACDh_12_26_9 (HIST/红绿柱)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)

    # 3. 动能指标: RSI (14) -> 生成列: RSI_14
    df.ta.rsi(length=14, append=True)

    # 4. 波动/震荡指标: KDJ (9, 3, 3) -> 生成列: K_9_3, D_9_3, J_9_3
    df.ta.kdj(length=9, signal=3, append=True)

    # 5. 波动指标: BOLL (20, 2)
    # 生成列: BBL_20_2.0 (下轨), BBM_20_2.0 (中轨), BBU_20_2.0 (上轨)
    df.ta.bbands(length=20, std=2, append=True)

    # 6. 计算当期 VWAP (成交量加权平均价) = 当期总成交额 / 当期总成交量
    # 注意：防止除以 0 的情况出现
    df['VWAP_Custom'] = np.where(df['Volume'] > 0, df['Turnover_Value'] / df['Volume'], df['Close'])

    return df

def _safe_get(row: pd.Series, col_name: str, is_int: bool = False):
    """
    内部辅助函数：安全提取数据，把 DataFrame 中的 NaN/NaT 转换为 JSON 友好的 None，并保留 2 位小数。
    """
    if col_name not in row.index or pd.isna(row[col_name]):
        return None

    val = row[col_name]
    if is_int:
        return int(val)
    return round(float(val), 2)

def _get_dynamic_col(df: pd.DataFrame, prefix: str) -> str:
    """
    动态列名匹配器：在 DataFrame 中寻找以指定前缀开头的列名。
    解决 pandas_ta 版本更新导致列名（如 BBU_20_2.0_2.0）变动的问题。
    """
    for col in df.columns:
        if col.startswith(prefix):
            return col
    return "" # 如果没找到，返回空字符串

def _extract_latest_features(df: pd.DataFrame) -> dict:
    """
    提取时间序列的最后一行(最新数据)，拼装成目标 JSON Schema 的结构。
    """
    if df is None or df.empty:
        return {}

    latest = df.iloc[-1]

    # 获取日期字符串
    date_str = latest.name.strftime('%Y-%m-%d') if isinstance(latest.name, pd.Timestamp) else str(latest.name)

    # 动态获取布林带的真实列名
    col_bbu = _get_dynamic_col(df, 'BBU_')
    col_bbm = _get_dynamic_col(df, 'BBM_')
    col_bbl = _get_dynamic_col(df, 'BBL_')

    return {
        "date": date_str,
        "volume": _safe_get(latest, 'Volume', is_int=True),
        "turnover_value": _safe_get(latest, 'Turnover_Value', is_int=True),
        "vwap": _safe_get(latest, 'VWAP_Custom'),
        "trend": {
            "ma5": _safe_get(latest, 'SMA_5'),
            "ma10": _safe_get(latest, 'SMA_10'),
            "ma20": _safe_get(latest, 'SMA_20'),
            "ma30": _safe_get(latest, 'SMA_30'),
            "ma60": _safe_get(latest, 'SMA_60'),
            "ma120": _safe_get(latest, 'SMA_120'),
            "ma250": _safe_get(latest, 'SMA_250')
        },
        "momentum": {
            "macd_dif": _safe_get(latest, 'MACD_12_26_9'),
            "macd_dea": _safe_get(latest, 'MACDs_12_26_9'),
            "macd_hist": _safe_get(latest, 'MACDh_12_26_9'),
            "rsi_14": _safe_get(latest, 'RSI_14'),
            "kdj_k": _safe_get(latest, 'K_9_3'),
            "kdj_d": _safe_get(latest, 'D_9_3'),
            "kdj_j": _safe_get(latest, 'J_9_3')
        },
        "volatility": {
            # 使用动态获取到的列名进行安全提取
            "boll_upper": _safe_get(latest, col_bbu) if col_bbu else None,
            "boll_mid": _safe_get(latest, col_bbm) if col_bbm else None,
            "boll_lower": _safe_get(latest, col_bbl) if col_bbl else None
        }
    }

# ==========================================
# 主调用函数
# ==========================================

def generate_technical_analysis(ticker_symbol: str) -> dict:
    """
    读取生数据，重采样日、周、月线，返回完美的 technical_analysis 字典。
    """
    file_path = OHLCV_DIR / f"{ticker_symbol}_daily.csv"

    if not file_path.exists():
        print(f"⚠️ 找不到 {ticker_symbol} 的量价数据: {file_path}")
        return {}

    print(f"⚙️ 正在计算 {ticker_symbol} 的多周期技术指标...")

    # 1. 读取日线数据，并把 Date 设为 DateTimeIndex (重采样必须的前置条件)
    df_daily = pd.read_csv(file_path)
    df_daily['Date'] = pd.to_datetime(df_daily['Date'])
    df_daily.set_index('Date', inplace=True)
    df_daily.sort_index(ascending=True, inplace=True)

    # 2. 计算日线指标
    df_daily = _add_technical_indicators(df_daily)
    daily_features = _extract_latest_features(df_daily)

    # 3. 重采样计算周线 (Weekly - 以周五为界)
    # Open 取本周第一天，Close 取本周最后一天，Volume 和 Value 求和
    agg_dict = {
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last',
        'Volume': 'sum', 'Turnover_Value': 'sum'
    }
    # 使用 'W-FRI' 确保周线的日期标签总是落在周五
    df_weekly = df_daily.resample('W-FRI').agg(agg_dict).dropna(subset=['Close'])
    df_weekly = _add_technical_indicators(df_weekly)
    weekly_features = _extract_latest_features(df_weekly)

    # 4. 重采样计算月线 (Monthly - 以月末为界)
    # 使用 'ME' (Month End)
    df_monthly = df_daily.resample('ME').agg(agg_dict).dropna(subset=['Close'])
    df_monthly = _add_technical_indicators(df_monthly)
    monthly_features = _extract_latest_features(df_monthly)

    # 5. 拼装成终极结构
    technical_analysis = {
        "daily": daily_features,
        "weekly": weekly_features,
        "monthly": monthly_features
    }

    print(f"✅ {ticker_symbol} 多周期技术面指标计算完成！")
    return technical_analysis

# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    import json

    test_ticker = "0700.HK"
    tech_data = generate_technical_analysis(test_ticker)

    # 漂亮地打印出最终的 JSON 结构
    if tech_data:
        print("\n最终输出的 JSON 结构片段:")
        print(json.dumps(tech_data, indent=4, ensure_ascii=False))
