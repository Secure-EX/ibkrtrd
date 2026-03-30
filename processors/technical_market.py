"""
technical_market.py — 大盘指数与自身周期分析模块

包含内容：
    - _load_index_data        : 统一加载港股大盘指数数据（只读一次磁盘）
          加载来源：INDEX_HSI_daily.csv（恒生指数）、INDEX_3033_HK_daily.csv（科技指数ETF）
    - _calc_own_cycle         : 剥离大盘周期，计算公司自身周期系数
          方法：OLS 回归残差 → 累计残差曲线 → 历史百分位排名
          输出：own_cycle_level(0~1)、own_cycle_zone、回归 β/α、使用的指数
    - _calc_market_correlation: 计算个股与大盘指数的滚动皮尔逊相关系数
          同时计算与所有可用指数的相关性（HSI + HSTECH）
          输出：多窗口相关系数、60日趋势变化、中文解读

依赖：config.OHLCV_DIR
     technical_utils._percentile_rank_in_series
     technical_risk._risk_zone_label
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import OHLCV_DIR

try:
    from .technical_utils import _percentile_rank_in_series
    from .technical_risk import _risk_zone_label
except ImportError:
    from technical_utils import _percentile_rank_in_series
    from technical_risk import _risk_zone_label


def _load_index_data() -> dict:
    """
    统一加载港股大盘指数数据（只读一次磁盘）。

    返回:
        {"HSI": df_hsi, "HSTECH_3033": df_hstech, ...}
        如果某个指数文件不存在或数据不足，则不包含该 key。
    """
    index_candidates = [
        ("HSI", OHLCV_DIR / "INDEX_HSI_daily.csv"),
        ("HSTECH_3033", OHLCV_DIR / "INDEX_3033_HK_daily.csv"),
    ]

    loaded = {}
    for name, path in index_candidates:
        if path.exists():
            try:
                df_idx = pd.read_csv(path)
                if 'Date' in df_idx.columns and 'Close' in df_idx.columns:
                    df_idx['Date'] = pd.to_datetime(df_idx['Date'])
                    df_idx.set_index('Date', inplace=True)
                    df_idx.sort_index(inplace=True)
                    if len(df_idx) >= 60:
                        loaded[name] = df_idx
            except Exception:
                continue
    return loaded


def _calc_own_cycle(df_stock: pd.DataFrame, index_data: dict = None, lookback: int = 1260) -> dict:
    """
    剥离大盘周期后，计算公司自身周期系数。

    方法：
        1. 线性回归：R_stock = α + β·R_index + ε，取残差 ε
        2. 累计残差曲线 = 公司"纯净价格路径"（去掉大盘 β 贡献）
        3. 当前累计残差在历史中的百分位排名 → 周期系数

    结果解读：
        接近 0 → 公司自身处于历史最低谷（周期机会区）
        接近 1 → 公司自身处于历史最高峰（周期风险区）
        <0.05 为周期机会区，>0.95 为周期风险区

    参数:
        df_stock   : 个股日线 DataFrame，含 Close 列，DateTimeIndex
        index_data : 大盘指数字典 {"HSI": df, ...}，由 _load_index_data() 提供
        lookback   : 回溯窗口（交易日），默认 1260（约 5 年）

    返回:
        {
            "own_cycle_level": 0.32,
            "own_cycle_zone": "中性",
            "regression_beta": 1.15,
            "regression_alpha_annualized": 0.03,
            "residual_cumulative": -0.12,
            "index_used": "HSI"
        }
    """
    result = {
        "own_cycle_level": None,
        "own_cycle_zone": "数据不足",
        "regression_beta": None,
        "regression_alpha_annualized": None,
        "residual_cumulative": None,
        "index_used": None,
    }

    if df_stock is None or df_stock.empty or len(df_stock) < 120:
        return result

    # ------ 从预加载的 index_data 中取第一个可用指数 ------
    if index_data is None:
        index_data = _load_index_data()

    df_index = None
    index_name = None
    for name in ["HSI", "HSTECH_3033"]:
        if name in index_data and len(index_data[name]) >= 120:
            df_index = index_data[name]
            index_name = name
            break

    if df_index is None:
        return result

    # ------ 对齐日期，计算日收益率 ------
    stock_ret = df_stock['Close'].pct_change().replace([np.inf, -np.inf], np.nan)
    index_ret = df_index['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

    common_idx = stock_ret.dropna().index.intersection(index_ret.dropna().index)
    # 截取回溯窗口
    common_idx = common_idx[-min(lookback, len(common_idx)):]
    if len(common_idx) < 120:
        return result

    y = stock_ret.loc[common_idx].values  # 个股收益率
    x = index_ret.loc[common_idx].values  # 大盘收益率

    # 清洗残留 NaN / inf，防止污染 OLS
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    if len(x) < 120:
        return result

    # ------ Step 1: OLS 回归 R_stock = α + β·R_index + ε ------
    # 手动 OLS，避免引入 statsmodels 依赖（x/y 已清洗，无 NaN）
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    beta = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2)
    alpha = y_mean - beta * x_mean
    residuals = y - (alpha + beta * x)

    # ------ Step 2: 累计残差曲线 ------
    cum_residuals = np.cumsum(residuals)

    # ------ Step 3: 当前累计残差的历史百分位 ------
    current_cum = cum_residuals[-1]
    cum_series = pd.Series(cum_residuals)
    own_cycle_level = _percentile_rank_in_series(current_cum, cum_series)

    result["own_cycle_level"] = round(own_cycle_level, 4) if own_cycle_level is not None else None
    result["own_cycle_zone"] = _risk_zone_label(own_cycle_level, "cycle")
    result["regression_beta"] = round(float(beta), 4)
    result["regression_alpha_annualized"] = round(float(alpha * 252), 4)
    result["residual_cumulative"] = round(float(current_cum), 4)
    result["index_used"] = index_name

    return result


def _calc_market_correlation(df_stock: pd.DataFrame, index_data: dict = None, windows: list = None) -> dict:
    """
    计算个股与港股大盘指数的滚动皮尔逊相关系数。

    同时计算与所有可用指数的相关性（HSI + HSTECH），而非只取第一个。

    大盘指数来源：
        - INDEX_HSI_daily.csv（恒生指数）
        - INDEX_3033_HK_daily.csv（南方東英恒生科技指數 ETF）

    参数:
        df_stock   : 个股日线 DataFrame，含 Close 列，DateTimeIndex
        index_data : 大盘指数字典 {"HSI": df, ...}，由 _load_index_data() 提供
        windows    : 滚动窗口列表，默认 [250, 500]

    返回:
        {
            "HSI": {
                "correlation_250d": 0.84,
                "correlation_500d": 0.83,
                "correlation_trend_60d": {"start": 0.85, "end": 0.84, "delta": -0.01},
                "interpretation": "高度正相关 (0.84)"
            },
            "HSTECH_3033": {
                "correlation_250d": 0.91,
                ...
            }
        }
        如果无法计算，返回空字典。
    """
    if windows is None:
        windows = [250, 500]

    if df_stock is None or df_stock.empty or len(df_stock) < 60:
        return {}

    # ------ 使用预加载的 index_data ------
    if index_data is None:
        index_data = _load_index_data()

    if not index_data:
        return {}

    stock_ret = df_stock['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

    # ------ 逐指数计算 ------
    result = {}
    for index_name, df_index in index_data.items():
        index_ret = df_index['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

        common_idx = stock_ret.dropna().index.intersection(index_ret.dropna().index)
        if len(common_idx) < 60:
            continue

        s_ret = stock_ret.loc[common_idx]
        i_ret = index_ret.loc[common_idx]

        entry = {}

        # 多窗口滚动相关系数
        for w in windows:
            if len(common_idx) < w:
                corr_val = float(s_ret.corr(i_ret))
                entry[f"correlation_{w}d"] = round(corr_val, 4) if pd.notna(corr_val) else None
            else:
                rolling_corr = s_ret.rolling(w, min_periods=int(w * 0.7)).corr(i_ret)
                latest_corr = rolling_corr.dropna().iloc[-1] if not rolling_corr.dropna().empty else None
                entry[f"correlation_{w}d"] = round(float(latest_corr), 4) if latest_corr is not None else None

        # 近 60 日趋势（压缩为统计量）
        if len(common_idx) >= 250:
            rolling_250 = s_ret.rolling(250, min_periods=175).corr(i_ret)
            trend = rolling_250.tail(60).dropna()
            if len(trend) >= 2:
                entry["correlation_trend_60d"] = {
                    "start": round(float(trend.iloc[0]), 4),
                    "end": round(float(trend.iloc[-1]), 4),
                    "delta": round(float(trend.iloc[-1] - trend.iloc[0]), 4),
                }

        # 相关性解读
        primary_corr = entry.get(f"correlation_{windows[0]}d")
        if primary_corr is not None:
            abs_corr = abs(primary_corr)
            if abs_corr >= 0.8:
                strength = "高度"
            elif abs_corr >= 0.5:
                strength = "中度"
            elif abs_corr >= 0.3:
                strength = "低度"
            else:
                strength = "极弱"
            direction = "正相关" if primary_corr >= 0 else "负相关"
            entry["interpretation"] = f"{strength}{direction} ({primary_corr:.2f})"

        result[index_name] = entry

    return result


# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    import json
    import pandas as pd

    test_ticker = "0700.HK"
    file_path = OHLCV_DIR / f"{test_ticker}_daily.csv"

    if not file_path.exists():
        print(f"⚠️ 找不到 {test_ticker} 的量价数据: {file_path}")
    else:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(ascending=True, inplace=True)

        print(f"⚙️ 正在计算 {test_ticker} 的大盘指数分析...")

        # 加载大盘指数（只读一次磁盘）
        index_data = _load_index_data()
        print(f"✅ 已加载大盘指数: {', '.join(index_data.keys()) if index_data else '无'}")

        # 计算自身周期
        own_cycle = _calc_own_cycle(df, index_data=index_data)
        print("\n自身周期分析:")
        print(json.dumps(own_cycle, indent=4, ensure_ascii=False))

        # 计算大盘相关性
        mkt_corr = _calc_market_correlation(df, index_data=index_data)
        print("\n大盘相关性分析:")
        print(json.dumps(mkt_corr, indent=4, ensure_ascii=False))
