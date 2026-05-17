"""
technical_utils.py — 共用辅助工具模块

包含内容：
    - 常量：FINANCIAL_PUBLICATION_LAG_DAYS（财报发布滞后天数）
    - _align_financial_to_daily : 将财报序列（EPS/BVPS）对齐到日线索引，防止 look-ahead bias
    - _ttm_from_ytd_series    : 将 YTD 累计季度序列还原为标准 TTM (trailing 12-month)
    - _safe_get               : 安全提取 DataFrame 行数据，NaN → None
    - _get_dynamic_col        : 动态列名匹配器（兼容 pandas_ta 版本差异）
    - _percentile_rank_in_series : 单点百分位排名（0~1）
    - _rolling_percentile_rank   : 向量化滚动百分位排名（替代 O(n²) 逐点循环）

本模块被 technical_indicators / technical_multifactor / technical_market / derived_writer
等子模块共同引用，不依赖任何其他 technical_* 子模块，处于依赖链最底层。
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import RISK_FREE_RATE  # noqa: F401 (re-exported for convenience)

# 财报发布滞后（日历天）：港股上市公司季报通常在季度结束后 45~90 天才披露，
# 直接用财报日期做 ffill 会让未来数据渗入历史 PE/PB 计算（look-ahead bias）。
# 将财报索引向后平移此天数后再 ffill，确保只使用"当时已公开"的数据。
FINANCIAL_PUBLICATION_LAG_DAYS = 60

# OHLCV 周/月重采样的统一聚合规则与频率别名
# (technical_calc.generate_technical_analysis 与 derived_writer.write_technical_history 共用)
RESAMPLE_AGG = {
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last",
    "Volume": "sum",
    "Turnover_Value": "sum",
}
RESAMPLE_RULES = {"weekly": "W-FRI", "monthly": "ME"}


def _align_financial_to_daily(fin_series: pd.Series, daily_index: pd.DatetimeIndex) -> pd.Series:
    """
    将财报序列（EPS / BVPS）对齐到日线索引，并加入发布滞后防止 look-ahead bias。

    步骤：
        1. 将财报日期向后平移 FINANCIAL_PUBLICATION_LAG_DAYS 个日历天
        2. reindex 到日线索引 + ffill（只向前填充已公开的数据）
    """
    shifted = fin_series.copy()
    shifted.index = shifted.index + pd.Timedelta(days=FINANCIAL_PUBLICATION_LAG_DAYS)
    return shifted.reindex(daily_index).ffill()


def _ttm_from_ytd_series(ytd: pd.Series) -> pd.Series:
    """把 YTD 累计的季度/中期序列转换为标准 TTM (trailing 12-month) 序列。

    数据规约：HK/A 股的 quarterly_income.csv 每行 EPS/Revenue 是该财年从年初到该报告日的累计值。
    年报 (12-31) 的累计 = 该年 TTM；H1 (06-30) 的累计 = 上半年 6 个月数据。

    TTM 公式（标准做法）:
        TTM(t) = 上年年报 + (本期 YTD - 上年同期 YTD)

    边界处理：
        - 当前是年报 (12-31)：直接返回 YTD (本身就是 TTM)
        - 找不到上年年报：跳过该期 (返回 NaN)
        - 找不到上年同期：跳过该期 (返回 NaN)
    """
    if ytd is None or ytd.empty:
        return ytd
    s = ytd.dropna().sort_index()
    if s.empty:
        return s

    annual_by_year: dict = {}
    same_period_by_key: dict = {}
    for date, val in s.items():
        same_period_by_key[(date.year, date.month, date.day)] = float(val)
        if date.month == 12:
            annual_by_year[date.year] = float(val)

    out: dict = {}
    for date, val in s.items():
        if date.month == 12:
            out[date] = float(val)
            continue

        prev_year = date.year - 1
        prev_annual = annual_by_year.get(prev_year)
        if prev_annual is None:
            continue

        prev_same = same_period_by_key.get((prev_year, date.month, date.day))
        if prev_same is None:
            same_month = [
                v for (y, m, d), v in same_period_by_key.items()
                if y == prev_year and m == date.month
            ]
            prev_same = same_month[0] if len(same_month) == 1 else None

        if prev_same is None:
            continue
        out[date] = prev_annual + (float(val) - prev_same)

    if not out:
        return pd.Series(dtype=float)
    result = pd.Series(out).sort_index()
    result.index.name = s.index.name
    return result


def _safe_get(row: pd.Series, col_name: str, is_int: bool = False):
    """
    安全提取数据，把 DataFrame 中的 NaN/NaT 转换为 JSON 友好的 None，并保留 2 位小数。
    """
    if col_name not in row.index or pd.isna(row[col_name]):
        return None

    val = row[col_name]
    if is_int:
        return int(val)
    return float(val)


def _get_dynamic_col(df: pd.DataFrame, prefix: str):
    """
    动态列名匹配器：在 DataFrame 中寻找以指定前缀开头的列名。
    解决 pandas_ta 版本更新导致列名（如 BBU_20_2.0_2.0）变动的问题。

    返回: 找到的列名 str；找不到时返回 None（明确表达"列缺失"）。
    """
    for col in df.columns:
        if col.startswith(prefix):
            return col
    return None


def _percentile_rank_in_series(value: float, history: pd.Series) -> float:
    """
    计算单个数值在历史序列中的百分位排名（0~1）。
    用于将任意因子的原始值归一化到统一尺度。
    """
    if history is None or history.empty or pd.isna(value):
        return None
    clean = history.dropna()
    if len(clean) < 10:
        return None
    return float((clean < value).sum()) / len(clean)


def _rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """
    向量化滚动百分位排名：对序列中每个点，计算它在过去 window 个值中的百分位。

    替代逐点调用 _percentile_rank_in_series 的 O(n²) 采样循环，
    一次 rolling pass 即可得到完整的百分位时间序列。

    返回: 与输入等长的 pd.Series，值域 [0, 1]
    """
    def _pct_rank(arr):
        n = len(arr)
        if n < 2:
            return np.nan
        return (arr[:-1] < arr[-1]).sum() / (n - 1)

    return series.rolling(window, min_periods=max(20, window // 4)).apply(_pct_rank, raw=True)


# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    import sys
    from pathlib import Path
    import pandas as pd

    BASE_DIR = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(BASE_DIR))
    from config import OHLCV_DIR

    test_ticker = "0700.HK"
    file_path = OHLCV_DIR / f"{test_ticker}_daily.csv"

    print(f"财报发布滞后天数常量: FINANCIAL_PUBLICATION_LAG_DAYS = {FINANCIAL_PUBLICATION_LAG_DAYS}")

    if not file_path.exists():
        print(f"⚠️ 找不到 {test_ticker} 的量价数据: {file_path}")
    else:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(ascending=True, inplace=True)

        print(f"\n✅ 已加载 {test_ticker} 日线数据，共 {len(df)} 行")

        # 测试 _rolling_percentile_rank
        rank_series = _rolling_percentile_rank(df['Close'], window=250)
        latest_rank = rank_series.dropna().iloc[-1] if not rank_series.dropna().empty else None
        print(f"\n滚动百分位排名 (250日窗口) 最新值: {round(latest_rank, 4) if latest_rank is not None else None}")

        # 测试 _percentile_rank_in_series
        current_price = float(df['Close'].iloc[-1])
        history_1y = df['Close'].tail(252)
        pct_rank = _percentile_rank_in_series(current_price, history_1y)
        print(f"当前价格 {current_price} 在过去1年的百分位: {round(pct_rank, 4) if pct_rank is not None else None}")
