"""
technical_utils.py — 共用辅助工具模块

包含内容：
    - 常量：FINANCIAL_PUBLICATION_LAG_DAYS（财报发布滞后天数）
    - _align_financial_to_daily : 将财报序列（EPS/BVPS）对齐到日线索引，防止 look-ahead bias
    - _safe_get               : 安全提取 DataFrame 行数据，NaN → None
    - _get_dynamic_col        : 动态列名匹配器（兼容 pandas_ta 版本差异）
    - _percentile_rank_in_series : 单点百分位排名（0~1）
    - _rolling_percentile_rank   : 向量化滚动百分位排名（替代 O(n²) 逐点循环）

本模块被 technical_indicators / technical_multifactor / technical_market 等子模块共同引用，
不依赖任何其他 technical_* 子模块，处于依赖链最底层。
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


def _get_dynamic_col(df: pd.DataFrame, prefix: str) -> str:
    """
    动态列名匹配器：在 DataFrame 中寻找以指定前缀开头的列名。
    解决 pandas_ta 版本更新导致列名（如 BBU_20_2.0_2.0）变动的问题。
    """
    for col in df.columns:
        if col.startswith(prefix):
            return col
    return ""  # 如果没找到，返回空字符串


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
