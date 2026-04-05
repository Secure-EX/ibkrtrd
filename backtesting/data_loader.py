"""
data_loader.py — 离线数据加载模块

从磁盘读取 CSV 数据，不发起任何网络请求。
复用 processors.technical_financial.load_financial_series() 加载 EPS/BVPS。

公开函数:
    load_ohlcv(ticker, ohlcv_dir)       → pd.DataFrame（日K线，DateTimeIndex）
    load_index_ohlcv(benchmark, ohlcv_dir) → pd.DataFrame（指数日K线）
    load_financials(ticker, fin_dir)    → (eps_series, bvps_series)
    list_available_tickers(ohlcv_dir)   → List[str]
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, List

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))


def load_ohlcv(ticker: str, ohlcv_dir: Path) -> Optional[pd.DataFrame]:
    """
    加载个股日K线 CSV。

    文件命名：{ticker}_daily.csv，例如 0700.HK_daily.csv
    列要求：Date, Open, High, Low, Close, Volume, Turnover_Value

    返回: DateTimeIndex 升序排列的 DataFrame，或 None（文件不存在）
    """
    path = ohlcv_dir / f"{ticker}_daily.csv"
    if not path.exists():
        print(f"  [DataLoader] ⚠️  找不到 OHLCV 文件: {path}")
        return None

    df = pd.read_csv(path)
    if 'Date' not in df.columns:
        print(f"  [DataLoader] ⚠️  CSV 缺少 Date 列: {path}")
        return None

    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.sort_index(ascending=True, inplace=True)

    # 确保必要列存在
    required = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [DataLoader] ⚠️  CSV 缺少必要列 {missing}: {path}")
        return None

    # Turnover_Value 如果不存在就用 0 填充（某些股票可能没有该字段）
    if 'Turnover_Value' not in df.columns:
        df['Turnover_Value'] = 0.0

    # 过滤数值异常行
    df = df[df['Close'] > 0].copy()

    print(f"  [DataLoader] ✅ {ticker} OHLCV: {len(df)} 行 "
          f"({df.index.min().date()} ~ {df.index.max().date()})")
    return df


def load_index_ohlcv(benchmark: str, ohlcv_dir: Path) -> Optional[pd.DataFrame]:
    """
    加载基准指数日K线。

    文件命名规则：
        "INDEX_HSI"      → INDEX_HSI_daily.csv
        "INDEX_3033_HK"  → INDEX_3033_HK_daily.csv
    如果 benchmark 字符串本身已是完整文件名前缀则直接使用。
    """
    filename = f"{benchmark}_daily.csv"
    path = ohlcv_dir / filename
    if not path.exists():
        print(f"  [DataLoader] ⚠️  找不到基准指数文件: {path}")
        return None

    df = pd.read_csv(path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.sort_index(ascending=True, inplace=True)
    df = df[df['Close'] > 0].copy()

    print(f"  [DataLoader] ✅ {benchmark}: {len(df)} 行 "
          f"({df.index.min().date()} ~ {df.index.max().date()})")
    return df


def load_financials(
    ticker: str, financials_dir: Path
) -> Tuple[Optional[pd.Series], Optional[pd.Series]]:
    """
    加载财报 EPS 和 BVPS 序列，复用 processors.technical_financial.load_financial_series()。

    返回: (eps_series, bvps_series)，若无数据则对应位置返回 None
    """
    try:
        from processors.technical_financial import load_financial_series
        eps, bvps = load_financial_series(ticker, financial_dir=financials_dir)
        return eps, bvps
    except Exception as e:
        print(f"  [DataLoader] ⚠️  加载 {ticker} 财报失败: {e}")
        return None, None


def list_available_tickers(ohlcv_dir: Path) -> List[str]:
    """
    列出 ohlcv/ 目录下所有可用的个股代码（排除指数文件）。

    返回: 如 ["0700.HK", "0881.HK", "0883.HK", ...]
    """
    tickers = []
    for f in sorted(ohlcv_dir.glob("*_daily.csv")):
        stem = f.stem.replace("_daily", "")
        if stem.startswith("INDEX_"):
            continue
        tickers.append(stem)
    return tickers
