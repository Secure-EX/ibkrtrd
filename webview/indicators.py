from __future__ import annotations

import numpy as np
import pandas as pd


VALID_TIMEFRAMES = ("daily", "weekly", "monthly")

# Sensible MA periods per timeframe (chart legibility + signal meaning).
MA_PERIODS_BY_TF: dict[str, tuple[int, ...]] = {
    "daily": (20, 60, 250),
    "weekly": (13, 26, 52),
    "monthly": (6, 12, 24),
}


def resample(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Resample daily OHLCV to weekly (W-FRI) or monthly (ME). Returns df unchanged for daily."""
    if tf == "daily" or df.empty:
        return df
    rule = {"weekly": "W-FRI", "monthly": "ME"}.get(tf)
    if rule is None:
        return df
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    keep = {k: v for k, v in agg.items() if k in df.columns}
    out = df.resample(rule).agg(keep).dropna(subset=["Close"])
    return out


def add_ma(df: pd.DataFrame, periods: tuple[int, ...] = (20, 60, 250)) -> pd.DataFrame:
    out = df.copy()
    for p in periods:
        out[f"ma{p}"] = out["Close"].rolling(p, min_periods=1).mean()
    return out


def add_bollinger(df: pd.DataFrame, period: int = 20, k: float = 2.0) -> pd.DataFrame:
    out = df.copy()
    mid = out["Close"].rolling(period, min_periods=period).mean()
    std = out["Close"].rolling(period, min_periods=period).std(ddof=0)
    out["bb_mid"] = mid
    out["bb_upper"] = mid + k * std
    out["bb_lower"] = mid - k * std
    return out


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    out = df.copy()
    ema_fast = out["Close"].ewm(span=fast, adjust=False).mean()
    ema_slow = out["Close"].ewm(span=slow, adjust=False).mean()
    out["macd"] = ema_fast - ema_slow
    out["macd_signal"] = out["macd"].ewm(span=signal, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]
    return out


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    out = df.copy()
    delta = out["Close"].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out["rsi"] = 100 - (100 / (1 + rs))
    return out


def slice_range(df: pd.DataFrame, choice: str) -> pd.DataFrame:
    if df.empty or choice == "All":
        return df
    end = df.index.max()
    years = {"1Y": 1, "3Y": 3, "5Y": 5}.get(choice)
    if years is None:
        return df
    start = end - pd.DateOffset(years=years)
    return df.loc[df.index >= start]


def build_full(df: pd.DataFrame, tf: str = "daily") -> pd.DataFrame:
    base = resample(df, tf)
    periods = MA_PERIODS_BY_TF.get(tf, MA_PERIODS_BY_TF["daily"])
    out = add_ma(base, periods)
    out = add_bollinger(out)
    out = add_macd(out)
    out = add_rsi(out)
    return out
