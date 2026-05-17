"""
derived_writer.py — 派生时序数据落盘层

把所有"预计算 + 重采样 + 历史时序"集中到这里，落成 parquet。
webview/分析层只读这些 parquet，不再二次计算。

输出位置：
    data/output/derived/technical/<ticker>_{daily,weekly,monthly}.parquet
    data/output/derived/valuation/<ticker>_daily.parquet
    data/output/derived/sentiment/sentiment_master.parquet  (单一 master)

公开接口：
    write_technical_history(ticker)   -> dict[tf, Path]
    write_valuation_history(ticker)   -> Path | None
    append_sentiment_archive(ticker)  -> int  # 新增行数
    backfill_all()                    -> dict[ticker, dict]

CLI：
    python -m processors.derived_writer --backfill-all
    python -m processors.derived_writer --ticker 0700.HK
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config import (
    DERIVED_SENTIMENT_DIR,
    DERIVED_TECHNICAL_DIR,
    DERIVED_VALUATION_DIR,
    FINANCIALS_DIR,
    OHLCV_DIR,
    SENTIMENT_DIR,
    SENTIMENT_MASTER_PARQUET,
)

try:
    from .technical_indicators import _add_technical_indicators
    from .technical_utils import _ttm_from_ytd_series, RESAMPLE_AGG, RESAMPLE_RULES
except ImportError:
    from processors.technical_indicators import _add_technical_indicators
    from processors.technical_utils import _ttm_from_ytd_series, RESAMPLE_AGG, RESAMPLE_RULES


# ==========================================================================
# 1. 技术面历史时序
# ==========================================================================


def _load_daily_ohlcv(ticker: str) -> pd.DataFrame | None:
    path = OHLCV_DIR / f"{ticker}_daily.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=["Date"])
    df = (
        df.dropna(subset=["Date"])
          .drop_duplicates(subset=["Date"], keep="last")
          .sort_values("Date")
          .set_index("Date")
    )
    if "Turnover_Value" not in df.columns:
        # 旧文件缺这列时 VWAP_Custom 会退化为 Close — 先补占位
        df["Turnover_Value"] = df["Close"] * df["Volume"]
    return df


def write_technical_history(ticker: str) -> dict[str, Path]:
    """读 OHLCV CSV → 加全套技术指标 → 重采样 weekly/monthly → 各落一份 parquet。"""
    df_daily = _load_daily_ohlcv(ticker)
    if df_daily is None or df_daily.empty:
        return {}

    written: dict[str, Path] = {}

    # daily
    daily = _add_technical_indicators(df_daily.copy())
    daily_path = DERIVED_TECHNICAL_DIR / f"{ticker}_daily.parquet"
    daily.to_parquet(daily_path)
    written["daily"] = daily_path

    # weekly / monthly
    keep = {k: v for k, v in RESAMPLE_AGG.items() if k in df_daily.columns}
    for tf, rule in RESAMPLE_RULES.items():
        resampled = df_daily.resample(rule).agg(keep).dropna(subset=["Close"])
        if len(resampled) < 20:
            continue
        with_ind = _add_technical_indicators(resampled)
        out = DERIVED_TECHNICAL_DIR / f"{ticker}_{tf}.parquet"
        with_ind.to_parquet(out)
        written[tf] = out

    return written


# ==========================================================================
# 2. 估值时序 (PE/PB/PS_TTM)
# ==========================================================================

def _load_quarterly_eps_revenue(ticker: str) -> pd.DataFrame:
    """季度 income → DataFrame[Date, eps_ttm, revenue_ttm]，已用标准 TTM 公式还原。"""
    path = FINANCIALS_DIR / f"{ticker}_quarterly_income.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["Date"])
    if df.empty or "Date" not in df.columns:
        return pd.DataFrame()
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
    eps_col = "Diluted EPS" if "Diluted EPS" in df.columns else "Basic EPS"
    rev_col = "Total Revenue" if "Total Revenue" in df.columns else "Operating Revenue"
    eps_ytd = pd.to_numeric(df[eps_col], errors="coerce") if eps_col in df.columns else pd.Series(dtype=float, index=df.index)
    rev_ytd = pd.to_numeric(df[rev_col], errors="coerce") if rev_col in df.columns else pd.Series(dtype=float, index=df.index)
    return pd.DataFrame({
        "eps_ttm": _ttm_from_ytd_series(eps_ytd),
        "revenue_ttm": _ttm_from_ytd_series(rev_ytd),
    })


def _load_quarterly_equity(ticker: str) -> pd.Series:
    path = FINANCIALS_DIR / f"{ticker}_quarterly_balance.csv"
    if not path.exists():
        return pd.Series(dtype=float)
    df = pd.read_csv(path, parse_dates=["Date"])
    if df.empty or "Date" not in df.columns:
        return pd.Series(dtype=float)
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
    col = "Stockholders Equity" if "Stockholders Equity" in df.columns else (
        "Total Equity Gross Minority Interest" if "Total Equity Gross Minority Interest" in df.columns else None
    )
    if col is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce").dropna()


def _load_shares_outstanding(ticker: str) -> float | None:
    path = FINANCIALS_DIR / f"{ticker}_info.json"
    if not path.exists():
        return None
    try:
        info = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    val = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
    return float(val) if val else None


def write_valuation_history(ticker: str) -> Path | None:
    """读季度财报 → TTM 滚动 → 与 daily Close 合并 → PE/PB/PS_TTM 时序落盘。"""
    df_daily = _load_daily_ohlcv(ticker)
    if df_daily is None or df_daily.empty:
        return None

    eps_rev = _load_quarterly_eps_revenue(ticker)
    equity_q = _load_quarterly_equity(ticker)
    shares_out = _load_shares_outstanding(ticker)

    # eps_ttm/revenue_ttm 已经是标准 TTM 序列（_ttm_from_ytd_series 处理过），不再 rolling
    eps_ttm_q = eps_rev["eps_ttm"].dropna() if not eps_rev.empty else pd.Series(dtype=float)
    rev_ttm_q = eps_rev["revenue_ttm"].dropna() if not eps_rev.empty else pd.Series(dtype=float)

    # BVPS：每季 Stockholders Equity / shares_out（用最新股本近似历史，info.json 没有历史股本）
    if not equity_q.empty and shares_out:
        bvps_q = equity_q / shares_out
    else:
        bvps_q = pd.Series(dtype=float)

    close = df_daily["Close"].astype(float)
    out = pd.DataFrame(index=close.index)
    out["Close"] = close

    if not eps_ttm_q.empty:
        out["EPS_TTM"] = eps_ttm_q.reindex(out.index.union(eps_ttm_q.index)).sort_index().ffill().reindex(out.index)
    else:
        out["EPS_TTM"] = np.nan

    if not rev_ttm_q.empty:
        out["Revenue_TTM"] = rev_ttm_q.reindex(out.index.union(rev_ttm_q.index)).sort_index().ffill().reindex(out.index)
    else:
        out["Revenue_TTM"] = np.nan

    if not bvps_q.empty:
        out["BVPS"] = bvps_q.reindex(out.index.union(bvps_q.index)).sort_index().ffill().reindex(out.index)
    else:
        out["BVPS"] = np.nan

    out["Shares_Out"] = shares_out if shares_out else np.nan
    out["MarketCap_implied"] = out["Close"] * out["Shares_Out"]

    # PE/PB/PS：分母 ≤ 0 时设 NaN（保留行，方便前端按需切片）
    out["PE_TTM"] = np.where(out["EPS_TTM"] > 0, out["Close"] / out["EPS_TTM"], np.nan)
    out["PB"] = np.where(out["BVPS"] > 0, out["Close"] / out["BVPS"], np.nan)
    out["PS_TTM"] = np.where(out["Revenue_TTM"] > 0, out["MarketCap_implied"] / out["Revenue_TTM"], np.nan)

    # 全空就跳过落盘（没有季度财报，下一轮再看）
    if out[["PE_TTM", "PB", "PS_TTM"]].dropna(how="all").empty:
        return None

    path = DERIVED_VALUATION_DIR / f"{ticker}_daily.parquet"
    out.to_parquet(path)
    return path


# ==========================================================================
# 3. Sentiment 历史归档（master parquet，按 url_hash 去重）
# ==========================================================================

_SENTIMENT_COLUMNS = [
    "ticker", "date", "captured_at",
    "title", "source", "url", "url_hash", "vendor",
]


def _url_hash(url: str) -> str:
    return hashlib.md5((url or "").encode("utf-8", errors="ignore")).hexdigest()[:16]


def _load_master() -> pd.DataFrame:
    if not SENTIMENT_MASTER_PARQUET.exists():
        return pd.DataFrame(columns=_SENTIMENT_COLUMNS)
    return pd.read_parquet(SENTIMENT_MASTER_PARQUET)


def append_sentiment_archive(ticker: str) -> int:
    """读 <ticker>_news.json → 追加到 master parquet（按 (ticker, url_hash) 去重）→ 返回新增行数。"""
    news_file = SENTIMENT_DIR / f"{ticker}_news.json"
    if not news_file.exists():
        return 0
    try:
        articles = json.loads(news_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0
    if not articles:
        return 0

    captured_at = pd.Timestamp.now("UTC").tz_localize(None)
    rows = []
    for a in articles:
        url = a.get("url") or ""
        rows.append({
            "ticker": ticker,
            "date": pd.to_datetime(a.get("date"), errors="coerce"),
            "captured_at": captured_at,
            "title": a.get("title") or "",
            "source": a.get("source") or "",
            "url": url,
            "url_hash": _url_hash(url) if url else _url_hash(a.get("title") or ""),
            "vendor": a.get("data_vendor") or "",
        })
    new_df = pd.DataFrame(rows, columns=_SENTIMENT_COLUMNS)
    new_df = new_df.dropna(subset=["date"])
    if new_df.empty:
        return 0

    master = _load_master()
    if master.empty:
        merged = new_df
    else:
        # 按 (ticker, url_hash) 防重复，旧记录优先（保留首次 captured_at）
        existing_keys = set(zip(master["ticker"], master["url_hash"]))
        keep_mask = ~new_df.apply(lambda r: (r["ticker"], r["url_hash"]) in existing_keys, axis=1)
        net_new = new_df[keep_mask]
        if net_new.empty:
            return 0
        merged = pd.concat([master, net_new], ignore_index=True)

    merged = merged.sort_values(["ticker", "date"], ascending=[True, False]).reset_index(drop=True)
    SENTIMENT_MASTER_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(SENTIMENT_MASTER_PARQUET, index=False)
    return len(merged) - len(master)


# ==========================================================================
# 4. Backfill / CLI
# ==========================================================================

def _list_tickers() -> list[str]:
    return sorted({
        p.stem.replace("_daily", "")
        for p in OHLCV_DIR.glob("*_daily.csv")
        if not p.stem.startswith("INDEX_")
    })


def backfill_all() -> dict[str, dict]:
    summary: dict[str, dict] = {}
    for ticker in _list_tickers():
        report: dict = {}
        try:
            tech = write_technical_history(ticker)
            report["technical"] = {tf: str(p.relative_to(BASE_DIR)) for tf, p in tech.items()}
        except Exception as e:
            report["technical_error"] = repr(e)
        try:
            val = write_valuation_history(ticker)
            report["valuation"] = str(val.relative_to(BASE_DIR)) if val else None
        except Exception as e:
            report["valuation_error"] = repr(e)
        try:
            n = append_sentiment_archive(ticker)
            report["sentiment_new_rows"] = n
        except Exception as e:
            report["sentiment_error"] = repr(e)
        summary[ticker] = report
        print(f"  [OK] {ticker}: {report}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Derived 时序数据落盘工具")
    parser.add_argument("--backfill-all", action="store_true", help="对所有 ticker 全量重建")
    parser.add_argument("--ticker", type=str, help="仅处理单只 ticker（如 0700.HK）")
    args = parser.parse_args()

    if args.backfill_all:
        backfill_all()
        return
    if args.ticker:
        t = args.ticker
        print(f"Technical: {write_technical_history(t)}")
        print(f"Valuation: {write_valuation_history(t)}")
        print(f"Sentiment new rows: {append_sentiment_archive(t)}")
        return
    parser.print_help()


if __name__ == "__main__":
    main()
