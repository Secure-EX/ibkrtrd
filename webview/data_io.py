from __future__ import annotations

import json
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import markdown as md
import pandas as pd

from config import (
    DERIVED_TECHNICAL_DIR,
    DERIVED_VALUATION_DIR,
    FINAL_REPORTS_DIR,
    FINANCIALS_DIR,
    LATEST_DIR,
    OHLCV_DIR,
    PORTFOLIO_DIR,
    SENTIMENT_MASTER_PARQUET,
    TRANSACTIONS_DIR,
)

REPORT_GLOB = "CLAUDE_staged_*.md"


def list_reports() -> list[Path]:
    files = sorted(FINAL_REPORTS_DIR.glob(REPORT_GLOB), reverse=True)
    return [p for p in files if p.is_file()]


def read_report_md(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def render_markdown(md_text: str) -> str:
    return md.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
        output_format="html5",
    )


def list_tickers() -> list[str]:
    out: list[str] = []
    for p in sorted(OHLCV_DIR.glob("*_daily.csv")):
        stem = p.stem.replace("_daily", "")
        if stem.startswith("INDEX_"):
            continue
        out.append(stem)
    return out


@lru_cache(maxsize=64)
def load_ohlcv(ticker: str) -> pd.DataFrame:
    """Raw daily OHLCV — kept for transactions filtering only. Webview chart 走 load_technical。"""
    path = OHLCV_DIR / f"{ticker}_daily.csv"
    df = pd.read_csv(path, parse_dates=["Date"])
    df = (
        df.dropna(subset=["Date"])
          .drop_duplicates(subset=["Date"], keep="last")
          .sort_values("Date")
          .set_index("Date")
    )
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    return df[keep].astype(float)


@lru_cache(maxsize=64)
def load_technical(ticker: str, tf: str = "daily") -> pd.DataFrame:
    """Read derived/technical/<ticker>_<tf>.parquet。返回空 DataFrame 表示数据缺失。"""
    path = DERIVED_TECHNICAL_DIR / f"{ticker}_{tf}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


@lru_cache(maxsize=64)
def load_valuation(ticker: str) -> pd.DataFrame:
    """Read derived/valuation/<ticker>_daily.parquet (Close, PE_TTM, PB, PS_TTM, ...)."""
    path = DERIVED_VALUATION_DIR / f"{ticker}_daily.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


@lru_cache(maxsize=1)
def load_sentiment_master() -> pd.DataFrame:
    if not SENTIMENT_MASTER_PARQUET.exists():
        return pd.DataFrame()
    return pd.read_parquet(SENTIMENT_MASTER_PARQUET)


@lru_cache(maxsize=64)
def load_company_info(ticker: str) -> dict:
    """yfinance info.json 直读 — 用于 trailingPE/trailingEps 等当前快照字段比对。"""
    path = FINANCIALS_DIR / f"{ticker}_info.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


@lru_cache(maxsize=64)
def load_payload(ticker: str) -> dict | None:
    path = LATEST_DIR / f"{ticker}_LLM_Payload.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def find_latest_stages_dir() -> Path | None:
    candidates = sorted(LATEST_DIR.glob("web_prompts_*"), reverse=True)
    for d in candidates:
        stages = d / "stages"
        if stages.is_dir():
            return stages
    return None


def load_stage1_full(ticker: str) -> tuple[str, Path] | None:
    """Return (markdown_text, source_path) for the ticker's stage1_<TICKER>_full.md, or None."""
    stages = find_latest_stages_dir()
    if stages is None:
        return None
    # Ticker like "0700.HK" -> filename uses "0700_HK"
    safe = ticker.replace(".", "_")
    path = stages / f"stage1_{safe}_full.md"
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8"), path
    except OSError:
        return None


def parse_report_date(filename: str) -> str:
    stem = Path(filename).stem
    digits = "".join(ch for ch in stem if ch.isdigit())
    if len(digits) == 8:
        try:
            return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return stem


def _normalize_symbol(raw) -> str | None:
    """IBKR positions CSV stores symbol as bare digits (700 or '700.0' -> '0700.HK')."""
    if raw is None:
        return None
    try:
        n = int(float(raw))
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return f"{n:04d}.HK"


@lru_cache(maxsize=1)
def load_positions() -> list[dict]:
    """Load most recent current_positions_*.csv, normalize symbols, return list of dicts."""
    files = sorted(PORTFOLIO_DIR.glob("current_positions_*.csv"), reverse=True)
    if not files:
        return []
    df = pd.read_csv(files[0])
    out: list[dict] = []
    for _, row in df.iterrows():
        sym = _normalize_symbol(row.get("Symbol"))
        if not sym:
            continue
        try:
            out.append({
                "symbol": sym,
                "name_en": str(row.get("Company Name (EN)", "")).strip(),
                "currency": str(row.get("Currency", "")).strip(),
                "qty": float(row.get("Position", 0) or 0),
                "avg_price": float(row.get("Avg Price", 0) or 0),
                "last": float(row.get("Last", 0) or 0),
                "change_ratio": float(row.get("Change Ratio", 0) or 0),
                "market_value": float(row.get("Market Value", 0) or 0),
                "cost_basis": float(row.get("Cost Basis", 0) or 0),
                "unrealized_pnl": float(row.get("Unrealized P&L", 0) or 0),
                "unrealized_pnl_ratio": float(row.get("Unrealized P&L Ratio", 0) or 0),
                "weight_ratio": float(row.get("Net Liq Ratio", 0) or 0),
            })
        except (ValueError, TypeError):
            continue
    out.sort(key=lambda r: r["weight_ratio"], reverse=True)
    return out


def get_position(ticker: str) -> dict | None:
    for p in load_positions():
        if p["symbol"] == ticker:
            return p
    return None


@lru_cache(maxsize=1)
def load_all_trades() -> pd.DataFrame:
    path = TRANSACTIONS_DIR / "transactions_master.csv"
    if not path.exists():
        return pd.DataFrame(columns=["ticker", "Time", "Action", "Quantity", "Price", "amount", "Realized_PnL"])
    df = pd.read_csv(path, parse_dates=["Time"])
    df["ticker"] = df["Symbol"].apply(_normalize_symbol)
    df = df.dropna(subset=["ticker", "Time"]).sort_values("Time").reset_index(drop=True)
    df["amount"] = df["Quantity"].astype(float) * df["Price"].astype(float)
    return df


def get_trades(ticker: str, start=None, end=None) -> list[dict]:
    df = load_all_trades()
    if df.empty:
        return []
    df = df[df["ticker"] == ticker]
    if start is not None:
        df = df[df["Time"] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df["Time"] <= pd.Timestamp(end)]
    out: list[dict] = []
    for row in df.itertuples(index=False):
        action = str(row.Action).upper()
        out.append({
            "date": row.Time.strftime("%Y-%m-%d"),
            "action": action,
            "qty": float(row.Quantity),
            "price": float(row.Price),
            "amount": float(row.amount),
            "realized_pnl": float(row.Realized_PnL) if action == "SELL" else None,
        })
    return out


@lru_cache(maxsize=1)
def load_portfolio_summary() -> dict | None:
    path = LATEST_DIR / "portfolio_risk.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def latest_data_date() -> str | None:
    """Date of newest portfolio snapshot (proxy for 'latest data refresh')."""
    files = sorted(PORTFOLIO_DIR.glob("current_positions_*.csv"), reverse=True)
    if not files:
        return None
    digits = "".join(ch for ch in files[0].stem if ch.isdigit())
    if len(digits) == 8:
        try:
            return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


_H1_RE = re.compile(r"^\s*#\s+(.+?)\s*$", re.MULTILINE)
_BLOCKQUOTE_RE = re.compile(r"^\s*>\s+(.+?)\s*$", re.MULTILINE)


def split_stage1_head(md_text: str) -> tuple[str | None, str | None, str]:
    """Pull the first H1 + first blockquote line out of the markdown.
    Returns (title, meta_line, body_without_them). The body keeps formatting
    so downstream markdown rendering is unaffected for the rest of the doc."""
    title = None
    meta = None
    body = md_text

    h1 = _H1_RE.search(body)
    if h1:
        title = h1.group(1).strip()
        body = body[:h1.start()] + body[h1.end():]

    bq = _BLOCKQUOTE_RE.search(body)
    if bq:
        meta = bq.group(1).strip()
        body = body[:bq.start()] + body[bq.end():]

    body = body.lstrip("\n").lstrip("-").lstrip("\n")
    return title, meta, body


def clear_caches() -> None:
    load_ohlcv.cache_clear()
    load_technical.cache_clear()
    load_valuation.cache_clear()
    load_sentiment_master.cache_clear()
    load_company_info.cache_clear()
    load_payload.cache_clear()
    load_positions.cache_clear()
    load_portfolio_summary.cache_clear()
    load_all_trades.cache_clear()
