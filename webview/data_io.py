from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import markdown as md
import pandas as pd

from config import FINAL_REPORTS_DIR, LATEST_DIR, OHLCV_DIR

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


def clear_caches() -> None:
    load_ohlcv.cache_clear()
    load_payload.cache_clear()
