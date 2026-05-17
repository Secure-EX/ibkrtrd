"""
technical_financial.py — 财报数据加载模块

包含内容：
    - load_financial_series : 从三表财报 CSV 中提取逐期 EPS (TTM) 和 BVPS 序列

        文件命名约定：
            {ticker}_quarterly_income.csv   → Basic EPS, Net Income（优先）
            {ticker}_annual_income.csv      → （fallback）
            {ticker}_quarterly_balance.csv  → Stockholders Equity（优先）
            {ticker}_annual_balance.csv     → （fallback）

        EPS 还原逻辑：
            HK/A 股季报 EPS 是 YTD 累计值，需用标准 TTM 公式还原
            （与 derived_writer.write_valuation_history 同源）

        BVPS 推算逻辑：
            implied_shares = Net Income / Basic EPS （季度截面）
            BVPS = Stockholders Equity / implied_shares

        返回：(eps_series, bvps_series) — 两个以 Date 为索引的 pd.Series
              如果找不到文件或解析失败，返回 (None, None)

依赖：config.OHLCV_DIR
     technical_utils._ttm_from_ytd_series
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import OHLCV_DIR

try:
    from .technical_utils import _ttm_from_ytd_series
except ImportError:
    from technical_utils import _ttm_from_ytd_series


def load_financial_series(ticker_symbol: str, financial_dir: Path = None) -> tuple:
    """
    从三表财报 CSV 中提取逐期 EPS 和 BVPS 序列。

    文件命名约定：
        {ticker}_quarterly_income.csv   → Basic EPS, Net Income
        {ticker}_annual_income.csv      → (fallback)
        {ticker}_quarterly_balance.csv  → Stockholders Equity
        {ticker}_annual_balance.csv     → (fallback)

    BVPS 推算逻辑：
        implied_shares = Net Income / Basic EPS
        BVPS = Stockholders Equity / implied_shares

    参数:
        ticker_symbol : 股票代码，如 "0700.HK"
        financial_dir : 财报 CSV 所在目录，默认为 OHLCV_DIR 同级的 financials/

    返回:
        (eps_series, bvps_series) — 两个以 Date 为索引的 pd.Series
        如果找不到文件或解析失败，返回 (None, None)
    """
    if financial_dir is None:
        financial_dir = OHLCV_DIR.parent / "financials"

    ticker_fs = ticker_symbol

    # ------ 辅助：读取单个 CSV ------
    def _read_fin_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path)
            if df.empty or 'Date' not in df.columns:
                return None
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e:
            print(f"⚠️ 读取 {path} 失败: {e}")
            return None

    # ------ 1. 加载 income 表 (优先季报，fallback 年报) ------
    income_df = _read_fin_csv(financial_dir / f"{ticker_fs}_quarterly_income.csv")
    if income_df is None:
        income_df = _read_fin_csv(financial_dir / f"{ticker_fs}_annual_income.csv")

    # ------ 2. 加载 balance 表 ------
    balance_df = _read_fin_csv(financial_dir / f"{ticker_fs}_quarterly_balance.csv")
    if balance_df is None:
        balance_df = _read_fin_csv(financial_dir / f"{ticker_fs}_annual_balance.csv")

    # ------ 3. 提取 EPS（季报需 TTM 还原；年报 YTD = TTM） ------
    eps_series = None
    is_quarterly = (
        income_df is not None
        and (financial_dir / f"{ticker_fs}_quarterly_income.csv").exists()
    )
    if income_df is not None:
        for col in ['Basic EPS', 'Diluted EPS']:
            if col in income_df.columns:
                s = pd.to_numeric(income_df[col], errors='coerce').dropna()
                if not s.empty:
                    # 季报 EPS 是 YTD 累计值，需要还原为 TTM 才能正确算 PE
                    eps_series = _ttm_from_ytd_series(s) if is_quarterly else s
                    if eps_series is not None and not eps_series.empty:
                        break

    # ------ 4. 推算 BVPS ------
    bvps_series = None
    if balance_df is not None and income_df is not None:
        equity_col = None
        for col in ['Stockholders Equity', 'Total Equity Gross Minority Interest']:
            if col in balance_df.columns:
                equity_col = col
                break

        if equity_col and 'Basic EPS' in income_df.columns and 'Net Income' in income_df.columns:
            eps_raw = pd.to_numeric(income_df['Basic EPS'], errors='coerce')
            ni_raw = pd.to_numeric(income_df['Net Income'], errors='coerce')
            equity_raw = pd.to_numeric(balance_df[equity_col], errors='coerce')

            # 推算股本数 = Net Income / Basic EPS (取交集日期)
            common_idx = eps_raw.dropna().index.intersection(ni_raw.dropna().index)
            if len(common_idx) > 0:
                shares = (ni_raw.loc[common_idx] / eps_raw.loc[common_idx].replace(0, np.nan)).dropna()
                # 取最新的股本数，向前填充用于整段历史
                shares_full = shares.reindex(balance_df.index).ffill().bfill()
                bvps = (equity_raw / shares_full.replace(0, np.nan)).dropna()
                if not bvps.empty:
                    bvps_series = bvps

    if eps_series is not None:
        print(f"  ✅ EPS: {len(eps_series)} 期 ({eps_series.index.min().date()} ~ {eps_series.index.max().date()})")
    if bvps_series is not None:
        print(f"  ✅ BVPS: {len(bvps_series)} 期 ({bvps_series.index.min().date()} ~ {bvps_series.index.max().date()})")

    return eps_series, bvps_series


# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    test_ticker = "0700.HK"
    print(f"⚙️ 正在加载 {test_ticker} 的财报数据...")
    eps_series, bvps_series = load_financial_series(test_ticker)

    if eps_series is not None:
        print(f"\nEPS 序列（最新5期）:")
        print(eps_series.tail(5).to_string())
    else:
        print("⚠️ 未找到 EPS 数据")

    if bvps_series is not None:
        print(f"\nBVPS 序列（最新5期）:")
        print(bvps_series.tail(5).to_string())
    else:
        print("⚠️ 未找到 BVPS 数据")
