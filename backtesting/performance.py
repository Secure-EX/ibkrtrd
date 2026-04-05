"""
performance.py — 回测绩效指标计算

接收权益曲线 DataFrame 和交易列表，计算标准绩效指标。
公式与 processors/technical_risk.py 中保持一致。

公开函数:
    calculate_performance(equity_df, trades, benchmark_df, risk_free_rate) → dict
"""

from __future__ import annotations
import sys
import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

from backtesting.simulator import Trade


def calculate_performance(
    equity_df: pd.DataFrame,
    trades: List[Trade],
    benchmark_df: Optional[pd.DataFrame],
    risk_free_rate: float = 0.04,
) -> Dict[str, Any]:
    """
    计算完整的回测绩效指标。

    参数:
        equity_df       : 权益曲线 DataFrame，index=date，含 equity 列
        trades          : 成交记录列表
        benchmark_df    : 基准指数 DataFrame（含 Close 列），可为 None
        risk_free_rate  : 无风险利率（年化）

    返回: 指标字典
    """
    if equity_df is None or equity_df.empty:
        return {"error": "权益曲线为空"}

    equity = equity_df['equity']
    initial = float(equity.iloc[0])
    final = float(equity.iloc[-1])

    # ---- 时间跨度 ----
    start_date = equity.index[0]
    end_date = equity.index[-1]
    years = max((end_date - start_date).days / 365.25, 1 / 365.25)

    # ---- 总收益 / 年化收益 ----
    total_return = (final - initial) / initial
    annualized_return = (1 + total_return) ** (1 / years) - 1

    # ---- 日收益率序列 ----
    daily_ret = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()

    # ---- 夏普比率 ----
    if len(daily_ret) > 1 and daily_ret.std() > 0:
        ann_ret = daily_ret.mean() * 252
        ann_vol = daily_ret.std() * np.sqrt(252)
        sharpe = (ann_ret - risk_free_rate) / ann_vol
    else:
        sharpe = None
        ann_vol = None

    # ---- 最大回撤 ----
    rolling_max = equity.cummax()
    drawdowns = (equity - rolling_max) / rolling_max
    max_drawdown = float(drawdowns.min())

    # 最大回撤持续天数
    max_dd_duration = _calc_max_drawdown_duration(equity)

    # ---- Calmar 比率 ----
    calmar = (
        annualized_return / abs(max_drawdown)
        if max_drawdown != 0 else None
    )

    # ---- 交易统计 ----
    sell_trades = [t for t in trades if t.action == "sell"]
    total_trades = len(sell_trades)
    win_trades = [t for t in sell_trades if t.pnl > 0]
    loss_trades = [t for t in sell_trades if t.pnl <= 0]

    win_rate = len(win_trades) / total_trades if total_trades > 0 else None
    avg_win_pct = float(np.mean([t.pnl_pct for t in win_trades])) if win_trades else None
    avg_loss_pct = float(np.mean([t.pnl_pct for t in loss_trades])) if loss_trades else None

    gross_profit = sum(t.pnl for t in win_trades)
    gross_loss = abs(sum(t.pnl for t in loss_trades))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

    # ---- 平均持仓天数（用 tranche_id 精确匹配买卖对）----
    buy_by_tranche = {t.tranche_id: t for t in trades if t.action == "buy"}
    holding_days_list = []
    for st in sell_trades:
        matched_buy = buy_by_tranche.get(st.tranche_id)
        if matched_buy:
            holding_days_list.append((st.date - matched_buy.date).days)
    avg_holding_days = float(np.mean(holding_days_list)) if holding_days_list else None

    # ---- 基准比较 ----
    benchmark_return = None
    alpha = None
    if benchmark_df is not None and 'Close' in benchmark_df.columns:
        bench = benchmark_df['Close'].reindex(equity.index, method='ffill').dropna()
        if len(bench) >= 2:
            bench_total = (float(bench.iloc[-1]) - float(bench.iloc[0])) / float(bench.iloc[0])
            bench_years = max((bench.index[-1] - bench.index[0]).days / 365.25, 1 / 365.25)
            benchmark_return = (1 + bench_total) ** (1 / bench_years) - 1
            alpha = annualized_return - benchmark_return

    # ---- 月度/年度收益分解 ----
    monthly_returns = _calc_period_returns(equity, 'ME')
    yearly_returns = _calc_period_returns(equity, 'YE')

    # ---- 回测区间权益曲线附加列 ----
    equity_df = equity_df.copy()
    equity_df['return_pct'] = (equity_df['equity'] / initial - 1) * 100
    equity_df['drawdown_pct'] = drawdowns * 100

    return {
        # 基本信息
        "start_date": str(start_date.date()),
        "end_date": str(end_date.date()),
        "backtest_years": round(years, 2),
        "initial_capital": round(initial, 2),
        "final_equity": round(final, 2),

        # 收益指标
        "total_return_pct": round(total_return * 100, 2),
        "annualized_return_pct": round(annualized_return * 100, 2),
        "annualized_volatility_pct": round(ann_vol * 100, 2) if ann_vol else None,

        # 风险指标
        "sharpe_ratio": round(sharpe, 3) if sharpe is not None else None,
        "max_drawdown_pct": round(max_drawdown * 100, 2),
        "max_drawdown_duration_days": max_dd_duration,
        "calmar_ratio": round(calmar, 3) if calmar is not None else None,

        # 交易统计
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate * 100, 1) if win_rate is not None else None,
        "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
        "avg_win_pct": round(avg_win_pct * 100, 2) if avg_win_pct is not None else None,
        "avg_loss_pct": round(avg_loss_pct * 100, 2) if avg_loss_pct is not None else None,
        "avg_holding_days": round(avg_holding_days, 1) if avg_holding_days is not None else None,

        # 基准比较
        "benchmark_annualized_return_pct": (
            round(benchmark_return * 100, 2) if benchmark_return is not None else None
        ),
        "alpha_pct": round(alpha * 100, 2) if alpha is not None else None,

        # 分解收益
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,

        # 附带修改后的权益曲线
        "_equity_df_enriched": equity_df,
    }


# ============================================================
# 辅助函数
# ============================================================

def _calc_max_drawdown_duration(equity: pd.Series) -> Optional[int]:
    """计算最大回撤持续天数（从峰值到恢复或回测结束）。"""
    rolling_max = equity.cummax()
    in_drawdown = equity < rolling_max

    max_dur = 0
    cur_dur = 0
    for flag in in_drawdown:
        if flag:
            cur_dur += 1
            max_dur = max(max_dur, cur_dur)
        else:
            cur_dur = 0
    return max_dur if max_dur > 0 else None


def _calc_period_returns(equity: pd.Series, freq: str) -> dict:
    """
    按周期（'M' 月 / 'Y' 年）计算区间收益率。
    返回: {"2020-01": 3.2, "2020-02": -1.5, ...}
    """
    resampled = equity.resample(freq).last()
    period_ret = resampled.pct_change().dropna()

    result = {}
    for date, ret in period_ret.items():
        if freq in ('M', 'ME'):
            key = date.strftime('%Y-%m')
        else:
            key = str(date.year)
        if not np.isnan(ret):
            result[key] = round(float(ret) * 100, 2)
    return result
