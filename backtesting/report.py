"""
report.py — 回测结果输出模块

生成以下输出文件（保存至 data/output/backtest/{run_id}/）：
    performance_summary.json  — 全部绩效指标
    trade_log.csv             — 逐笔成交记录
    equity_curve.csv          — 每日权益曲线（含回撤）
    backtest_chart.png        — 可视化图表（可选，需 matplotlib）

公开函数:
    generate_report(config, metrics, equity_df, trades, run_id, plot=False) → Path
"""

from __future__ import annotations
import sys
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

from backtesting.config_bt import BacktestConfig
from backtesting.simulator import Trade


def generate_report(
    config: BacktestConfig,
    metrics: Dict[str, Any],
    equity_df: pd.DataFrame,
    trades: List[Trade],
    run_id: Optional[str] = None,
    plot: bool = False,
) -> Path:
    """
    生成并保存所有输出文件。

    参数:
        config    : 回测配置
        metrics   : calculate_performance() 返回的指标字典
        equity_df : 权益曲线 DataFrame（可能包含 _equity_df_enriched）
        trades    : 成交记录列表
        run_id    : 输出子目录名称，None 时自动生成
        plot      : 是否生成图表（需要 matplotlib）

    返回: 输出目录路径
    """
    if run_id is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{config.label()}_{ts}"

    out_dir = config.output_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n  [Report] 输出目录: {out_dir}")

    # 1. performance_summary.json
    _save_performance_json(metrics, out_dir)

    # 2. trade_log.csv
    _save_trade_log(trades, out_dir)

    # 3. equity_curve.csv
    enriched = metrics.pop("_equity_df_enriched", equity_df)
    _save_equity_curve(enriched, out_dir)

    # 4. 可视化图表（可选）
    if plot:
        try:
            _plot_backtest(enriched, trades, metrics, config, out_dir)
        except ImportError:
            print("  [Report] ⚠️  matplotlib 未安装，跳过图表生成")
        except Exception as e:
            print(f"  [Report] ⚠️  图表生成失败: {e}")

    print(f"  [Report] ✅ 报告已生成: {out_dir}")
    return out_dir


# ============================================================
# 子函数
# ============================================================

def _save_performance_json(metrics: Dict[str, Any], out_dir: Path):
    """保存绩效摘要 JSON，过滤掉 DataFrame 等不可序列化对象。"""
    clean = {
        k: v for k, v in metrics.items()
        if not k.startswith("_") and not isinstance(v, pd.DataFrame)
    }
    path = out_dir / "performance_summary.json"
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"  [Report] ✅ performance_summary.json")


def _save_trade_log(trades: List[Trade], out_dir: Path):
    """保存交易日志 CSV。"""
    if not trades:
        print("  [Report] ⚠️  无成交记录，跳过 trade_log.csv")
        return

    rows = []
    cumulative_pnl = 0.0
    for t in trades:
        if t.action == "sell":
            cumulative_pnl += t.pnl
        rows.append({
            "date": t.date.strftime("%Y-%m-%d"),
            "action": t.action,
            "price": round(t.price, 4),
            "shares": t.shares,
            "commission": round(t.commission, 2),
            "proceeds": round(t.proceeds, 2),
            "pnl": round(t.pnl, 2) if t.action == "sell" else "",
            "pnl_pct": f"{t.pnl_pct:.2%}" if t.action == "sell" else "",
            "cumulative_pnl": round(cumulative_pnl, 2) if t.action == "sell" else "",
            "long_term_risk": t.long_term_risk,
            "short_term_risk": t.short_term_risk,
            "reason": t.reason,
        })

    df = pd.DataFrame(rows)
    path = out_dir / "trade_log.csv"
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"  [Report] ✅ trade_log.csv ({len(trades)} 条)")


def _save_equity_curve(equity_df: pd.DataFrame, out_dir: Path):
    """保存权益曲线 CSV。"""
    if equity_df is None or equity_df.empty:
        print("  [Report] ⚠️  权益曲线为空，跳过")
        return

    path = out_dir / "equity_curve.csv"
    equity_df.to_csv(path, encoding='utf-8-sig')
    print(f"  [Report] ✅ equity_curve.csv ({len(equity_df)} 行)")


def _plot_backtest(
    equity_df: pd.DataFrame,
    trades: List[Trade],
    metrics: Dict[str, Any],
    config: BacktestConfig,
    out_dir: Path,
):
    """生成回测可视化图表（3个子图）。"""
    import matplotlib
    matplotlib.use('Agg')  # 非交互模式
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, axes = plt.subplots(3, 1, figsize=(14, 12),
                              gridspec_kw={'height_ratios': [3, 1.5, 1]})
    fig.suptitle(
        f"{config.ticker} 回测结果 | {config.strategy_name} | "
        f"{metrics.get('start_date')} ~ {metrics.get('end_date')}",
        fontsize=13, fontweight='bold'
    )

    # ---- 子图1：权益曲线 ----
    ax1 = axes[0]
    equity = equity_df['equity']
    ax1.plot(equity.index, equity.values / equity.values[0],
             label='策略净值', color='#2196F3', linewidth=1.5)

    # 买卖标记
    buy_dates = [t.date for t in trades if t.action == "buy"]
    sell_dates = [t.date for t in trades if t.action == "sell"]
    for bd in buy_dates:
        if bd in equity.index:
            ax1.axvline(bd, color='green', alpha=0.3, linewidth=0.8)
    for sd in sell_dates:
        if sd in equity.index:
            ax1.axvline(sd, color='red', alpha=0.3, linewidth=0.8)

    ax1.set_ylabel('净值（初始=1.0）')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)

    # ---- 子图2：回撤 ----
    ax2 = axes[1]
    dd = equity_df.get('drawdown_pct', (equity / equity.cummax() - 1) * 100)
    ax2.fill_between(dd.index, dd.values, 0, alpha=0.4, color='#F44336', label='回撤%')
    ax2.set_ylabel('回撤 (%)')
    ax2.legend(loc='lower left')
    ax2.grid(True, alpha=0.3)

    # ---- 子图3：月度收益热力图（条形）----
    ax3 = axes[2]
    monthly = metrics.get('monthly_returns', {})
    if monthly:
        months = list(monthly.keys())[-36:]  # 最近 3 年
        values = [monthly[m] for m in months]
        colors = ['#4CAF50' if v >= 0 else '#F44336' for v in values]
        x_pos = range(len(months))
        ax3.bar(x_pos, values, color=colors, alpha=0.8, width=0.8)
        # 只显示部分 x 标签避免拥挤
        step = max(1, len(months) // 12)
        ax3.set_xticks(list(x_pos)[::step])
        ax3.set_xticklabels(months[::step], rotation=45, fontsize=7)
        ax3.axhline(0, color='black', linewidth=0.5)
        ax3.set_ylabel('月收益 (%)')
        ax3.grid(True, alpha=0.3, axis='y')

    # 统计摘要文本
    summary = (
        f"总收益: {metrics.get('total_return_pct', 'N/A')}%  |  "
        f"年化: {metrics.get('annualized_return_pct', 'N/A')}%  |  "
        f"Sharpe: {metrics.get('sharpe_ratio', 'N/A')}  |  "
        f"最大回撤: {metrics.get('max_drawdown_pct', 'N/A')}%  |  "
        f"胜率: {metrics.get('win_rate_pct', 'N/A')}%  |  "
        f"Alpha: {metrics.get('alpha_pct', 'N/A')}%"
    )
    fig.text(0.5, 0.01, summary, ha='center', fontsize=9,
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.4))

    plt.tight_layout(rect=[0, 0.04, 1, 0.96])

    path = out_dir / "backtest_chart.png"
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [Report] ✅ backtest_chart.png")
