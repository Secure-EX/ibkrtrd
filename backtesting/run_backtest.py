"""
run_backtest.py — CLI 入口点

用法:
    # 单股，默认多因子策略
    python -m backtesting.run_backtest --ticker 0700.HK

    # 自定义时间范围
    python -m backtesting.run_backtest --ticker 0700.HK --start 2018-01-01 --end 2024-12-31

    # 自定义策略参数
    python -m backtesting.run_backtest --ticker 0700.HK --buy-threshold 0.10 --sell-threshold 0.90

    # 生成图表
    python -m backtesting.run_backtest --ticker 0700.HK --plot

    # 其他策略
    python -m backtesting.run_backtest --ticker 0700.HK --strategy composite

    # 运行所有可用股票
    python -m backtesting.run_backtest --all

也可作为函数调用：
    from backtesting.run_backtest import run_backtest
    metrics = run_backtest("0700.HK", start_date="2018-01-01", plot=True)
"""

from __future__ import annotations
import argparse
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

from backtesting.config_bt import BacktestConfig
from backtesting.data_loader import load_ohlcv, load_index_ohlcv, load_financials, list_available_tickers, load_board_lot
from backtesting.signal_engine import SignalEngine
from backtesting.strategy import create_strategy, SignalConfirmationFilter
from backtesting.simulator import Simulator
from backtesting.simulator import DynamicStopLoss
from backtesting.performance import calculate_performance
from backtesting.report import generate_report
from config import OHLCV_DIR


try:
    # 复用现有的 _load_index_data（从 processors/technical_market.py）
    from processors.technical_market import _load_index_data
except ImportError:
    _load_index_data = None


def run_backtest(
    ticker: str,
    strategy_name: str = "multifactor_risk",
    start_date: str = "2015-01-01",
    end_date: Optional[str] = None,
    initial_capital: float = 1_000_000.0,
    fixed_fraction: float = 0.25,
    max_tranches: int = 3,
    buy_threshold: float = 0.05,
    sell_threshold: float = 0.95,
    stop_loss_pct: float = -0.30,
    short_term_filter: bool = True,
    rebalance_freq: str = "weekly",
    warmup_days: int = 260,
    plot: bool = False,
    board_lot: Optional[int] = None,
    pyramid: bool = False,
    confirmation_weeks: int = 0,
    dynamic_stop: bool = True,
    stock_type: Optional[str] = None,
    z_buy: float = -1.5,
    z_sell: float = 1.5,
) -> Dict[str, Any]:
    """
    运行单标的回测，返回绩效指标字典。

    参数均有合理默认值，与 MultifactorRiskStrategy 对应。
    board_lot=None 时自动从持仓 CSV 检测每手股数。
    """
    print(f"\n{'='*60}")
    print(f"  回测: {ticker} | {strategy_name} | {start_date} ~ {end_date or '最新'}")
    print(f"{'='*60}")

    # ---- 构建配置 ----
    strategy_params = {
        "buy_threshold": buy_threshold,
        "sell_threshold": sell_threshold,
        "stop_loss_pct": stop_loss_pct,
        "short_term_filter": short_term_filter,
        "short_term_buy_max": 0.80,
        "z_buy_threshold": z_buy,
        "z_sell_threshold": z_sell,
        "risk_free_rate": 0.04,
    }

    # 自动检测股票类型
    resolved_stock_type = stock_type or DynamicStopLoss.get_stock_type(ticker)

    config = BacktestConfig(
        ticker=ticker,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        fixed_fraction=fixed_fraction,
        max_tranches=max_tranches,
        rebalance_freq=rebalance_freq,
        warmup_days=warmup_days,
        position_sizing_mode="pyramid" if pyramid else "equal",
        signal_confirmation_periods=confirmation_weeks,
        use_dynamic_stop=dynamic_stop,
        stock_type=resolved_stock_type,
    )

    # ---- 自动检测 Board Lot ----
    if board_lot is None:
        board_lot = load_board_lot(ticker, config.portfolio_dir)
    print(f"  Board Lot: {board_lot} 股/手")

    # ---- 加载数据 ----
    print(f"\n[1/5] 加载数据...")
    df_ohlcv = load_ohlcv(ticker, config.ohlcv_dir)
    if df_ohlcv is None:
        print(f"  ❌ 找不到 {ticker} 的 OHLCV 数据，终止回测")
        return {}

    eps_series, bvps_series = load_financials(ticker, config.financials_dir)

    # 基准指数（用于 alpha 计算）
    df_bench = load_index_ohlcv(config.benchmark, config.ohlcv_dir)

    # 指数数据（用于 SignalEngine 内的市场相关性，如有需要）
    if _load_index_data is not None:
        index_data = _load_index_data()
    else:
        index_data = {}

    # ---- 初始化信号引擎 ----
    print(f"\n[2/5] 初始化信号引擎...")
    engine = SignalEngine(df_ohlcv, eps_series, bvps_series, index_data)

    # ---- 创建策略 ----
    print(f"\n[3/5] 创建策略: {strategy_name}")
    strategy = create_strategy(strategy_name, config.strategy_params)

    # 信号确认过滤器
    if config.signal_confirmation_periods > 0:
        strategy = SignalConfirmationFilter(
            strategy, confirmation_periods=config.signal_confirmation_periods
        )
        print(f"  信号确认: 需连续 {config.signal_confirmation_periods} 个决策日")

    # ---- 运行模拟 ----
    print(f"\n[4/5] 运行 Walk-Forward 模拟...")
    sim = Simulator(config, engine, strategy, df_ohlcv, board_lot=board_lot)
    sim_results = sim.run()

    equity_df = sim_results["equity_curve"]
    trades = sim_results["trades"]

    if equity_df is None or equity_df.empty:
        print("  ❌ 权益曲线为空，回测可能没有触发任何交易")
        # 仍然生成空报告
        return {"error": "no trades", "ticker": ticker}

    # ---- 计算绩效 ----
    print(f"\n[5/5] 计算绩效指标...")
    metrics = calculate_performance(
        equity_df, trades, df_bench, config.risk_free_rate
    )

    # ---- 生成报告 ----
    out_dir = generate_report(config, metrics, equity_df, trades, plot=plot)

    # ---- 打印摘要 ----
    _print_summary(ticker, strategy_name, metrics)

    return metrics


def _print_summary(ticker: str, strategy: str, metrics: Dict[str, Any]):
    """在终端打印简洁的绩效摘要表格。"""
    print(f"\n{'─'*50}")
    print(f"  {ticker} | {strategy} 回测摘要")
    print(f"{'─'*50}")
    rows = [
        ("区间", f"{metrics.get('start_date')} ~ {metrics.get('end_date')}"),
        ("总收益", f"{metrics.get('total_return_pct', 'N/A')}%"),
        ("年化收益", f"{metrics.get('annualized_return_pct', 'N/A')}%"),
        ("年化波动率", f"{metrics.get('annualized_volatility_pct', 'N/A')}%"),
        ("夏普比率", str(metrics.get('sharpe_ratio', 'N/A'))),
        ("最大回撤", f"{metrics.get('max_drawdown_pct', 'N/A')}%"),
        ("最大回撤天数", str(metrics.get('max_drawdown_duration_days', 'N/A'))),
        ("Calmar比率", str(metrics.get('calmar_ratio', 'N/A'))),
        ("总交易次数", str(metrics.get('total_trades', 0))),
        ("胜率", f"{metrics.get('win_rate_pct', 'N/A')}%"),
        ("盈亏比(Profit Factor)", str(metrics.get('profit_factor', 'N/A'))),
        ("平均持仓天数", str(metrics.get('avg_holding_days', 'N/A'))),
        ("基准年化收益", f"{metrics.get('benchmark_annualized_return_pct', 'N/A')}%"),
        ("超额收益(Alpha)", f"{metrics.get('alpha_pct', 'N/A')}%"),
    ]
    for label, value in rows:
        print(f"  {label:<20} {value}")
    print(f"{'─'*50}\n")


# ============================================================
# CLI 解析
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="HK 股票多因子风险回测引擎",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m backtesting.run_backtest --ticker 0700.HK
  python -m backtesting.run_backtest --ticker 0700.HK --start 2018-01-01 --plot
  python -m backtesting.run_backtest --ticker 0700.HK --strategy composite --buy-threshold 0.08
  python -m backtesting.run_backtest --all
        """
    )

    parser.add_argument("--ticker", type=str, help="股票代码，如 0700.HK")
    parser.add_argument("--all", action="store_true", help="对所有可用股票运行回测")
    parser.add_argument("--strategy", type=str, default="multifactor_risk",
                        choices=["multifactor_risk", "technical_momentum", "composite",
                                 "custom", "valuation_reversion", "dual_momentum", "atr_trend"],
                        help="策略名称（默认: multifactor_risk）")
    parser.add_argument("--start", type=str, default="2015-01-01", help="回测起始日期")
    parser.add_argument("--end", type=str, default=None, help="回测结束日期（默认：最新）")
    parser.add_argument("--capital", type=float, default=1_000_000.0, help="初始资金 HKD")
    parser.add_argument("--fraction", type=float, default=0.25, help="该股最大总仓位比例（占初始资金）")
    parser.add_argument("--max-tranches", type=int, default=3,
                        help="最大分批建仓次数（默认 3，每批 = fraction/max_tranches）")
    parser.add_argument("--buy-threshold", type=float, default=0.05,
                        help="多因子买入阈值（默认 0.05，机会区）")
    parser.add_argument("--sell-threshold", type=float, default=0.95,
                        help="多因子卖出阈值（默认 0.95，风险区）")
    parser.add_argument("--stop-loss", type=float, default=-0.30,
                        help="止损比例（负数，默认 -0.30 = -30%%）")
    parser.add_argument("--freq", type=str, default="weekly",
                        choices=["daily", "weekly", "monthly"],
                        help="调仓频率（默认: weekly）")
    parser.add_argument("--warmup", type=int, default=260,
                        help="信号热身天数（默认 260 ≈ 1年）")
    parser.add_argument("--board-lot", type=int, default=None,
                        help="每手股数（默认自动检测，从持仓CSV或内置表）")
    parser.add_argument("--pyramid", action="store_true",
                        help="启用金字塔建仓（首批最小，逐批递增，自动适配）")
    parser.add_argument("--confirmation-weeks", type=int, default=0,
                        help="信号确认周数（默认 0=禁用，3=需连续3周信号才建仓）")
    parser.add_argument("--dynamic-stop", action="store_true", default=True,
                        help="启用ATR动态止损（默认启用）")
    parser.add_argument("--no-dynamic-stop", dest="dynamic_stop", action="store_false",
                        help="禁用动态止损，使用固定止损")
    parser.add_argument("--stock-type", type=str, default=None,
                        choices=["blue_chip", "growth", "high_volatility"],
                        help="股票类型（影响止损宽度，默认自动分类）")
    parser.add_argument("--z-buy", type=float, default=-1.5,
                        help="估值回归策略：买入Z-score阈值（默认 -1.5）")
    parser.add_argument("--z-sell", type=float, default=1.5,
                        help="估值回归策略：卖出Z-score阈值（默认 1.5）")
    parser.add_argument("--plot", action="store_true", help="生成可视化图表")

    args = parser.parse_args()

    if args.all:
        # 批量运行所有可用股票
        tickers = list_available_tickers(OHLCV_DIR)
        print(f"发现 {len(tickers)} 只股票: {tickers}")
        all_metrics = {}
        for t in tickers:
            try:
                m = run_backtest(
                    ticker=t,
                    strategy_name=args.strategy,
                    start_date=args.start,
                    end_date=args.end,
                    initial_capital=args.capital,
                    fixed_fraction=args.fraction,
                    max_tranches=args.max_tranches,
                    buy_threshold=args.buy_threshold,
                    sell_threshold=args.sell_threshold,
                    stop_loss_pct=args.stop_loss,
                    rebalance_freq=args.freq,
                    warmup_days=args.warmup,
                    plot=args.plot,
                    board_lot=args.board_lot,
                    pyramid=args.pyramid,
                    confirmation_weeks=args.confirmation_weeks,
                    dynamic_stop=args.dynamic_stop,
                    stock_type=args.stock_type,
                    z_buy=args.z_buy,
                    z_sell=args.z_sell,
                )
                all_metrics[t] = {
                    "annualized_return_pct": m.get("annualized_return_pct"),
                    "sharpe_ratio": m.get("sharpe_ratio"),
                    "max_drawdown_pct": m.get("max_drawdown_pct"),
                    "alpha_pct": m.get("alpha_pct"),
                }
            except Exception as e:
                print(f"  ❌ {t} 回测失败: {e}")
                all_metrics[t] = {"error": str(e)}

        print("\n" + "="*60)
        print("全部股票汇总：")
        print(json.dumps(all_metrics, ensure_ascii=False, indent=2))

    elif args.ticker:
        run_backtest(
            ticker=args.ticker,
            strategy_name=args.strategy,
            start_date=args.start,
            end_date=args.end,
            initial_capital=args.capital,
            fixed_fraction=args.fraction,
            max_tranches=args.max_tranches,
            buy_threshold=args.buy_threshold,
            sell_threshold=args.sell_threshold,
            stop_loss_pct=args.stop_loss,
            rebalance_freq=args.freq,
            warmup_days=args.warmup,
            plot=args.plot,
            board_lot=args.board_lot,
            pyramid=args.pyramid,
            confirmation_weeks=args.confirmation_weeks,
            dynamic_stop=args.dynamic_stop,
            stock_type=args.stock_type,
            z_buy=args.z_buy,
            z_sell=args.z_sell,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
