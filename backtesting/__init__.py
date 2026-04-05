"""
backtesting — HK股票多因子风险回测引擎

复用现有 processors/ 中的所有计算函数，无需修改。
完全离线运行，不依赖 IBKR 连接。

快速开始:
    python -m backtesting.run_backtest --ticker 0700.HK
    python -m backtesting.run_backtest --ticker 0700.HK --start 2018-01-01 --plot
    python -m backtesting.run_backtest --all
"""

from backtesting.run_backtest import run_backtest

__all__ = ["run_backtest"]
