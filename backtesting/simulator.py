"""
simulator.py — Walk-forward 模拟循环（分批建仓版）

工作流程：
    1. 生成决策日序列（依 rebalance_freq/rebalance_day 过滤交易日）
    2. 跳过热身期（warmup_days 之前不交易）
    3. 每个决策日（循环 evaluate 直到 HOLD）：
        a. 调用 SignalEngine.compute_at(date) 获取信号
        b. 调用 Strategy.evaluate(signal, tranches, max_tranches) 获取交易指令
        c. SELL_TRANCHE：平掉指定批次（止损，不影响其他批次）
           SELL：平掉全部批次（信号卖出）
           BUY：新增一个批次
        d. 记录权益曲线
    4. 返回 {equity_curve, trades} 供 performance.py 计算指标

分批建仓说明:
    - 每批仓位大小 = initial_capital * fixed_fraction / max_tranches
    - 止损 (SELL_TRANCHE) 只平该批，其余批次继续持有
    - 信号卖出 (SELL) 清空全部批次
    - 每个决策日至多执行一次 BUY（防止同日多次建仓）
"""

from __future__ import annotations
import sys
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

from backtesting.config_bt import BacktestConfig
from backtesting.signal_engine import SignalEngine, SignalSnapshot
from backtesting.strategy import BaseStrategy, Action, TranchInfo


@dataclass
class Position:
    """单批持仓状态。"""
    tranche_id: int
    ticker: str
    entry_date: pd.Timestamp
    entry_price: float
    shares: int
    cost_basis: float           # 含佣金总成本（HKD）

    def market_value(self, price: float) -> float:
        return self.shares * price

    def unrealized_pnl(self, price: float) -> float:
        return self.market_value(price) - self.cost_basis

    def unrealized_pnl_pct(self, price: float) -> float:
        if self.cost_basis <= 0:
            return 0.0
        return self.unrealized_pnl(price) / self.cost_basis


@dataclass
class Trade:
    """单笔成交记录。"""
    ticker: str
    action: str                 # "buy" | "sell"
    date: pd.Timestamp
    price: float
    shares: int
    commission: float           # 佣金+印花税（HKD）
    proceeds: float             # 实际收到/付出的现金（扣费后）
    tranche_id: int = 0         # 对应的批次编号（用于买卖配对）
    pnl: float = 0.0           # 仅卖出时有意义
    pnl_pct: float = 0.0
    reason: str = ""
    long_term_risk: Optional[float] = None
    short_term_risk: Optional[float] = None


class Simulator:
    """
    Walk-forward 回测模拟器（单标的，支持分批建仓）。

    用法:
        sim = Simulator(config, signal_engine, strategy, df_ohlcv)
        results = sim.run()
        # results: {"equity_curve": DataFrame, "trades": List[Trade]}
    """

    def __init__(
        self,
        config: BacktestConfig,
        signal_engine: SignalEngine,
        strategy: BaseStrategy,
        df_ohlcv: pd.DataFrame,
        board_lot: int = 100,
    ):
        self.cfg = config
        self.engine = signal_engine
        self.strategy = strategy
        self.df = df_ohlcv
        self.board_lot = board_lot

        # 账户状态
        self.cash: float = config.initial_capital
        self.positions: List[Position] = []     # 当前所有持仓批次
        self._next_tranche_id: int = 1          # 批次 ID 计数器（从 1 开始，便于阅读）
        self.trades: List[Trade] = []
        self.equity_records: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    def run(self) -> dict:
        decision_dates = self._get_decision_dates()
        total = len(decision_dates)
        print(f"  [Simulator] 决策日共 {total} 个 "
              f"({decision_dates[0].date()} ~ {decision_dates[-1].date()})")

        for i, date in enumerate(decision_dates):
            if (i + 1) % 100 == 0 or i == total - 1:
                print(f"  [Simulator] 进度 {i+1}/{total} ({date.date()})", end="\r")

            # 获取当日收盘价
            row = self.df.loc[date] if date in self.df.index else None
            if row is None:
                self._record_equity(date, self._last_close())
                continue

            close = float(row['Close'])

            # 计算信号
            signal = self.engine.compute_at(date)

            # ---- 策略评估循环（处理多批次止损 + 加仓/清仓）----
            bought_today = False
            for _iter in range(self.cfg.max_tranches + 5):   # 防止无限循环
                tranches = [
                    TranchInfo(
                        tranche_id=p.tranche_id,
                        entry_date=p.entry_date,
                        entry_price=p.entry_price,
                        pnl_pct=p.unrealized_pnl_pct(close),
                    )
                    for p in self.positions
                ]
                trade_signal = self.strategy.evaluate(
                    signal, tranches, self.cfg.max_tranches
                )

                if trade_signal.action == Action.HOLD:
                    break

                elif trade_signal.action == Action.SELL_TRANCHE:
                    # 只平掉指定批次
                    target = next(
                        (p for p in self.positions if p.tranche_id == trade_signal.tranche_id),
                        None,
                    )
                    if target:
                        self._execute_sell_tranche(date, close, target, trade_signal, signal)
                    else:
                        break  # 找不到批次，终止

                elif trade_signal.action == Action.SELL:
                    # 平掉所有批次
                    for pos in list(self.positions):
                        self._execute_sell_tranche(date, close, pos, trade_signal, signal)
                    break

                elif trade_signal.action == Action.BUY:
                    if not bought_today:
                        self._execute_buy(date, close, trade_signal, signal)
                        bought_today = True
                    break   # 每日至多一次买入

            # 记录权益
            self._record_equity(date, close)

        print()  # 换行
        print(f"  [Simulator] 完成，共成交 {len(self.trades)} 笔")

        equity_df = pd.DataFrame(self.equity_records)
        if not equity_df.empty:
            equity_df['date'] = pd.to_datetime(equity_df['date'])
            equity_df.set_index('date', inplace=True)

        return {"equity_curve": equity_df, "trades": self.trades}

    # ------------------------------------------------------------------
    # 交易执行
    # ------------------------------------------------------------------

    def _execute_buy(
        self, date: pd.Timestamp, price: float,
        trade_signal, signal: SignalSnapshot,
    ):
        tranche_id = self._next_tranche_id
        shares = self._calc_shares(price, trade_signal.size_hint)
        if shares <= 0:
            return

        trade_value = shares * price
        commission = self._calc_commission(trade_value)
        total_cost = trade_value + commission

        if total_cost > self.cash:
            # 资金不足，重新计算可买手数
            affordable = self.cash - commission
            shares = int(affordable / price / self.board_lot) * self.board_lot
            if shares <= 0:
                return
            trade_value = shares * price
            commission = self._calc_commission(trade_value)
            total_cost = trade_value + commission

        self.cash -= total_cost
        self.positions.append(Position(
            tranche_id=tranche_id,
            ticker=self.cfg.ticker,
            entry_date=date,
            entry_price=price,
            shares=shares,
            cost_basis=total_cost,
        ))
        self._next_tranche_id += 1

        self.trades.append(Trade(
            ticker=self.cfg.ticker,
            action="buy",
            date=date,
            price=price,
            shares=shares,
            commission=commission,
            proceeds=-total_cost,
            tranche_id=tranche_id,
            reason=trade_signal.reason,
            long_term_risk=signal.long_term_risk,
            short_term_risk=signal.short_term_risk,
        ))

    def _execute_sell_tranche(
        self, date: pd.Timestamp, price: float,
        pos: Position, trade_signal, signal: SignalSnapshot,
    ):
        shares = pos.shares
        trade_value = shares * price
        commission = self._calc_commission(trade_value)
        net_proceeds = trade_value - commission

        pnl = net_proceeds - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis if pos.cost_basis > 0 else 0.0

        self.cash += net_proceeds
        self.positions.remove(pos)

        self.trades.append(Trade(
            ticker=self.cfg.ticker,
            action="sell",
            date=date,
            price=price,
            shares=shares,
            commission=commission,
            proceeds=net_proceeds,
            tranche_id=pos.tranche_id,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=trade_signal.reason,
            long_term_risk=signal.long_term_risk,
            short_term_risk=signal.short_term_risk,
        ))

    # ------------------------------------------------------------------
    # 辅助计算
    # ------------------------------------------------------------------

    def _calc_shares(self, price: float, size_hint: float = 1.0) -> int:
        """
        每批仓位大小 = initial_capital * fixed_fraction / max_tranches。
        使用初始资金（而非当前现金）作为基准，保持每批仓位大小一致。
        """
        cfg = self.cfg
        if cfg.position_sizing == "fixed_fraction":
            per_tranche = cfg.initial_capital * cfg.fixed_fraction / cfg.max_tranches
        else:  # all_in
            per_tranche = cfg.initial_capital * cfg.max_position_pct / cfg.max_tranches

        target_value = per_tranche * size_hint
        shares = int(target_value / price / self.board_lot) * self.board_lot
        return max(shares, 0)

    def _calc_commission(self, trade_value: float) -> float:
        """计算交易成本：佣金 + 印花税，最低保证金。"""
        cfg = self.cfg
        cost = trade_value * (cfg.commission_rate + cfg.stamp_duty)
        return max(cost, cfg.min_commission)

    def _record_equity(self, date: pd.Timestamp, close: float):
        pos_value = sum(p.market_value(close) for p in self.positions)
        equity = self.cash + pos_value
        self.equity_records.append({
            "date": date,
            "equity": equity,
            "cash": self.cash,
            "position_value": pos_value,
            "active_tranches": len(self.positions),
        })

    def _last_close(self) -> float:
        if self.equity_records:
            return self.equity_records[-1].get("equity", self.cash)
        return self.cash

    # ------------------------------------------------------------------
    # 决策日序列生成
    # ------------------------------------------------------------------

    def _get_decision_dates(self) -> List[pd.Timestamp]:
        """
        生成有效决策日列表：
            1. 只包含 OHLCV 数据中存在的交易日
            2. 过滤到指定频率（daily/weekly/monthly）
            3. 跳过热身期（前 warmup_days 个交易日）
            4. 限制在 start_date ~ end_date 之间
        """
        cfg = self.cfg
        all_trading_days = self.df.index

        # 日期范围过滤
        start = pd.Timestamp(cfg.start_date)
        end = pd.Timestamp(cfg.end_date) if cfg.end_date else all_trading_days[-1]
        mask = (all_trading_days >= start) & (all_trading_days <= end)
        trading_days = all_trading_days[mask]

        if len(trading_days) == 0:
            raise ValueError(f"在 {cfg.start_date} ~ {cfg.end_date} 范围内无交易数据")

        # 热身期：跳过前 warmup_days 个交易日
        warmup_cutoff = (
            all_trading_days[cfg.warmup_days - 1]
            if len(all_trading_days) > cfg.warmup_days
            else all_trading_days[0]
        )
        trading_days = trading_days[trading_days >= warmup_cutoff]

        if len(trading_days) == 0:
            raise ValueError("热身期后无有效交易日，请缩短 warmup_days 或延长数据范围")

        # 频率过滤
        freq = cfg.rebalance_freq
        if freq == "daily":
            return list(trading_days)
        elif freq == "weekly":
            return self._filter_weekly(trading_days, cfg.rebalance_day)
        elif freq == "monthly":
            return self._filter_monthly(trading_days)
        else:
            raise ValueError(f"未知调仓频率: {freq}")

    @staticmethod
    def _filter_weekly(
        trading_days: pd.DatetimeIndex, rebalance_day: int
    ) -> List[pd.Timestamp]:
        """
        每周选一天决策：优先选指定星期几（rebalance_day），
        若当周该天是非交易日则选当周最后一个交易日。
        """
        result = []
        seen_weeks = set()
        for date in trading_days:
            week = date.to_period('W')
            if week in seen_weeks:
                continue
            week_days = trading_days[trading_days.to_period('W') == week]
            target_days = [d for d in week_days if d.dayofweek == rebalance_day]
            if target_days:
                result.append(target_days[0])
            else:
                result.append(week_days[-1])
            seen_weeks.add(week)
        return result

    @staticmethod
    def _filter_monthly(trading_days: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """每月选最后一个交易日。"""
        result = []
        seen_months = set()
        for date in reversed(list(trading_days)):
            month = date.to_period('M')
            if month not in seen_months:
                result.append(date)
                seen_months.add(month)
        return sorted(result)
