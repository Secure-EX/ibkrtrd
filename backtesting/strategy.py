"""
strategy.py — 策略定义模块

提供策略基类与若干内置策略实现。
每个策略接收 SignalSnapshot + 分批持仓状态，返回 TradeSignal。

内置策略:
    MultifactorRiskStrategy  — 主策略：基于多因子长线风险阈值，支持分批建仓
    TechnicalMomentumStrategy — MA金叉 + RSI 确认
    CompositeStrategy        — 多因子 + 技术双重确认
    CustomStrategy           — 字典规则驱动，无需编写代码

分批建仓逻辑:
    - BUY: 添加一批新仓位（当前批次 < max_tranches 时）
    - SELL_TRANCHE: 仅平掉某一批（止损时使用，保留其他批次）
    - SELL: 清掉所有批次（信号卖出时使用）
    - HOLD: 不操作

策略注册表:
    STRATEGY_REGISTRY = {"multifactor_risk": ..., "technical_momentum": ..., ...}
"""

from __future__ import annotations
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

from backtesting.signal_engine import SignalSnapshot


class Action(Enum):
    BUY = "buy"
    SELL = "sell"                   # 清掉全部批次
    SELL_TRANCHE = "sell_tranche"   # 仅平一批（止损专用）
    HOLD = "hold"


@dataclass
class TranchInfo:
    """单批持仓信息，传递给策略用于决策。"""
    tranche_id: int
    entry_date: pd.Timestamp
    entry_price: float
    pnl_pct: float                  # 该批当前浮动盈亏比例


@dataclass
class TradeSignal:
    action: Action
    size_hint: float = 1.0          # 0~1，相对单批仓位大小
    reason: str = ""
    tranche_id: Optional[int] = None  # SELL_TRANCHE 时指定哪一批


# ============================================================
# 基类
# ============================================================

class BaseStrategy(ABC):
    """所有策略的基类。"""

    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        """
        参数:
            signal       : 当前决策日的信号快照
            tranches     : 当前所有持仓批次（空列表 = 空仓）
            max_tranches : 允许的最大批次数

        返回:
            TradeSignal（每次调用返回单个最高优先级指令）
        """


# ============================================================
# 策略 1：多因子风险策略（默认/推荐策略）
# ============================================================

class MultifactorRiskStrategy(BaseStrategy):
    """
    基于多因子综合风险评分的主要策略，支持分批建仓。

    优先级（高→低）:
        1. 逐批检查止损：pnl_pct < stop_loss_pct → SELL_TRANCHE（只平这一批）
        2. 信号卖出：long_term_risk > sell_threshold → SELL（清仓所有批次）
        3. 加仓：批次数 < max_tranches AND long_term_risk < buy_threshold → BUY
        4. 无操作：HOLD
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        if not signal.is_valid():
            return TradeSignal(Action.HOLD, reason="信号数据不足")

        p = self.params
        buy_thr = p.get("buy_threshold", 0.05)
        sell_thr = p.get("sell_threshold", 0.95)
        stop_loss = p.get("stop_loss_pct", -0.30)
        use_st_filter = p.get("short_term_filter", True)
        st_max = p.get("short_term_buy_max", 0.80)

        lr = signal.long_term_risk
        sr = signal.short_term_risk
        has_position = len(tranches) > 0

        # ---- 优先级1：逐批止损（不影响其他批次）----
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损触发 {t.pnl_pct:.1%} < {stop_loss:.1%} [批次#{t.tranche_id}]",
                )

        # ---- 优先级2：长线风险过高，清仓所有批次 ----
        if has_position and lr is not None and lr > sell_thr:
            return TradeSignal(
                Action.SELL,
                reason=f"长线风险过高 {lr:.3f} > {sell_thr}",
            )

        # ---- 优先级3：加仓（未满仓且在机会区）----
        if len(tranches) < max_tranches:
            if lr is not None and lr < buy_thr:
                if use_st_filter and sr is not None and sr > st_max:
                    return TradeSignal(
                        Action.HOLD,
                        reason=f"短线过热过滤 sr={sr:.3f} > {st_max}",
                    )
                return TradeSignal(
                    Action.BUY,
                    reason=f"长线机会区 {lr:.3f} < {buy_thr} [{len(tranches)}/{max_tranches}批]",
                )

        return TradeSignal(Action.HOLD, reason="无信号")


# ============================================================
# 策略 2：技术动量策略
# ============================================================

class TechnicalMomentumStrategy(BaseStrategy):
    """
    MA 金叉 + RSI 确认。支持分批建仓。

    买入：MA 多头排列（bullish）+ RSI 不处于超买区
    卖出：MA 空头排列（bearish）或 RSI 超买
    止损：逐批检查
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        if not signal.is_valid():
            return TradeSignal(Action.HOLD, reason="信号数据不足")

        stop_loss = self.params.get("stop_loss_pct", -0.20)
        has_position = len(tranches) > 0

        # 逐批止损
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        ma_bull = signal.ma_alignment == "bullish"
        ma_bear = signal.ma_alignment == "bearish"
        rsi_ob = signal.rsi_zone == "overbought"
        rsi_os = signal.rsi_zone == "oversold"
        above_250 = signal.above_ma250 is True

        if has_position:
            if ma_bear:
                return TradeSignal(Action.SELL, reason="均线空头排列")
            if rsi_ob:
                return TradeSignal(Action.SELL, reason="RSI超买清仓")

        if len(tranches) < max_tranches:
            if ma_bull and not rsi_ob and above_250:
                return TradeSignal(Action.BUY, reason="均线多头+RSI未超买")
            if rsi_os and above_250:
                return TradeSignal(Action.BUY, size_hint=0.5, reason="RSI超卖反弹")

        return TradeSignal(Action.HOLD, reason="无信号")


# ============================================================
# 策略 3：复合策略（多因子 + 技术双重确认）
# ============================================================

class CompositeStrategy(BaseStrategy):
    """
    多因子风险 + 技术信号双重确认。支持分批建仓。

    买入：long_term_risk < buy_threshold AND (MA多头 OR RSI超卖)
    卖出：long_term_risk > sell_threshold OR MA空头
    止损：逐批检查
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        if not signal.is_valid():
            return TradeSignal(Action.HOLD, reason="信号数据不足")

        p = self.params
        buy_thr = p.get("buy_threshold", 0.08)
        sell_thr = p.get("sell_threshold", 0.90)
        stop_loss = p.get("stop_loss_pct", -0.25)
        has_position = len(tranches) > 0

        # 逐批止损
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        lr = signal.long_term_risk
        ma_bull = signal.ma_alignment == "bullish"
        ma_bear = signal.ma_alignment == "bearish"
        rsi_os = signal.rsi_zone == "oversold"

        if has_position:
            if lr is not None and lr > sell_thr:
                return TradeSignal(Action.SELL, reason=f"多因子风险区 lr={lr:.3f}")
            if ma_bear:
                return TradeSignal(Action.SELL, reason="均线空头排列确认卖出")

        if len(tranches) < max_tranches:
            if lr is not None and lr < buy_thr:
                if ma_bull or rsi_os:
                    tech_reason = "均线多头" if ma_bull else "RSI超卖"
                    return TradeSignal(
                        Action.BUY,
                        reason=f"多因子+技术双确认：lr={lr:.3f},{tech_reason}",
                    )

        return TradeSignal(Action.HOLD, reason="无信号")


# ============================================================
# 策略 4：自定义规则策略
# ============================================================

class CustomStrategy(BaseStrategy):
    """
    字典规则驱动的自定义策略，无需编写代码。支持分批建仓。

    params 格式示例：
        {
            "buy_rules": [
                {"signal": "long_term_risk", "op": "<", "value": 0.10},
                {"signal": "rsi_zone", "op": "==", "value": "oversold"},
            ],
            "buy_logic": "any",   # "all" = AND，"any" = OR
            "sell_rules": [
                {"signal": "long_term_risk", "op": ">", "value": 0.85},
            ],
            "sell_logic": "any",
            "stop_loss_pct": -0.25,
        }
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        if not signal.is_valid():
            return TradeSignal(Action.HOLD, reason="信号数据不足")

        stop_loss = self.params.get("stop_loss_pct", -0.30)
        has_position = len(tranches) > 0

        # 逐批止损
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        if has_position:
            sell_rules = self.params.get("sell_rules", [])
            sell_logic = self.params.get("sell_logic", "any")
            if self._eval_rules(signal, sell_rules, sell_logic):
                return TradeSignal(Action.SELL, reason="自定义卖出规则触发")

        if len(tranches) < max_tranches:
            buy_rules = self.params.get("buy_rules", [])
            buy_logic = self.params.get("buy_logic", "all")
            if self._eval_rules(signal, buy_rules, buy_logic):
                return TradeSignal(Action.BUY, reason="自定义买入规则触发")

        return TradeSignal(Action.HOLD, reason="无信号")

    @staticmethod
    def _eval_rules(signal: SignalSnapshot, rules: list, logic: str) -> bool:
        if not rules:
            return False
        results = []
        for rule in rules:
            attr = rule.get("signal")
            op = rule.get("op")
            val = rule.get("value")
            sig_val = getattr(signal, attr, None)
            if sig_val is None:
                results.append(False)
                continue
            if op == "<":
                results.append(sig_val < val)
            elif op == "<=":
                results.append(sig_val <= val)
            elif op == ">":
                results.append(sig_val > val)
            elif op == ">=":
                results.append(sig_val >= val)
            elif op == "==":
                results.append(sig_val == val)
            elif op == "!=":
                results.append(sig_val != val)
            else:
                results.append(False)

        if logic == "all":
            return all(results)
        else:
            return any(results)


# ============================================================
# 策略注册表与工厂函数
# ============================================================

STRATEGY_REGISTRY = {
    "multifactor_risk": MultifactorRiskStrategy,
    "technical_momentum": TechnicalMomentumStrategy,
    "composite": CompositeStrategy,
    "custom": CustomStrategy,
}


def create_strategy(name: str, params: dict) -> BaseStrategy:
    """工厂函数：按名称创建策略实例。"""
    cls = STRATEGY_REGISTRY.get(name)
    if cls is None:
        available = list(STRATEGY_REGISTRY.keys())
        raise ValueError(f"未知策略 '{name}'，可用策略: {available}")
    return cls(params)
