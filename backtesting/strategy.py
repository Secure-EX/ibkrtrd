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
# 策略 5：估值均值回归策略（保守型）
# ============================================================

class ValuationMeanReversionStrategy(BaseStrategy):
    """
    历史估值均值回归策略，适合蓝筹股长线波段。

    核心逻辑：
        好公司的基本面长期向上，但市场情绪导致估值在"高估"和"低估"之间摆动。
        利用 PE/PB 的 Z-score 判断当前估值偏离历史均值的程度。

    买入：avg Z-score < z_buy_threshold（默认 -1.5，历史极低区间）
    卖出：avg Z-score > z_sell_threshold（默认 1.5，历史极高区间）
    止损：由 Simulator 层的动态止损管理

    最佳适用：腾讯、中海油、中升集团等有长期财报数据的蓝筹股。
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        p = self.params
        z_buy = p.get("z_buy_threshold", -1.5)
        z_sell = p.get("z_sell_threshold", 1.5)
        stop_loss = p.get("stop_loss_pct", -0.30)
        has_position = len(tranches) > 0

        # 固定止损回退（use_dynamic_stop=False 时生效）
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        # 收集可用的 Z-score
        z_scores = [z for z in [signal.pe_zscore, signal.pb_zscore]
                     if z is not None]
        if not z_scores:
            return TradeSignal(Action.HOLD, reason="估值数据不足（无EPS/BVPS）")
        avg_z = sum(z_scores) / len(z_scores)

        # 卖出：估值过高
        if has_position and avg_z > z_sell:
            return TradeSignal(
                Action.SELL,
                reason=f"估值Z-score过高 {avg_z:.2f} > {z_sell}",
            )

        # 买入：估值极低
        if len(tranches) < max_tranches and avg_z < z_buy:
            # 短线超买过滤
            if signal.rsi_zone == "overbought":
                return TradeSignal(
                    Action.HOLD,
                    reason=f"Z={avg_z:.2f}但RSI超买，等待回调",
                )
            return TradeSignal(
                Action.BUY,
                reason=f"估值均值回归 Z={avg_z:.2f} < {z_buy} "
                       f"[{len(tranches)}/{max_tranches}批]",
            )

        return TradeSignal(Action.HOLD, reason=f"估值Z={avg_z:.2f}，无信号")


# ============================================================
# 策略 6：双动量策略（中性型）
# ============================================================

class DualMomentumStrategy(BaseStrategy):
    """
    双动量策略：绝对动量过滤 + 趋势跟踪。

    核心逻辑：
        - 绝对动量：过去12个月回报 > 无风险利率 → 持有/买入
        - 绝对动量转负 → 果断卖出，切换为现金
        - 避免在大熊市初期死扛，实现长期稳健复利

    买入：return_12m > risk_free_rate（默认 4%）
    卖出：return_12m < 0（绝对动量转负）
    推荐频率：月频调仓

    适用场景：全部股票，特别适合避免长期阴跌。
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        p = self.params
        risk_free = p.get("risk_free_rate", 0.04)
        exit_threshold = p.get("exit_threshold", 0.0)
        stop_loss = p.get("stop_loss_pct", -0.30)
        has_position = len(tranches) > 0

        # 固定止损回退
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        if signal.return_12m is None:
            return TradeSignal(Action.HOLD, reason="动量数据不足（不足252日）")

        abs_mom = signal.return_12m

        # 卖出：绝对动量转负
        if has_position and abs_mom < exit_threshold:
            return TradeSignal(
                Action.SELL,
                reason=f"绝对动量转负 {abs_mom:.2%} < {exit_threshold:.2%}",
            )

        # 买入：绝对动量正向且超过无风险利率
        if len(tranches) < max_tranches and abs_mom > risk_free:
            return TradeSignal(
                Action.BUY,
                reason=f"绝对动量正向 {abs_mom:.2%} > {risk_free:.2%} "
                       f"[{len(tranches)}/{max_tranches}批]",
            )

        return TradeSignal(
            Action.HOLD,
            reason=f"12M回报={abs_mom:.2%}，介于退出与买入阈值之间",
        )


# ============================================================
# 策略 7：ATR 趋势跟踪策略（激进型）
# ============================================================

class ATRTrendFollowingStrategy(BaseStrategy):
    """
    自适应趋势跟踪策略，适合高波动成长股。

    核心逻辑：
        - 使用 KAMA（Kaufman Adaptive Moving Average）判断趋势方向
        - KAMA 在趋势明确时快速跟随，在震荡时自动平滑
        - 入场需要价格突破 KAMA + 成交量放大确认
        - 出场由 KAMA 趋势反转 + Simulator 层动态止损协同管理

    买入：price > KAMA AND KAMA 趋势向上 AND 成交量放大
    卖出：price < KAMA AND KAMA 趋势向下
    止损：ATR 移动止损（Simulator 层管理）

    最佳适用：泡泡玛特、理想汽车、心动公司等高波动标的。
    """

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        p = self.params
        stop_loss = p.get("stop_loss_pct", -0.25)
        require_volume = p.get("require_volume_breakout", True)
        has_position = len(tranches) > 0

        # 固定止损回退
        for t in tranches:
            if t.pnl_pct < stop_loss:
                return TradeSignal(
                    Action.SELL_TRANCHE,
                    tranche_id=t.tranche_id,
                    reason=f"止损 {t.pnl_pct:.1%} [批次#{t.tranche_id}]",
                )

        if signal.kama_value is None:
            return TradeSignal(Action.HOLD, reason="KAMA数据不足")

        price_above_kama = signal.close > signal.kama_value
        kama_up = signal.kama_direction == "up"
        kama_down = signal.kama_direction == "down"
        vol_confirm = bool(signal.volume_breakout)

        # 卖出：价格跌破 KAMA + KAMA 趋势向下
        if has_position and not price_above_kama and kama_down:
            return TradeSignal(
                Action.SELL,
                reason=(f"趋势反转: 价格{signal.close:.2f} < "
                        f"KAMA{signal.kama_value:.2f}, 趋势向下"),
            )

        # 买入逻辑
        if len(tranches) < max_tranches:
            if price_above_kama and kama_up:
                if vol_confirm or not require_volume:
                    vol_str = "+放量" if vol_confirm else ""
                    return TradeSignal(
                        Action.BUY,
                        reason=(f"KAMA趋势突破{vol_str}: "
                                f"价格{signal.close:.2f} > "
                                f"KAMA{signal.kama_value:.2f} "
                                f"[{len(tranches)}/{max_tranches}批]"),
                    )
                elif has_position:
                    # 已有持仓时，KAMA 趋势延续可加仓（不需要放量确认）
                    return TradeSignal(
                        Action.BUY,
                        size_hint=0.5,
                        reason=f"KAMA趋势延续加仓 [{len(tranches)}/{max_tranches}批]",
                    )

        return TradeSignal(Action.HOLD, reason="无趋势信号")


# ============================================================
# 信号确认过滤器（装饰器模式）
# ============================================================

class SignalConfirmationFilter(BaseStrategy):
    """
    信号确认过滤器：要求买入信号连续出现 N 个决策日才允许首次建仓。

    设计逻辑：
        - 仅对第一批建仓（空仓 → 建仓）生效
        - 已有持仓后的加仓不需要重新确认
        - 连续 N 次 evaluate 返回 BUY 后才放行
        - 出现非 BUY 信号时重置计数器

    用法：
        base = MultifactorRiskStrategy(params)
        confirmed = SignalConfirmationFilter(base, confirmation_periods=3)
        # 以 confirmed 作为策略传入 Simulator
    """

    def __init__(self, inner_strategy: BaseStrategy, confirmation_periods: int = 3):
        super().__init__(inner_strategy.params)
        self.inner = inner_strategy
        self.required = confirmation_periods
        self._consecutive_buy_count = 0

    def evaluate(
        self,
        signal: SignalSnapshot,
        tranches: List[TranchInfo],
        max_tranches: int = 1,
    ) -> TradeSignal:
        result = self.inner.evaluate(signal, tranches, max_tranches)

        if result.action == Action.BUY and len(tranches) == 0:
            # 仅对首批建仓（观察仓位）施加确认期
            self._consecutive_buy_count += 1
            if self._consecutive_buy_count < self.required:
                return TradeSignal(
                    Action.HOLD,
                    reason=(f"信号确认中 {self._consecutive_buy_count}/{self.required} "
                            f"({result.reason})"),
                )
            # 确认通过，放行买入
            return result
        elif result.action == Action.BUY:
            # 已有持仓的加仓，不需要确认
            return result
        else:
            # 非 BUY 信号，重置计数器
            if result.action != Action.HOLD:
                self._consecutive_buy_count = 0
            elif "无信号" in result.reason:
                self._consecutive_buy_count = 0
            return result


# ============================================================
# 策略注册表与工厂函数
# ============================================================

STRATEGY_REGISTRY = {
    "multifactor_risk": MultifactorRiskStrategy,
    "technical_momentum": TechnicalMomentumStrategy,
    "composite": CompositeStrategy,
    "custom": CustomStrategy,
    "valuation_reversion": ValuationMeanReversionStrategy,
    "dual_momentum": DualMomentumStrategy,
    "atr_trend": ATRTrendFollowingStrategy,
}


def create_strategy(name: str, params: dict) -> BaseStrategy:
    """工厂函数：按名称创建策略实例。"""
    cls = STRATEGY_REGISTRY.get(name)
    if cls is None:
        available = list(STRATEGY_REGISTRY.keys())
        raise ValueError(f"未知策略 '{name}'，可用策略: {available}")
    return cls(params)
