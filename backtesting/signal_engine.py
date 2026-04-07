"""
signal_engine.py — Walk-forward 信号计算引擎

核心设计：
    1. 技术指标（MA/MACD/RSI/KDJ/BOLL/ATR）只计算一次（因果性，无前视偏差）
    2. 多因子风险评分在每个决策日做 walk-forward 切片重算（百分位排名需要历史上下文）
    3. 所有信号封装为 SignalSnapshot dataclass，供策略模块使用

复用函数（不修改原文件）：
    processors.technical_indicators._add_technical_indicators()
    processors.technical_indicators._calc_trend_signals()
    processors.technical_indicators._calc_price_percentile_rank()
    processors.technical_multifactor.calc_multifactor_risk()
    processors.technical_risk._assess_resonance()
"""

from __future__ import annotations
import sys
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

from processors.technical_indicators import (
    _add_technical_indicators,
    _calc_trend_signals,
    _calc_price_percentile_rank,
)
from processors.technical_multifactor import calc_multifactor_risk
from processors.technical_risk import _assess_resonance
from processors.technical_utils import _align_financial_to_daily, _get_dynamic_col


@dataclass
class SignalSnapshot:
    """某一决策日的完整信号快照。"""

    date: pd.Timestamp
    close: float

    # ---- 多因子风险（主要信号）----
    long_term_risk: Optional[float]     # 0~1，< 0.05 = 机会区，> 0.95 = 风险区
    long_term_zone: str
    short_term_risk: Optional[float]
    short_term_zone: str
    resonance: str                      # "bullish" | "bearish" | "divergent" | "neutral"

    # ---- 技术信号（辅助）----
    ma_alignment: Optional[str]         # "bullish" | "bearish" | "mixed"
    macd_cross: Optional[str]           # "golden_cross" | "death_cross" | "none"
    macd_above_zero: Optional[bool]
    rsi_zone: Optional[str]             # "overbought" | "oversold" | "neutral"
    rsi_value: Optional[float]
    kdj_zone: Optional[str]
    boll_position: Optional[str]
    above_ma20: Optional[bool]
    above_ma60: Optional[bool]
    above_ma250: Optional[bool]

    # ---- 价格百分位 ----
    price_pct_1y: Optional[float]       # 过去 252 日分位数
    price_pct_5y: Optional[float]       # 过去 1260 日分位数

    # ---- 各子因子明细（多因子长线）----
    factor_valuation: Optional[float] = None
    factor_momentum: Optional[float] = None
    factor_volatility: Optional[float] = None
    factor_technical: Optional[float] = None
    factor_capital_flow: Optional[float] = None

    # ---- 估值均值回归（Valuation Mean Reversion）----
    pe_zscore: Optional[float] = None       # PE Z-score（3年滚动窗口）
    pb_zscore: Optional[float] = None       # PB Z-score（3年滚动窗口）

    # ---- 双动量（Dual Momentum）----
    return_12m: Optional[float] = None      # 12个月绝对回报

    # ---- ATR 趋势跟踪（ATR Trend Following）----
    kama_value: Optional[float] = None      # Kaufman 自适应均线值
    kama_direction: Optional[str] = None    # "up" | "down" | "flat"
    volume_breakout: Optional[bool] = None  # 成交量 > 1.5 倍 20日均量
    atr_value: Optional[float] = None       # 当日 ATR(14)

    # ---- 原始指标值（供自定义策略访问）----
    raw: Dict[str, Any] = field(default_factory=dict)

    def is_valid(self) -> bool:
        """是否有足够数据生成有效信号。"""
        return self.long_term_risk is not None and self.close > 0


class SignalEngine:
    """
    Walk-forward 信号引擎。

    初始化时预计算技术指标（一次性），决策日调用 compute_at() 做
    walk-forward 切片，只重算需要百分位排名的多因子风险评分。

    参数:
        df_ohlcv      : 完整日K线 DataFrame（DateTimeIndex）
        eps_series    : EPS 序列（可为 None）
        bvps_series   : BVPS 序列（可为 None）
        index_data    : {"HSI": df_hsi, ...}，用于市场相关性（可为空 dict）
    """

    def __init__(
        self,
        df_ohlcv: pd.DataFrame,
        eps_series: Optional[pd.Series],
        bvps_series: Optional[pd.Series],
        index_data: dict,
    ):
        self._eps = eps_series
        self._bvps = bvps_series
        self._index_data = index_data

        # 预计算全量技术指标（因果指标，无前视偏差）
        print("  [SignalEngine] 预计算技术指标...", end=" ", flush=True)
        self._df = _add_technical_indicators(df_ohlcv.copy())
        print(f"完成，共 {len(self._df)} 行，{len(self._df.columns)} 列")

    def _calc_valuation_zscores(
        self, df_slice: pd.DataFrame, window: int = 756
    ) -> tuple:
        """
        计算 PE/PB 的 Z-score（均值回归策略核心指标）。

        Z = (current_val - rolling_mean) / rolling_std
        window 默认 756 天 ≈ 3 年。

        返回: (pe_zscore, pb_zscore)，无数据时返回 None。
        """
        pe_z, pb_z = None, None

        if self._eps is not None and not self._eps.empty:
            eps_aligned = _align_financial_to_daily(self._eps, df_slice.index)
            pe = df_slice['Close'] / eps_aligned.replace(0, np.nan)
            pe = pe.dropna()
            if len(pe) >= window:
                pe_tail = pe.tail(window)
                mu, sigma = pe_tail.mean(), pe_tail.std()
                if sigma > 0:
                    pe_z = float((pe_tail.iloc[-1] - mu) / sigma)
            elif len(pe) >= 60:
                mu, sigma = pe.mean(), pe.std()
                if sigma > 0:
                    pe_z = float((pe.iloc[-1] - mu) / sigma)

        if self._bvps is not None and not self._bvps.empty:
            bvps_aligned = _align_financial_to_daily(self._bvps, df_slice.index)
            pb = df_slice['Close'] / bvps_aligned.replace(0, np.nan)
            pb = pb.dropna()
            if len(pb) >= window:
                pb_tail = pb.tail(window)
                mu, sigma = pb_tail.mean(), pb_tail.std()
                if sigma > 0:
                    pb_z = float((pb_tail.iloc[-1] - mu) / sigma)
            elif len(pb) >= 60:
                mu, sigma = pb.mean(), pb.std()
                if sigma > 0:
                    pb_z = float((pb.iloc[-1] - mu) / sigma)

        return pe_z, pb_z

    def compute_at(self, date: pd.Timestamp) -> SignalSnapshot:
        """
        在 date 这一决策日计算完整信号快照。
        只使用 date 及之前的数据（walk-forward 保证）。
        """
        # Walk-forward 切片
        df_slice = self._df.loc[:date]

        if df_slice.empty:
            return self._empty_snapshot(date, 0.0)

        latest = df_slice.iloc[-1]
        close = float(latest.get('Close', 0))

        if close <= 0:
            return self._empty_snapshot(date, close)

        # ---- 多因子风险（walk-forward，需历史百分位）----
        long_risk_dict = calc_multifactor_risk(
            df_slice, term="long",
            eps_series=self._eps,
            bvps_series=self._bvps,
        )
        short_risk_dict = calc_multifactor_risk(
            df_slice, term="short",
            eps_series=self._eps,
            bvps_series=self._bvps,
        )

        long_risk = long_risk_dict.get("risk_level")
        short_risk = short_risk_dict.get("risk_level")

        resonance_dict = _assess_resonance(long_risk, short_risk)

        # ---- 技术信号（直接从预计算切片末尾读取）----
        trend = _calc_trend_signals(df_slice)

        # ---- 价格百分位 ----
        pct_1y = _calc_price_percentile_rank(df_slice, lookback_days=252)
        pct_5y = _calc_price_percentile_rank(df_slice, lookback_days=1260)

        # ---- 子因子明细 ----
        factors = long_risk_dict.get("factors", {})

        # ---- 估值 Z-score（均值回归策略）----
        pe_zscore, pb_zscore = self._calc_valuation_zscores(df_slice)

        # ---- 12 个月绝对回报（双动量策略）----
        return_12m = None
        if len(df_slice) >= 252:
            return_12m = float(
                df_slice['Close'].iloc[-1] / df_slice['Close'].iloc[-252] - 1
            )

        # ---- KAMA / 成交量突破 / ATR（趋势跟踪策略）----
        kama_value, kama_direction, volume_breakout, atr_value = (
            None, None, None, None
        )

        # KAMA
        kama_col = _get_dynamic_col(df_slice, 'KAMA')
        if kama_col and kama_col in df_slice.columns:
            kv = latest.get(kama_col)
            if kv is not None and not (isinstance(kv, float) and np.isnan(kv)):
                kama_value = float(kv)
                # KAMA 方向：对比5日前
                if len(df_slice) >= 6:
                    kama_prev = df_slice[kama_col].iloc[-6]
                    if not (isinstance(kama_prev, float) and np.isnan(kama_prev)):
                        diff_pct = (kama_value - float(kama_prev)) / float(kama_prev)
                        if diff_pct > 0.005:
                            kama_direction = "up"
                        elif diff_pct < -0.005:
                            kama_direction = "down"
                        else:
                            kama_direction = "flat"

        # 成交量突破
        vol = latest.get('Volume')
        if vol is not None and len(df_slice) >= 20:
            ma_vol = df_slice['Volume'].tail(20).mean()
            if ma_vol > 0:
                volume_breakout = bool(float(vol) > 1.5 * ma_vol)

        # ATR
        for atr_col_name in ['ATRr_14', 'ATR_14']:
            if atr_col_name in df_slice.columns:
                av = latest.get(atr_col_name)
                if av is not None and not (isinstance(av, float) and np.isnan(av)):
                    atr_value = float(av)
                    break

        # ---- 原始指标值 ----
        raw = {}
        for col in ['RSI_14', 'MACD_12_26_9', 'MACDs_12_26_9',
                    'SMA_5', 'SMA_20', 'SMA_60', 'SMA_250',
                    'K_9_3', 'D_9_3', 'J_9_3']:
            v = latest.get(col)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                raw[col] = float(v)

        return SignalSnapshot(
            date=date,
            close=close,
            long_term_risk=long_risk,
            long_term_zone=long_risk_dict.get("risk_zone", "数据不足"),
            short_term_risk=short_risk,
            short_term_zone=short_risk_dict.get("risk_zone", "数据不足"),
            resonance=resonance_dict.get("direction", "unknown"),
            ma_alignment=trend.get("ma_alignment"),
            macd_cross=trend.get("macd_cross"),
            macd_above_zero=trend.get("macd_above_zero"),
            rsi_zone=trend.get("rsi_zone"),
            rsi_value=raw.get("RSI_14"),
            kdj_zone=trend.get("kdj_zone"),
            boll_position=trend.get("boll_position"),
            above_ma20=trend.get("above_ma20"),
            above_ma60=trend.get("above_ma60"),
            above_ma250=trend.get("above_ma250"),
            price_pct_1y=pct_1y,
            price_pct_5y=pct_5y,
            factor_valuation=factors.get("valuation"),
            factor_momentum=factors.get("momentum"),
            factor_volatility=factors.get("volatility"),
            factor_technical=factors.get("technical"),
            factor_capital_flow=factors.get("capital_flow"),
            pe_zscore=pe_zscore,
            pb_zscore=pb_zscore,
            return_12m=return_12m,
            kama_value=kama_value,
            kama_direction=kama_direction,
            volume_breakout=volume_breakout,
            atr_value=atr_value,
            raw=raw,
        )

    @staticmethod
    def _empty_snapshot(date: pd.Timestamp, close: float) -> SignalSnapshot:
        return SignalSnapshot(
            date=date, close=close,
            long_term_risk=None, long_term_zone="数据不足",
            short_term_risk=None, short_term_zone="数据不足",
            resonance="unknown",
            ma_alignment=None, macd_cross=None, macd_above_zero=None,
            rsi_zone=None, rsi_value=None, kdj_zone=None, boll_position=None,
            above_ma20=None, above_ma60=None, above_ma250=None,
            price_pct_1y=None, price_pct_5y=None,
        )
