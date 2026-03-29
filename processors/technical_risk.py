"""
technical_risk.py — 风控指标模块

包含内容：
    - _calc_1y_risk_metrics : 计算过去 1 年核心风控指标
          输出：夏普比率(Sharpe)、最大回撤(Max Drawdown)、
          52 周高低价、52 周价格水位、3 年价格水位
    - _risk_zone_label      : 将风险水平数值（0~1）转换为人类可读标签
          支持三种标准：long（长线）、short（短线）、cycle（周期）
    - _assess_resonance     : 多周期共振判断
          比较长线与短线风险方向，输出 bullish/bearish/divergent/neutral

依赖：config.RISK_FREE_RATE
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import RISK_FREE_RATE


def _calc_1y_risk_metrics(df: pd.DataFrame, risk_free_rate: float = RISK_FREE_RATE) -> dict:
    """
    计算过去 1 年 (约 252 个交易日) 的核心风控指标：夏普比率与最大回撤，及 52 周水位。
    假设无风险利率(Rf) 为 4% (0.04)。
    """
    if df.empty or len(df) < 20:
        return {
            "sharpe_ratio_1y": None,
            "max_drawdown_1y_ratio": None,
            "high_52w": None,
            "low_52w": None,
            "price_position_52w_ratio": None
        }

    # 取最近一年的数据切片 (约252个交易日)
    df_1y = df.tail(252).copy()

    # --- 1. 计算夏普比率 (Sharpe Ratio) ---
    # 计算每日收益率
    daily_returns = df_1y['Close'].pct_change()

    # 防御：将无限大(inf)强行转换为 NaN，然后再统一清除，防止底层数学运算崩溃
    daily_returns = daily_returns.replace([np.inf, -np.inf], np.nan).dropna()

    if daily_returns.empty or daily_returns.std() == 0:
        sharpe_ratio = None
    else:
        # 年化收益率 = 日均收益率 * 252
        annual_return = daily_returns.mean() * 252
        # 年化波动率 = 日收益率标准差 * sqrt(252)
        annual_volatility = daily_returns.std() * np.sqrt(252)
        # 夏普比率 = (年化收益 - 无风险收益) / 年化波动率
        sharpe = (annual_return - risk_free_rate) / annual_volatility
        sharpe_ratio = float(sharpe)

    # --- 2. 计算最大回撤 (Maximum Drawdown) ---
    # 累计最高价
    rolling_max = df_1y['Close'].cummax()
    # 当前价与累计最高价的回撤比例
    drawdowns = (df_1y['Close'] - rolling_max) / rolling_max
    max_drawdown = float(drawdowns.min())

    # -- 3. 计算 52 周最高/最低及水位线 ---
    high_52w = float(df_1y['High'].max())
    low_52w = float(df_1y['Low'].min())
    current_price = float(df_1y['Close'].iloc[-1])

    # 水位线：计算当前价格在 52 周区间内的百分位 (0~1 之间)
    if high_52w > low_52w:
        price_position = (current_price - low_52w) / (high_52w - low_52w)
    else:
        price_position = None

    # 3 年价格百分位 (约 756 个交易日)
    df_3y = df.tail(756)
    high_3y = float(df_3y['High'].max())
    low_3y = float(df_3y['Low'].min())
    if high_3y > low_3y:
        price_position_3y = (current_price - low_3y) / (high_3y - low_3y)
    else:
        price_position_3y = None

    return {
        "sharpe_ratio_1y": sharpe_ratio,
        "max_drawdown_1y_ratio": max_drawdown,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "price_position_52w_ratio": float(price_position) if price_position is not None else None,
        "high_3y": high_3y,
        "low_3y": low_3y,
        "price_position_3y_ratio": float(price_position_3y) if price_position_3y is not None else None
    }


def _risk_zone_label(risk_level: float, term: str) -> str:
    """
    将风险水平数值转换为人类和 AI 都能直接理解的标签。

    term = "long":  长线标准 (<0.05 机会区, >0.95 风险区)
    term = "short": 短线标准 (<0.01 机会点, >0.99 风险点)
    term = "cycle": 周期标准 (<0.05 周期机会区, >0.95 周期风险区)
    """
    if risk_level is None:
        return "数据不足"

    if term == "short":
        if risk_level < 0.01:
            return "短线机会点"
        elif risk_level < 0.10:
            return "短线偏低"
        elif risk_level > 0.99:
            return "短线风险点"
        elif risk_level > 0.90:
            return "短线偏高"
        else:
            return "短线中性"
    else:
        # long 和 cycle 共用同一套阈值
        if risk_level < 0.05:
            return "机会区"
        elif risk_level < 0.20:
            return "偏低（有吸引力）"
        elif risk_level > 0.95:
            return "风险区"
        elif risk_level > 0.80:
            return "偏高（需谨慎）"
        else:
            return "中性"


def _assess_resonance(long_risk: float, short_risk: float) -> dict:
    """
    多周期共振判断：长线与短线风险方向是否一致。

    返回:
        {
            "direction": "bullish" / "bearish" / "divergent" / "neutral",
            "description": 人类可读的中文解释
        }
    """
    if long_risk is None or short_risk is None:
        return {"direction": "unknown", "description": "数据不足，无法判断多周期共振"}

    long_low = long_risk < 0.30   # 长线偏低
    long_high = long_risk > 0.70  # 长线偏高
    short_low = short_risk < 0.30
    short_high = short_risk > 0.70

    if long_low and short_low:
        return {
            "direction": "bullish",
            "description": f"多周期共振看多：长线({long_risk:.2f})和短线({short_risk:.2f})风险均偏低，长短共振形成较强机会信号"
        }
    elif long_high and short_high:
        return {
            "direction": "bearish",
            "description": f"多周期共振看空：长线({long_risk:.2f})和短线({short_risk:.2f})风险均偏高，长短共振形成较强风险信号"
        }
    elif long_low and short_high:
        return {
            "direction": "divergent",
            "description": f"长短背离（短空长多）：长线({long_risk:.2f})偏低但短线({short_risk:.2f})偏高，短期可能有回调但长期仍有价值"
        }
    elif long_high and short_low:
        return {
            "direction": "divergent",
            "description": f"长短背离（短多长空）：长线({long_risk:.2f})偏高但短线({short_risk:.2f})偏低，短期可能反弹但长期需警惕"
        }
    else:
        return {
            "direction": "neutral",
            "description": f"长线({long_risk:.2f})和短线({short_risk:.2f})均处于中性区间，无明显方向信号"
        }
