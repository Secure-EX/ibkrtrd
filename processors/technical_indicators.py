"""
technical_indicators.py — 技术指标计算模块

包含内容：
    - _add_technical_indicators  : 批量为 OHLCV DataFrame 添加技术指标
          计算的指标：MA(5/10/20/30/60/120/250)、MACD(12,26,9)、RSI(14)、
          KDJ(9,3)、BOLL(20,2)、ATR(14)、KAMA(10,2,30)、VWAP_Custom
    - _calc_trend_signals        : 基于已有指标生成确定性研判信号
          信号包含：均线多空排列、价格相对MA位置、MACD金叉/死叉、
          RSI区间、KDJ区间、布林带位置
    - _calc_price_percentile_rank: 计算当前价格在过去 N 个交易日中的分位数排名
          抗极端值，反映真实价格分布位置（0=历史低位，1=历史高位）

依赖：pandas_ta、technical_utils._get_dynamic_col
"""

import pandas as pd
import numpy as np
import pandas_ta as ta  # noqa: F401 (used via df.ta accessor)

try:
    from .technical_utils import _get_dynamic_col
except ImportError:
    from technical_utils import _get_dynamic_col


def _add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    内部辅助函数：为输入的 DataFrame 批量添加技术指标 (MA, MACD, RSI, KDJ, BOLL, VWAP)。
    依赖: pandas_ta 库
    """
    if df.empty or len(df) < 20:
        return df

    # 1. 均线系统 (MA: 5, 10, 20, 30, 60, 120, 250)
    ma_windows = [5, 10, 20, 30, 60, 120, 250]
    for w in ma_windows:
        # append=True 会直接把算好的列 (例如 SMA_5) 加进原 df
        df.ta.sma(length=w, append=True)

    # 2. 动能指标: MACD (12, 26, 9)
    # 自动生成列: MACD_12_26_9 (DIF), MACDs_12_26_9 (DEA), MACDh_12_26_9 (HIST/红绿柱)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)

    # 3. 动能指标: RSI (14) -> 生成列: RSI_14
    df.ta.rsi(length=14, append=True)

    # 4. 波动/震荡指标: KDJ (9, 3, 3) -> 生成列: K_9_3, D_9_3, J_9_3
    df.ta.kdj(length=9, signal=3, append=True)

    # 5. 波动指标: BOLL (20, 2)
    # 生成列: BBL_20_2.0 (下轨), BBM_20_2.0 (中轨), BBU_20_2.0 (上轨)
    df.ta.bbands(length=20, std=2, append=True)

    # 6. 波动指标: ATR (14) -> 生成列: ATRr_14
    df.ta.atr(length=14, append=True)

    # 7. 自适应均线: KAMA (10, 2, 30) -> 生成列: KAMA_10_2_30
    # Kaufman Adaptive Moving Average: 在趋势明确时快速跟随，在震荡时平滑过滤
    df.ta.kama(length=10, fast=2, slow=30, append=True)

    # 8. 计算当期 VWAP (成交量加权平均价) = 当期总成交额 / 当期总成交量
    # 注意：防止除以 0 的情况出现
    df['VWAP_Custom'] = np.where(df['Volume'] > 0, df['Turnover_Value'] / df['Volume'], df['Close'])

    return df


def _calc_trend_signals(df: pd.DataFrame) -> dict:
    """
    基于技术指标计算确定性研判信号，减少 LLM 的猜测负担。

    信号说明：
        ma_alignment      : 均线多空排列 (bullish/bearish/mixed)
        above_ma20/60/250 : 价格是否在均线上方
        macd_cross        : MACD 金叉/死叉/无
        macd_above_zero   : DIF 是否在零轴上方
        rsi_zone          : RSI 超买/超卖/中性
        kdj_zone          : KDJ-J 超买/超卖/中性
        boll_position     : 布林带位置（上轨外/下轨外/轨内）
    """
    if df is None or df.empty or len(df) < 5:
        return {}

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None

    signals = {}

    # --- 1. 均线排列判定 ---
    ma_keys = ['SMA_5', 'SMA_10', 'SMA_20', 'SMA_30', 'SMA_60', 'SMA_120', 'SMA_250']
    ma_vals = [latest.get(k) for k in ma_keys if k in latest.index and pd.notna(latest.get(k))]

    if len(ma_vals) >= 4:
        # 短期均线全部 > 长期均线 = 多头排列
        is_bullish = all(ma_vals[i] >= ma_vals[i+1] for i in range(len(ma_vals)-1))
        is_bearish = all(ma_vals[i] <= ma_vals[i+1] for i in range(len(ma_vals)-1))

        if is_bullish:
            signals["ma_alignment"] = "bullish"
        elif is_bearish:
            signals["ma_alignment"] = "bearish"
        else:
            signals["ma_alignment"] = "mixed"

    # --- 2. 价格相对均线位置 ---
    close = latest.get('Close')
    if close and pd.notna(close):
        ma20 = latest.get('SMA_20')
        ma60 = latest.get('SMA_60')
        ma250 = latest.get('SMA_250')
        signals["above_ma20"] = bool(close > ma20) if pd.notna(ma20) else None
        signals["above_ma60"] = bool(close > ma60) if pd.notna(ma60) else None
        signals["above_ma250"] = bool(close > ma250) if pd.notna(ma250) else None

    # --- 3. MACD 金叉/死叉 ---
    if prev is not None:
        macd_col = 'MACD_12_26_9'
        signal_col = 'MACDs_12_26_9'
        if all(c in latest.index for c in [macd_col, signal_col]):
            dif_now = latest.get(macd_col)
            dea_now = latest.get(signal_col)
            dif_prev = prev.get(macd_col)
            dea_prev = prev.get(signal_col)

            if all(pd.notna(v) for v in [dif_now, dea_now, dif_prev, dea_prev]):
                if dif_prev <= dea_prev and dif_now > dea_now:
                    signals["macd_cross"] = "golden_cross"
                elif dif_prev >= dea_prev and dif_now < dea_now:
                    signals["macd_cross"] = "death_cross"
                else:
                    signals["macd_cross"] = "none"

                # DIF 在零轴上方/下方
                signals["macd_above_zero"] = bool(dif_now > 0)

    # --- 4. RSI 区间判定 ---
    rsi = latest.get('RSI_14')
    if pd.notna(rsi):
        if rsi >= 70:
            signals["rsi_zone"] = "overbought"
        elif rsi <= 30:
            signals["rsi_zone"] = "oversold"
        else:
            signals["rsi_zone"] = "neutral"

    # --- 5. KDJ 区间判定 ---
    # J 值常规阈值是 ≥100 超买 / ≤0 超卖（J 可超出 [0,100] 区间），
    # 与 K/D 的 80/20 阈值不同。
    kdj_j = latest.get('J_9_3')
    if pd.notna(kdj_j):
        if kdj_j >= 100:
            signals["kdj_zone"] = "overbought"
        elif kdj_j <= 0:
            signals["kdj_zone"] = "oversold"
        else:
            signals["kdj_zone"] = "neutral"

    # --- 6. 布林带位置 ---
    col_bbu = None
    col_bbl = None
    for c in df.columns:
        if c.startswith('BBU_'): col_bbu = c
        if c.startswith('BBL_'): col_bbl = c

    if close and col_bbu and col_bbl:
        bbu = latest.get(col_bbu)
        bbl = latest.get(col_bbl)
        if pd.notna(bbu) and pd.notna(bbl):
            if close >= bbu:
                signals["boll_position"] = "above_upper"
            elif close <= bbl:
                signals["boll_position"] = "below_lower"
            else:
                signals["boll_position"] = "within_bands"

    return signals


def _calc_price_percentile_rank(df: pd.DataFrame, lookback_days: int) -> float:
    """
    计算当前价格在过去 N 个交易日所有收盘价中的百分位排名（分位数法）。

    与线性极值法 (current-min)/(max-min) 的区别：
    极值法容易被单日极端价格扭曲，分位数法统计的是"有多少天比现在便宜"，
    抗极端值能力更强，更能反映真实的价格分布位置。

    返回: 0~1 之间的数值
        0.19 = 过去 N 天中只有 19% 的时间比现在低 → 当前处于相对低位 (机会区)
        0.85 = 过去 N 天中有 85% 的时间比现在低 → 当前处于相对高位 (风险区)
    """
    if df is None or df.empty or len(df) < lookback_days * 0.5:
        # 数据量不足回溯窗口的一半，结果不可靠
        return None

    closes = df['Close'].tail(lookback_days).dropna()
    if closes.empty:
        return None

    current_price = float(closes.iloc[-1])
    rank = float((closes < current_price).sum()) / len(closes)
    return rank


# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path
    import pandas as pd

    BASE_DIR = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(BASE_DIR))
    from config import OHLCV_DIR

    test_ticker = "0700.HK"
    file_path = OHLCV_DIR / f"{test_ticker}_daily.csv"

    if not file_path.exists():
        print(f"⚠️ 找不到 {test_ticker} 的量价数据: {file_path}")
    else:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(ascending=True, inplace=True)

        print(f"⚙️ 正在计算 {test_ticker} 的技术指标...")

        # 计算技术指标
        base_cols = set(df.columns)
        df = _add_technical_indicators(df)
        new_cols = [c for c in df.columns if c not in base_cols]
        print(f"✅ 技术指标计算完成，新增 {len(new_cols)} 列: {new_cols}")

        # 计算趋势信号
        signals = _calc_trend_signals(df)
        print("\n趋势研判信号:")
        print(json.dumps(signals, indent=4, ensure_ascii=False))

        # 计算价格百分位
        pct_1y = _calc_price_percentile_rank(df, lookback_days=252)
        pct_5y = _calc_price_percentile_rank(df, lookback_days=1260)
        print(f"\n价格分位数（1年窗口）: {round(pct_1y, 4) if pct_1y is not None else None}")
        print(f"价格分位数（5年窗口）: {round(pct_5y, 4) if pct_5y is not None else None}")
