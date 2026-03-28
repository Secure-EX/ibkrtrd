import sys
import pandas as pd
import pandas_ta as ta
import numpy as np
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import OHLCV_DIR, RISK_FREE_RATE

# 财报发布滞后（交易日）：港股上市公司季报通常在季度结束后 45~90 天才披露，
# 直接用财报日期做 ffill 会让未来数据渗入历史 PE/PB 计算（look-ahead bias）。
# 将财报索引向后平移此天数后再 ffill，确保只使用"当时已公开"的数据。
FINANCIAL_PUBLICATION_LAG_DAYS = 60


def _align_financial_to_daily(fin_series: pd.Series, daily_index: pd.DatetimeIndex) -> pd.Series:
    """
    将财报序列（EPS / BVPS）对齐到日线索引，并加入发布滞后防止 look-ahead bias。

    步骤：
        1. 将财报日期向后平移 FINANCIAL_PUBLICATION_LAG_DAYS 个日历天
        2. reindex 到日线索引 + ffill（只向前填充已公开的数据）
    """
    shifted = fin_series.copy()
    shifted.index = shifted.index + pd.Timedelta(days=FINANCIAL_PUBLICATION_LAG_DAYS)
    return shifted.reindex(daily_index).ffill()

# ==========================================
# 核心计算函数
# ==========================================

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

def _calc_trend_signals(df: pd.DataFrame) -> dict:
    """
    基于技术指标计算确定性研判信号，减少 LLM 的猜测负担。
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
    kdj_j = latest.get('J_9_3')
    if pd.notna(kdj_j):
        if kdj_j >= 80:
            signals["kdj_zone"] = "overbought"
        elif kdj_j <= 20:
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
# 多因子风险水平评估系统
# ==========================================

def _percentile_rank_in_series(value: float, history: pd.Series) -> float:
    """
    计算单个数值在历史序列中的百分位排名（0~1）。
    用于将任意因子的原始值归一化到统一尺度。
    """
    if history is None or history.empty or pd.isna(value):
        return None
    clean = history.dropna()
    if len(clean) < 10:
        return None
    return float((clean < value).sum()) / len(clean)


def _calc_valuation_factor(df: pd.DataFrame, window: int,
                           eps_series: pd.Series = None,
                           bvps_series: pd.Series = None) -> float:
    """
    估值因子：PE/PB 历史百分位的均值。

    如果没有传入 eps_series / bvps_series，则退化为
    价格/SMA250 的历史百分位（一种简易的价格偏离度估值代理）。

    参数:
        df       : 含 Close 列的 OHLCV DataFrame
        window   : 回溯窗口长度
        eps_series : 与 df 同索引的每股收益序列（可选）
        bvps_series: 与 df 同索引的每股净资产序列（可选）
    """
    closes = df['Close'].tail(window)
    if len(closes) < 20:
        return None

    factors = []

    # --- PE 百分位 ---
    if eps_series is not None and not eps_series.empty:
        # 对齐索引，加入发布滞后防止 look-ahead bias
        eps_aligned = _align_financial_to_daily(eps_series, df.index)
        pe = df['Close'] / eps_aligned.replace(0, np.nan)
        pe_window = pe.tail(window).dropna()
        if len(pe_window) >= 20:
            current_pe = pe_window.iloc[-1]
            factors.append(_percentile_rank_in_series(current_pe, pe_window))

    # --- PB 百分位 ---
    if bvps_series is not None and not bvps_series.empty:
        bvps_aligned = _align_financial_to_daily(bvps_series, df.index)
        pb = df['Close'] / bvps_aligned.replace(0, np.nan)
        pb_window = pb.tail(window).dropna()
        if len(pb_window) >= 20:
            current_pb = pb_window.iloc[-1]
            factors.append(_percentile_rank_in_series(current_pb, pb_window))

    # --- Fallback: 价格偏离度代理 ---
    if not factors:
        # 用 Close / SMA_250（或窗口适配的长均线）作为估值代理
        ma_len = min(250, window // 2) if window >= 500 else min(60, window // 2)
        ma = df['Close'].rolling(ma_len, min_periods=max(10, ma_len // 2)).mean()
        deviation = (df['Close'] / ma.replace(0, np.nan)).tail(window).dropna()
        if len(deviation) >= 20:
            current_dev = deviation.iloc[-1]
            factors.append(_percentile_rank_in_series(current_dev, deviation))

    valid = [f for f in factors if f is not None]
    return float(np.mean(valid)) if valid else None


def _calc_momentum_factor(df: pd.DataFrame, periods: list, window: int) -> float:
    """
    动量因子：多周期涨跌幅的历史百分位均值。

    参数:
        df      : 含 Close 列的 OHLCV DataFrame
        periods : 计算动量的周期列表，如 [252]（长线12月） 或 [5, 10]（短线）
        window  : 百分位回溯窗口
    """
    factors = []
    for p in periods:
        ret = df['Close'].pct_change(p)
        ret_window = ret.tail(window).dropna()
        if len(ret_window) < 20:
            continue
        current_ret = ret_window.iloc[-1]
        factors.append(_percentile_rank_in_series(current_ret, ret_window))

    valid = [f for f in factors if f is not None]
    return float(np.mean(valid)) if valid else None


def _calc_volatility_factor_long(df: pd.DataFrame, window: int) -> float:
    """
    长线波动率因子：年化波动率的历史百分位。

    使用 20 日滚动窗口计算日收益率标准差，再年化，
    最后取当前值在历史窗口内的百分位。
    """
    daily_ret = df['Close'].pct_change().replace([np.inf, -np.inf], np.nan)
    rolling_vol = daily_ret.rolling(20, min_periods=10).std() * np.sqrt(252)
    vol_window = rolling_vol.tail(window).dropna()
    if len(vol_window) < 20:
        return None
    current_vol = vol_window.iloc[-1]
    return _percentile_rank_in_series(current_vol, vol_window)


def _calc_volatility_factor_short(df: pd.DataFrame, window: int) -> float:
    """
    短线波动率因子：ATR 或日内波幅百分位。

    优先使用已计算的 ATR 列；如果不存在，使用 (High-Low)/Close 作为替代。
    """
    atr_col = _get_dynamic_col(df, 'ATR')
    if atr_col and atr_col in df.columns:
        vol_series = df[atr_col] / df['Close']  # 归一化为比率
    else:
        # 日内波幅率
        vol_series = (df['High'] - df['Low']) / df['Close'].replace(0, np.nan)

    vol_window = vol_series.tail(window).dropna()
    if len(vol_window) < 20:
        return None
    current_vol = vol_window.iloc[-1]
    return _percentile_rank_in_series(current_vol, vol_window)


def _calc_technical_factor(df: pd.DataFrame, window: int) -> float:
    """
    技术因子：RSI 和 KDJ-J 值的历史百分位均值。

    RSI/KDJ 本身是 0~100 的指标，但在不同股票/不同时期的分布差异很大，
    通过百分位归一化可以跨标的比较。
    """
    factors = []

    # RSI 百分位
    rsi_col = 'RSI_14'
    if rsi_col in df.columns:
        rsi_window = df[rsi_col].tail(window).dropna()
        if len(rsi_window) >= 20:
            current_rsi = rsi_window.iloc[-1]
            factors.append(_percentile_rank_in_series(current_rsi, rsi_window))

    # KDJ-J 百分位
    j_col = 'J_9_3'
    if j_col in df.columns:
        j_window = df[j_col].tail(window).dropna()
        if len(j_window) >= 20:
            current_j = j_window.iloc[-1]
            factors.append(_percentile_rank_in_series(current_j, j_window))

    valid = [f for f in factors if f is not None]
    return float(np.mean(valid)) if valid else None


def _calc_capital_flow_factor(df: pd.DataFrame, window: int) -> float:
    """
    资金因子（机构持仓代理）：量比（volume_ratio）的历史百分位。

    由于缺失机构持仓数据，使用以下代理指标的组合：
    1. 量比 (当日成交量 / 20日均量) — 反映资金活跃度
    2. 价量相关性 — 正相关说明主力推动，负相关说明散户出货

    两者取均值作为最终的资金因子。
    """
    factors = []

    # --- 量比百分位 ---
    ma_vol = df['Volume'].rolling(20, min_periods=5).mean()
    vol_ratio = (df['Volume'] / ma_vol.replace(0, np.nan))
    vr_window = vol_ratio.tail(window).dropna()
    if len(vr_window) >= 20:
        current_vr = vr_window.iloc[-1]
        factors.append(_percentile_rank_in_series(current_vr, vr_window))

    # --- 价量相关性百分位（10 日滚动 Pearson） ---
    price_ret = df['Close'].pct_change()
    vol_ret = df['Volume'].pct_change()
    corr_rolling = price_ret.rolling(10, min_periods=5).corr(vol_ret)
    corr_window = corr_rolling.tail(window).dropna()
    if len(corr_window) >= 20:
        current_corr = corr_window.iloc[-1]
        factors.append(_percentile_rank_in_series(current_corr, corr_window))

    valid = [f for f in factors if f is not None]
    return float(np.mean(valid)) if valid else None


def _rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """
    向量化滚动百分位排名：对序列中每个点，计算它在过去 window 个值中的百分位。

    替代逐点调用 _percentile_rank_in_series 的 O(n²) 采样循环，
    一次 rolling pass 即可得到完整的百分位时间序列。

    返回: 与输入等长的 pd.Series，值域 [0, 1]
    """
    def _pct_rank(arr):
        n = len(arr)
        if n < 2:
            return np.nan
        return (arr[:-1] < arr[-1]).sum() / (n - 1)

    return series.rolling(window, min_periods=max(20, window // 4)).apply(_pct_rank, raw=True)


def _calc_all_factor_series(
    df: pd.DataFrame, term: str,
    val_window: int, mom_periods: list, mom_window: int,
    vol_window: int, tech_window: int, cap_window: int,
    eps_series: pd.Series = None, bvps_series: pd.Series = None,
) -> dict:
    """
    向量化计算所有因子的滚动百分位排名时间序列。

    与逐因子调用 _calc_*_factor() 获取单点值不同，本函数一次性返回
    每个因子在整段历史上的百分位序列，供 calc_multifactor_risk 用于：
        - 取最新值作为当前因子分数（替代 Step 1）
        - 加权合成为 composite 时间序列（替代 Step 3 的采样循环）

    返回:
        {"valuation": pd.Series, "momentum": pd.Series, ...}
        如果某因子无法计算，则不包含该 key。
    """
    result = {}

    # ========== 估值因子 ==========
    val_ranks = []
    if eps_series is not None and not eps_series.empty:
        eps_aligned = _align_financial_to_daily(eps_series, df.index)
        pe = df['Close'] / eps_aligned.replace(0, np.nan)
        r = _rolling_percentile_rank(pe, val_window)
        if r.dropna().shape[0] >= 20:
            val_ranks.append(r)
    if bvps_series is not None and not bvps_series.empty:
        bvps_aligned = _align_financial_to_daily(bvps_series, df.index)
        pb = df['Close'] / bvps_aligned.replace(0, np.nan)
        r = _rolling_percentile_rank(pb, val_window)
        if r.dropna().shape[0] >= 20:
            val_ranks.append(r)
    if not val_ranks:
        # Fallback: 价格偏离度
        ma_len = min(250, val_window // 2) if val_window >= 500 else min(60, val_window // 2)
        ma = df['Close'].rolling(ma_len, min_periods=max(10, ma_len // 2)).mean()
        deviation = df['Close'] / ma.replace(0, np.nan)
        r = _rolling_percentile_rank(deviation, val_window)
        if r.dropna().shape[0] >= 20:
            val_ranks.append(r)
    if val_ranks:
        result["valuation"] = sum(val_ranks) / len(val_ranks)

    # ========== 动量因子 ==========
    mom_ranks = []
    for p in mom_periods:
        ret = df['Close'].pct_change(p)
        r = _rolling_percentile_rank(ret, mom_window)
        if r.dropna().shape[0] >= 20:
            mom_ranks.append(r)
    if mom_ranks:
        result["momentum"] = sum(mom_ranks) / len(mom_ranks)

    # ========== 波动率因子 ==========
    if term == "long":
        daily_ret = df['Close'].pct_change().replace([np.inf, -np.inf], np.nan)
        vol_raw = daily_ret.rolling(20, min_periods=10).std() * np.sqrt(252)
    else:
        atr_col = _get_dynamic_col(df, 'ATR')
        if atr_col and atr_col in df.columns:
            vol_raw = df[atr_col] / df['Close']
        else:
            vol_raw = (df['High'] - df['Low']) / df['Close'].replace(0, np.nan)
    r = _rolling_percentile_rank(vol_raw, vol_window)
    if r.dropna().shape[0] >= 20:
        result["volatility"] = r

    # ========== 技术因子 ==========
    tech_ranks = []
    if 'RSI_14' in df.columns:
        r = _rolling_percentile_rank(df['RSI_14'], tech_window)
        if r.dropna().shape[0] >= 20:
            tech_ranks.append(r)
    if 'J_9_3' in df.columns:
        r = _rolling_percentile_rank(df['J_9_3'], tech_window)
        if r.dropna().shape[0] >= 20:
            tech_ranks.append(r)
    if tech_ranks:
        result["technical"] = sum(tech_ranks) / len(tech_ranks)

    # ========== 资金因子 ==========
    cap_ranks = []
    ma_vol = df['Volume'].rolling(20, min_periods=5).mean()
    vol_ratio = df['Volume'] / ma_vol.replace(0, np.nan)
    r = _rolling_percentile_rank(vol_ratio, cap_window)
    if r.dropna().shape[0] >= 20:
        cap_ranks.append(r)
    price_ret = df['Close'].pct_change()
    vol_ret = df['Volume'].pct_change()
    corr_rolling = price_ret.rolling(10, min_periods=5).corr(vol_ret)
    r = _rolling_percentile_rank(corr_rolling, cap_window)
    if r.dropna().shape[0] >= 20:
        cap_ranks.append(r)
    if cap_ranks:
        result["capital_flow"] = sum(cap_ranks) / len(cap_ranks)

    return result


def calc_multifactor_risk(
    df: pd.DataFrame,
    term: str = "long",
    hist_window: int = None,
    weights: dict = None,
    eps_series: pd.Series = None,
    bvps_series: pd.Series = None,
) -> dict:
    """
    多因子风险水平评估（核心入口函数）。

    工作流程：
        1. 分别计算 5 个子因子的百分位分数 (0~1)
        2. 加权合成为综合风险原始分
        3. 对综合分数做历史百分位归一化 → 最终风险水平

    参数:
        df           : 含 OHLCV + 技术指标的 DataFrame (日线或月线)
        term         : "long" = 长线 (月线/年线窗口) | "short" = 短线 (日线窗口)
        hist_window  : 历史百分位归一化的回溯窗口
                       默认 long=3780(约15年日线) / short=504(约2年日线)
        weights      : 各因子权重字典，键为因子名，值为权重 (自动归一化)
                       默认长线侧重估值，短线侧重动量与技术
        eps_series   : 逐期每股收益 (可选，提升估值因子精度)
        bvps_series  : 逐期每股净资产 (可选，提升估值因子精度)

    返回:
        {
            "risk_level": 0.15,              # 归一化后的最终风险水平 (0~1)
            "risk_zone": "偏低（有吸引力）",   # 人类可读标签
            "composite_raw": 0.32,           # 加权合成原始分（归一化前）
            "factors": {                     # 各因子明细
                "valuation": 0.12,
                "momentum": 0.45,
                "volatility": 0.28,
                "technical": 0.35,
                "capital_flow": 0.40
            },
            "weights_used": { ... },         # 实际使用的权重
            "data_quality": "full"           # full / partial / insufficient
        }
    """
    result_template = {
        "risk_level": None, "risk_zone": "数据不足", "composite_raw": None,
        "factors": {}, "weights_used": {}, "data_quality": "insufficient"
    }

    if df is None or df.empty or len(df) < 60:
        return result_template

    # ========== 参数默认值 ==========
    if term == "long":
        if hist_window is None:
            hist_window = min(len(df), 3780)  # 最多 15 年日线
        if weights is None:
            weights = {
                "valuation": 0.30,    # 长线核心：估值
                "momentum": 0.15,
                "volatility": 0.15,
                "technical": 0.20,
                "capital_flow": 0.20,
            }
        # 长线因子参数
        val_window = min(len(df), 3780)       # 估值回溯 ~15 年
        mom_periods = [252]                   # 12 个月涨跌幅
        mom_window = min(len(df), 3780)
        vol_window = min(len(df), 3780)
        tech_window = min(len(df), 1260)      # 技术指标 ~5 年
        cap_window = min(len(df), 1260)
    else:
        if hist_window is None:
            hist_window = min(len(df), 504)   # 约 2 年日线
        if weights is None:
            weights = {
                "valuation": 0.10,
                "momentum": 0.30,    # 短线核心：动量
                "volatility": 0.15,
                "technical": 0.30,   # 短线核心：技术信号
                "capital_flow": 0.15,
            }
        # 短线因子参数
        val_window = min(len(df), 504)
        mom_periods = [5, 10]                 # 5日/10日涨跌幅
        mom_window = min(len(df), 504)
        vol_window = min(len(df), 504)
        tech_window = min(len(df), 252)
        cap_window = min(len(df), 252)

    # ========== 向量化计算：一次 pass 替代 O(n²) 采样循环 ==========
    factor_series = _calc_all_factor_series(
        df, term, val_window, mom_periods, mom_window,
        vol_window, tech_window, cap_window, eps_series, bvps_series,
    )

    if not factor_series:
        return result_template

    # Step 1: 各因子最新值（取序列末尾）
    factors = {}
    for name, fs in factor_series.items():
        last_valid = fs.dropna()
        factors[name] = float(last_valid.iloc[-1]) if not last_valid.empty else None

    # Step 2: 加权合成（缺失因子权重重分配）
    valid_factors = {k: v for k, v in factors.items() if v is not None}
    if not valid_factors:
        return result_template

    total_w = sum(weights.get(k, 0) for k in valid_factors)
    if total_w == 0:
        return result_template
    norm_weights = {k: weights.get(k, 0) / total_w for k in valid_factors}

    composite_raw = sum(norm_weights[k] * valid_factors[k] for k in valid_factors)

    # Step 3: 历史百分位归一化（向量化版）
    # 用 factor_series 构建 composite 时间序列，对当前值做百分位排名
    factor_df = pd.DataFrame({k: factor_series[k] for k in valid_factors})
    factor_df = factor_df.dropna()  # 只保留所有因子都有值的行

    if len(factor_df) >= 10:
        composite_ts = sum(norm_weights[k] * factor_df[k] for k in valid_factors)
        composite_ts = composite_ts.tail(hist_window)
        risk_level = _percentile_rank_in_series(composite_raw, composite_ts)
    else:
        risk_level = composite_raw

    # ========== 数据质量评估 ==========
    n_valid = len(valid_factors)
    n_total = len(factor_series)
    if n_valid == n_total:
        data_quality = "full"
    elif n_valid >= 3:
        data_quality = "partial"
    else:
        data_quality = "limited"

    return {
        "risk_level": round(risk_level, 4) if risk_level is not None else None,
        "risk_zone": _risk_zone_label(risk_level, term if term == "short" else "long"),
        "composite_raw": round(composite_raw, 4),
        "factors": {k: round(v, 4) if v is not None else None for k, v in factors.items()},
        "weights_used": {k: round(v, 4) for k, v in norm_weights.items()},
        "data_quality": data_quality,
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

    # 7. 计算当期 VWAP (成交量加权平均价) = 当期总成交额 / 当期总成交量
    # 注意：防止除以 0 的情况出现
    df['VWAP_Custom'] = np.where(df['Volume'] > 0, df['Turnover_Value'] / df['Volume'], df['Close'])

    return df

def _safe_get(row: pd.Series, col_name: str, is_int: bool = False):
    """
    内部辅助函数：安全提取数据，把 DataFrame 中的 NaN/NaT 转换为 JSON 友好的 None，并保留 2 位小数。
    """
    if col_name not in row.index or pd.isna(row[col_name]):
        return None

    val = row[col_name]
    if is_int:
        return int(val)
    return float(val)

def _get_dynamic_col(df: pd.DataFrame, prefix: str) -> str:
    """
    动态列名匹配器：在 DataFrame 中寻找以指定前缀开头的列名。
    解决 pandas_ta 版本更新导致列名（如 BBU_20_2.0_2.0）变动的问题。
    """
    for col in df.columns:
        if col.startswith(prefix):
            return col
    return "" # 如果没找到，返回空字符串

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


def _extract_latest_features(
    df: pd.DataFrame,
    cycle_risk_block: dict = None,
    market_correlation: dict = None,
) -> dict:
    """
    提取时间序列的最后一行(最新数据)，拼装成目标 JSON Schema 的结构。

    参数:
        df                  : 含 OHLCV + 技术指标的 DataFrame
        cycle_risk_block    : 预计算的多因子风险评估结果（仅日线传入，周线/月线为 None）
        market_correlation  : 与大盘相关性字典（仅日线传入）
    """
    if df is None or df.empty:
        return {}

    latest = df.iloc[-1]

    # 获取日期字符串
    date_str = latest.name.strftime('%Y-%m-%d') if isinstance(latest.name, pd.Timestamp) else str(latest.name)

    # 动态获取布林带的真实列名
    col_bbu = _get_dynamic_col(df, 'BBU_')
    col_bbm = _get_dynamic_col(df, 'BBM_')
    col_bbl = _get_dynamic_col(df, 'BBL_')

    # 调用风控计算模块
    risk_metrics = _calc_1y_risk_metrics(df)

    # 调用趋势研判模块
    trend_signals = _calc_trend_signals(df)

    # 近期走势摘要（压缩版，节省 token）
    n_recent = min(10, len(df))
    recent_slice = df['Close'].tail(n_recent)
    recent_trend = None
    if len(recent_slice) >= 2:
        first_close = float(recent_slice.iloc[0])
        last_close = float(recent_slice.iloc[-1])
        pct_chg = (last_close - first_close) / first_close if first_close != 0 else 0
        first_date = recent_slice.index[0].strftime('%Y-%m-%d') if isinstance(recent_slice.index[0], pd.Timestamp) else str(recent_slice.index[0])
        last_date = recent_slice.index[-1].strftime('%Y-%m-%d') if isinstance(recent_slice.index[-1], pd.Timestamp) else str(recent_slice.index[-1])
        recent_trend = {
            "period": f"{first_date} ~ {last_date}",
            "open": round(first_close, 4),
            "close": round(last_close, 4),
            "high": round(float(recent_slice.max()), 4),
            "low": round(float(recent_slice.min()), 4),
            "change_pct": round(pct_chg, 4),
        }

    result = {
        "date": date_str,
        "close": _safe_get(latest, 'Close'),
        "volume": _safe_get(latest, 'Volume', is_int=True),
        "volume_ratio_20d": float(latest['Volume'] / df['Volume'].tail(20).mean()) if len(df) >= 20 and df['Volume'].tail(20).mean() > 0 else None,
        "turnover_value": _safe_get(latest, 'Turnover_Value', is_int=True),
        "vwap": _safe_get(latest, 'VWAP_Custom'),
        "risk_metrics": risk_metrics,
        "trend_signals": trend_signals,
        "recent_trend": recent_trend,
        "trend": {
            "ma5": _safe_get(latest, 'SMA_5'),
            "ma10": _safe_get(latest, 'SMA_10'),
            "ma20": _safe_get(latest, 'SMA_20'),
            "ma30": _safe_get(latest, 'SMA_30'),
            "ma60": _safe_get(latest, 'SMA_60'),
            "ma120": _safe_get(latest, 'SMA_120'),
            "ma250": _safe_get(latest, 'SMA_250')
        },
        "momentum": {
            "macd_dif": _safe_get(latest, 'MACD_12_26_9'),
            "macd_dea": _safe_get(latest, 'MACDs_12_26_9'),
            "macd_hist": _safe_get(latest, 'MACDh_12_26_9'),
            "rsi_14": _safe_get(latest, 'RSI_14'),
            "kdj_k": _safe_get(latest, 'K_9_3'),
            "kdj_d": _safe_get(latest, 'D_9_3'),
            "kdj_j": _safe_get(latest, 'J_9_3')
        },
        "volatility": {
            "boll_upper": _safe_get(latest, col_bbu) if col_bbu else None,
            "boll_mid": _safe_get(latest, col_bbm) if col_bbm else None,
            "boll_lower": _safe_get(latest, col_bbl) if col_bbl else None,
            "atr_14": _safe_get(latest, _get_dynamic_col(df, 'ATR'))
        }
    }

    # 仅日线级别才有多因子风险评估 / 大盘相关性（周线/月线数据点不足，不计算）
    if cycle_risk_block is not None:
        result["cycle_risk"] = cycle_risk_block
    if market_correlation:
        result["market_correlation"] = market_correlation

    return result

def _load_index_data() -> dict:
    """
    统一加载港股大盘指数数据（只读一次磁盘）。

    返回:
        {"HSI": df_hsi, "HSTECH_3033": df_hstech, ...}
        如果某个指数文件不存在或数据不足，则不包含该 key。
    """
    index_candidates = [
        ("HSI", OHLCV_DIR / "INDEX_HSI_daily.csv"),
        ("HSTECH_3033", OHLCV_DIR / "INDEX_3033_HK_daily.csv"),
    ]

    loaded = {}
    for name, path in index_candidates:
        if path.exists():
            try:
                df_idx = pd.read_csv(path)
                if 'Date' in df_idx.columns and 'Close' in df_idx.columns:
                    df_idx['Date'] = pd.to_datetime(df_idx['Date'])
                    df_idx.set_index('Date', inplace=True)
                    df_idx.sort_index(inplace=True)
                    if len(df_idx) >= 60:
                        loaded[name] = df_idx
            except Exception:
                continue
    return loaded


def _calc_own_cycle(df_stock: pd.DataFrame, index_data: dict = None, lookback: int = 1260) -> dict:
    """
    剥离大盘周期后，计算公司自身周期系数。

    方法：
        1. 线性回归：R_stock = α + β·R_index + ε，取残差 ε
        2. 累计残差曲线 = 公司"纯净价格路径"（去掉大盘 β 贡献）
        3. 当前累计残差在历史中的百分位排名 → 周期系数

    结果解读：
        接近 0 → 公司自身处于历史最低谷（周期机会区）
        接近 1 → 公司自身处于历史最高峰（周期风险区）
        <0.05 为周期机会区，>0.95 为周期风险区

    参数:
        df_stock   : 个股日线 DataFrame，含 Close 列，DateTimeIndex
        index_data : 大盘指数字典 {"HSI": df, ...}，由 _load_index_data() 提供
        lookback   : 回溯窗口（交易日），默认 1260（约 5 年）

    返回:
        {
            "own_cycle_level": 0.32,
            "own_cycle_zone": "中性",
            "regression_beta": 1.15,
            "regression_alpha_annualized": 0.03,
            "residual_cumulative": -0.12,
            "index_used": "HSI"
        }
    """
    result = {
        "own_cycle_level": None,
        "own_cycle_zone": "数据不足",
        "regression_beta": None,
        "regression_alpha_annualized": None,
        "residual_cumulative": None,
        "index_used": None,
    }

    if df_stock is None or df_stock.empty or len(df_stock) < 120:
        return result

    # ------ 从预加载的 index_data 中取第一个可用指数 ------
    if index_data is None:
        index_data = _load_index_data()

    df_index = None
    index_name = None
    for name in ["HSI", "HSTECH_3033"]:
        if name in index_data and len(index_data[name]) >= 120:
            df_index = index_data[name]
            index_name = name
            break

    if df_index is None:
        return result

    # ------ 对齐日期，计算日收益率 ------
    stock_ret = df_stock['Close'].pct_change().replace([np.inf, -np.inf], np.nan)
    index_ret = df_index['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

    common_idx = stock_ret.dropna().index.intersection(index_ret.dropna().index)
    # 截取回溯窗口
    common_idx = common_idx[-min(lookback, len(common_idx)):]
    if len(common_idx) < 120:
        return result

    y = stock_ret.loc[common_idx].values  # 个股收益率
    x = index_ret.loc[common_idx].values  # 大盘收益率

    # 清洗残留 NaN / inf，防止污染 OLS
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    if len(x) < 120:
        return result

    # ------ Step 1: OLS 回归 R_stock = α + β·R_index + ε ------
    # 手动 OLS，避免引入 statsmodels 依赖（x/y 已清洗，无 NaN）
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    beta = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2)
    alpha = y_mean - beta * x_mean
    residuals = y - (alpha + beta * x)

    # ------ Step 2: 累计残差曲线 ------
    cum_residuals = np.cumsum(residuals)

    # ------ Step 3: 当前累计残差的历史百分位 ------
    current_cum = cum_residuals[-1]
    cum_series = pd.Series(cum_residuals)
    own_cycle_level = _percentile_rank_in_series(current_cum, cum_series)

    result["own_cycle_level"] = round(own_cycle_level, 4) if own_cycle_level is not None else None
    result["own_cycle_zone"] = _risk_zone_label(own_cycle_level, "cycle")
    result["regression_beta"] = round(float(beta), 4)
    result["regression_alpha_annualized"] = round(float(alpha * 252), 4)
    result["residual_cumulative"] = round(float(current_cum), 4)
    result["index_used"] = index_name

    return result


def _calc_market_correlation(df_stock: pd.DataFrame, index_data: dict = None, windows: list = None) -> dict:
    """
    计算个股与港股大盘指数的滚动皮尔逊相关系数。

    同时计算与所有可用指数的相关性（HSI + HSTECH），而非只取第一个。

    大盘指数来源：
        - INDEX_HSI_daily.csv（恒生指数）
        - INDEX_3033_HK_daily.csv（南方東英恒生科技指數 ETF）

    参数:
        df_stock   : 个股日线 DataFrame，含 Close 列，DateTimeIndex
        index_data : 大盘指数字典 {"HSI": df, ...}，由 _load_index_data() 提供
        windows    : 滚动窗口列表，默认 [250, 500]

    返回:
        {
            "HSI": {
                "correlation_250d": 0.84,
                "correlation_500d": 0.83,
                "correlation_trend_60d": {"start": 0.85, "end": 0.84, "delta": -0.01},
                "interpretation": "高度正相关 (0.84)"
            },
            "HSTECH_3033": {
                "correlation_250d": 0.91,
                ...
            }
        }
        如果无法计算，返回空字典。
    """
    if windows is None:
        windows = [250, 500]

    if df_stock is None or df_stock.empty or len(df_stock) < 60:
        return {}

    # ------ 使用预加载的 index_data ------
    if index_data is None:
        index_data = _load_index_data()

    if not index_data:
        return {}

    stock_ret = df_stock['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

    # ------ 逐指数计算 ------
    result = {}
    for index_name, df_index in index_data.items():
        index_ret = df_index['Close'].pct_change().replace([np.inf, -np.inf], np.nan)

        common_idx = stock_ret.dropna().index.intersection(index_ret.dropna().index)
        if len(common_idx) < 60:
            continue

        s_ret = stock_ret.loc[common_idx]
        i_ret = index_ret.loc[common_idx]

        entry = {}

        # 多窗口滚动相关系数
        for w in windows:
            if len(common_idx) < w:
                corr_val = float(s_ret.corr(i_ret))
                entry[f"correlation_{w}d"] = round(corr_val, 4) if pd.notna(corr_val) else None
            else:
                rolling_corr = s_ret.rolling(w, min_periods=int(w * 0.7)).corr(i_ret)
                latest_corr = rolling_corr.dropna().iloc[-1] if not rolling_corr.dropna().empty else None
                entry[f"correlation_{w}d"] = round(float(latest_corr), 4) if latest_corr is not None else None

        # 近 60 日趋势（压缩为统计量）
        if len(common_idx) >= 250:
            rolling_250 = s_ret.rolling(250, min_periods=175).corr(i_ret)
            trend = rolling_250.tail(60).dropna()
            if len(trend) >= 2:
                entry["correlation_trend_60d"] = {
                    "start": round(float(trend.iloc[0]), 4),
                    "end": round(float(trend.iloc[-1]), 4),
                    "delta": round(float(trend.iloc[-1] - trend.iloc[0]), 4),
                }

        # 相关性解读
        primary_corr = entry.get(f"correlation_{windows[0]}d")
        if primary_corr is not None:
            abs_corr = abs(primary_corr)
            if abs_corr >= 0.8:
                strength = "高度"
            elif abs_corr >= 0.5:
                strength = "中度"
            elif abs_corr >= 0.3:
                strength = "低度"
            else:
                strength = "极弱"
            direction = "正相关" if primary_corr >= 0 else "负相关"
            entry["interpretation"] = f"{strength}{direction} ({primary_corr:.2f})"

        result[index_name] = entry

    return result


# ==========================================
# 主调用函数
# ==========================================

def load_financial_series(ticker_symbol: str, financial_dir: Path = None) -> tuple:
    """
    从三表财报 CSV 中提取逐期 EPS 和 BVPS 序列。

    文件命名约定：
        {ticker}_quarterly_income.csv   → Basic EPS, Net Income
        {ticker}_annual_income.csv      → (fallback)
        {ticker}_quarterly_balance.csv  → Stockholders Equity
        {ticker}_annual_balance.csv     → (fallback)

    BVPS 推算逻辑：
        implied_shares = Net Income / Basic EPS
        BVPS = Stockholders Equity / implied_shares

    参数:
        ticker_symbol : 股票代码，如 "0700.HK"
        financial_dir : 财报 CSV 所在目录，默认为 OHLCV_DIR 同级的 financials/

    返回:
        (eps_series, bvps_series) — 两个以 Date 为索引的 pd.Series
        如果找不到文件或解析失败，返回 (None, None)
    """
    if financial_dir is None:
        financial_dir = OHLCV_DIR.parent / "financials"

    ticker_fs = ticker_symbol

    # ------ 辅助：读取单个 CSV ------
    def _read_fin_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path)
            if df.empty or 'Date' not in df.columns:
                return None
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e:
            print(f"⚠️ 读取 {path} 失败: {e}")
            return None

    # ------ 1. 加载 income 表 (优先季报，fallback 年报) ------
    income_df = _read_fin_csv(financial_dir / f"{ticker_fs}_quarterly_income.csv")
    if income_df is None:
        income_df = _read_fin_csv(financial_dir / f"{ticker_fs}_annual_income.csv")

    # ------ 2. 加载 balance 表 ------
    balance_df = _read_fin_csv(financial_dir / f"{ticker_fs}_quarterly_balance.csv")
    if balance_df is None:
        balance_df = _read_fin_csv(financial_dir / f"{ticker_fs}_annual_balance.csv")

    # ------ 3. 提取 EPS ------
    eps_series = None
    if income_df is not None:
        for col in ['Basic EPS', 'Diluted EPS']:
            if col in income_df.columns:
                s = pd.to_numeric(income_df[col], errors='coerce').dropna()
                if not s.empty:
                    eps_series = s
                    break

    # ------ 4. 推算 BVPS ------
    bvps_series = None
    if balance_df is not None and income_df is not None:
        equity_col = None
        for col in ['Stockholders Equity', 'Total Equity Gross Minority Interest']:
            if col in balance_df.columns:
                equity_col = col
                break

        if equity_col and 'Basic EPS' in income_df.columns and 'Net Income' in income_df.columns:
            eps_raw = pd.to_numeric(income_df['Basic EPS'], errors='coerce')
            ni_raw = pd.to_numeric(income_df['Net Income'], errors='coerce')
            equity_raw = pd.to_numeric(balance_df[equity_col], errors='coerce')

            # 推算股本数 = Net Income / Basic EPS (取交集日期)
            common_idx = eps_raw.dropna().index.intersection(ni_raw.dropna().index)
            if len(common_idx) > 0:
                shares = (ni_raw.loc[common_idx] / eps_raw.loc[common_idx].replace(0, np.nan)).dropna()
                # 取最新的股本数，向前填充用于整段历史
                shares_full = shares.reindex(balance_df.index).ffill().bfill()
                bvps = (equity_raw / shares_full.replace(0, np.nan)).dropna()
                if not bvps.empty:
                    bvps_series = bvps

    if eps_series is not None:
        print(f"  ✅ EPS: {len(eps_series)} 期 ({eps_series.index.min().date()} ~ {eps_series.index.max().date()})")
    if bvps_series is not None:
        print(f"  ✅ BVPS: {len(bvps_series)} 期 ({bvps_series.index.min().date()} ~ {bvps_series.index.max().date()})")

    return eps_series, bvps_series


def generate_technical_analysis(
    ticker_symbol: str,
    eps_series: pd.Series = None,
    bvps_series: pd.Series = None,
    financial_dir: Path = None,
) -> dict:
    """
    读取生数据，重采样日、周、月线，返回完美的 technical_analysis 字典。

    多因子风险评估升级说明：
        - 长线风险水平：5因子(估值/动量/波动率/技术/资金)加权合成 → 历史百分位归一化
          阈值: <0.05 机会区, >0.95 风险区
        - 短线风险水平：同框架，短窗口因子
          阈值: <0.01 短线机会点, >0.99 短线风险点

    参数:
        ticker_symbol : 股票代码，如 "0700.HK"
        eps_series    : 逐期 EPS 序列（可选）。若不传，函数会尝试从 financial_dir 自动加载
        bvps_series   : 逐期 BVPS 序列（可选）
        financial_dir : 财报 CSV 目录（可选）
    """
    file_path = OHLCV_DIR / f"{ticker_symbol}_daily.csv"

    if not file_path.exists():
        print(f"⚠️ 找不到 {ticker_symbol} 的量价数据: {file_path}")
        return {}

    print(f"⚙️ 正在计算 {ticker_symbol} 的多周期技术指标 + 多因子风险评估...")

    # 0. 尝试加载财报数据（EPS / BVPS）以提升估值因子精度
    if eps_series is None and bvps_series is None:
        eps_series, bvps_series = load_financial_series(ticker_symbol, financial_dir)
        if eps_series is not None:
            print(f"  📊 已加载 {ticker_symbol} 财报数据 (EPS/BVPS)，估值因子将使用 PE/PB")
        else:
            print(f"  📊 未找到 {ticker_symbol} 财报数据，估值因子将使用价格偏离度代理")

    # 1. 读取日线数据，并把 Date 设为 DateTimeIndex (重采样必须的前置条件)
    df_daily = pd.read_csv(file_path)
    df_daily['Date'] = pd.to_datetime(df_daily['Date'])
    df_daily.set_index('Date', inplace=True)
    df_daily.sort_index(ascending=True, inplace=True)

    # 2. 计算日线指标
    df_daily = _add_technical_indicators(df_daily)

    # 2.5 加载大盘指数数据（统一加载一次，避免重复 IO）
    index_data = _load_index_data()
    if index_data:
        print(f"  📊 已加载大盘指数: {', '.join(index_data.keys())}")

    # 2.6 计算与大盘相关性（基于日收益率，只在日线级别有意义）
    mkt_corr = _calc_market_correlation(df_daily, index_data=index_data)
    if mkt_corr:
        for idx_name, idx_data in mkt_corr.items():
            print(f"  📈 大盘相关性 ({idx_name}): {idx_data.get('interpretation', 'N/A')}")

    # 2.7 计算剥离大盘后公司自身周期系数
    own_cyc = _calc_own_cycle(df_daily, index_data=index_data)
    if own_cyc.get("own_cycle_level") is not None:
        print(f"  🔄 自身周期系数: {own_cyc['own_cycle_level']} ({own_cyc['own_cycle_zone']}), β={own_cyc['regression_beta']}")

    # 2.8 多因子风险评估（只在日线级别计算，周线/月线数据点不足）
    long_risk = calc_multifactor_risk(
        df_daily, term="long", eps_series=eps_series, bvps_series=bvps_series
    )
    short_risk = calc_multifactor_risk(
        df_daily, term="short", eps_series=eps_series, bvps_series=bvps_series
    )
    long_level = long_risk["risk_level"]
    short_level = short_risk["risk_level"]
    print(f"  📊 长线风险: {long_level} ({long_risk['risk_zone']}), 短线风险: {short_level} ({short_risk['risk_zone']})")

    # 拼装 cycle_risk 块
    price_pct_5y = _calc_price_percentile_rank(df_daily, lookback_days=1260)
    price_pct_60d = _calc_price_percentile_rank(df_daily, lookback_days=60)
    cycle_risk_block = {
        "long_term_risk_level": long_level,
        "long_term_risk_zone": long_risk["risk_zone"],
        "long_term_composite_raw": long_risk["composite_raw"],
        "long_term_factors": long_risk["factors"],
        "long_term_weights": long_risk["weights_used"],
        "long_term_data_quality": long_risk["data_quality"],
        "investment_win_rate": round(1 - long_level, 4) if long_level is not None else None,
        "short_term_risk_level": short_level,
        "short_term_risk_zone": short_risk["risk_zone"],
        "short_term_composite_raw": short_risk["composite_raw"],
        "short_term_factors": short_risk["factors"],
        "short_term_weights": short_risk["weights_used"],
        "short_term_data_quality": short_risk["data_quality"],
        "price_percentile_5y": price_pct_5y,
        "price_percentile_60d": price_pct_60d,
        "multi_timeframe_resonance": _assess_resonance(long_level, short_level),
        "own_cycle": own_cyc if own_cyc else {},
    }

    daily_features = _extract_latest_features(
        df_daily, cycle_risk_block=cycle_risk_block, market_correlation=mkt_corr,
    )

    # 3. 重采样计算周线 (Weekly - 以周五为界)
    agg_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'Turnover_Value': 'sum'
    }
    df_weekly = df_daily.resample('W-FRI').agg(agg_dict).dropna(subset=['Close'])
    df_weekly = _add_technical_indicators(df_weekly)
    weekly_features = _extract_latest_features(df_weekly)

    # 4. 重采样计算月线 (Monthly - 以月末为界)
    df_monthly = df_daily.resample('ME').agg(agg_dict).dropna(subset=['Close'])
    df_monthly = _add_technical_indicators(df_monthly)
    monthly_features = _extract_latest_features(df_monthly)

    # 5. 拼装成终极结构
    technical_analysis = {
        "daily": daily_features,
        "weekly": weekly_features,
        "monthly": monthly_features
    }

    print(f"✅ {ticker_symbol} 多周期技术面指标 + 多因子风险评估计算完成！")
    return technical_analysis

# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    import json

    test_ticker = "0700.HK"
    tech_data = generate_technical_analysis(test_ticker)

    # 漂亮地打印出最终的 JSON 结构
    if tech_data:
        print("\n最终输出的 JSON 结构片段:")
        print(json.dumps(tech_data, indent=4, ensure_ascii=False))
