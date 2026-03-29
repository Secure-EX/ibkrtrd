"""
technical_multifactor.py — 多因子风险评估系统模块

包含内容：
    子因子计算（各自独立，返回当前值在历史中的百分位 0~1）：
        - _calc_valuation_factor      : 估值因子（PE/PB 历史百分位；无财报时退化为价格偏离度）
        - _calc_momentum_factor       : 动量因子（多周期涨跌幅历史百分位）
        - _calc_volatility_factor_long: 长线波动率因子（20日年化波动率百分位）
        - _calc_volatility_factor_short: 短线波动率因子（ATR/日内波幅比率百分位）
        - _calc_technical_factor      : 技术因子（RSI + KDJ-J 历史百分位均值）
        - _calc_capital_flow_factor   : 资金因子（量比 + 价量相关性百分位均值）

    向量化批量计算：
        - _calc_all_factor_series     : 一次 pass 计算所有因子的滚动百分位时间序列

    主入口：
        - calc_multifactor_risk       : 5因子加权合成 → 历史百分位归一化 → 风险水平(0~1)
          支持 term="long"（长线，侧重估值）和 term="short"（短线，侧重动量与技术）

依赖：technical_utils（_align_financial_to_daily, _percentile_rank_in_series,
                      _rolling_percentile_rank, _get_dynamic_col）
     technical_risk（_risk_zone_label）
"""

import pandas as pd
import numpy as np

try:
    from .technical_utils import (
        _align_financial_to_daily,
        _percentile_rank_in_series,
        _rolling_percentile_rank,
        _get_dynamic_col,
    )
    from .technical_risk import _risk_zone_label
except ImportError:
    from technical_utils import (
        _align_financial_to_daily,
        _percentile_rank_in_series,
        _rolling_percentile_rank,
        _get_dynamic_col,
    )
    from technical_risk import _risk_zone_label


# ==========================================
# 子因子计算函数
# ==========================================

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


# ==========================================
# 向量化批量计算
# ==========================================

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


# ==========================================
# 主入口
# ==========================================

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


