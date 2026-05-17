"""
technical_multifactor.py — 多因子风险评估系统模块

包含内容：
    向量化批量计算：
        - _calc_all_factor_series     : 一次 pass 计算所有 5 个因子的滚动百分位时间序列
                                         (估值/动量/波动率/技术/资金)

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
    # 方向语义：量比高 + 价量正相关 = 主力推动 = 机会信号 → 取 (1 - rank)
    # 与其它因子统一为「百分位高 = 风险高」的方向，否则放量主力建仓会被误判为风险。
    cap_ranks = []
    ma_vol = df['Volume'].rolling(20, min_periods=5).mean()
    vol_ratio = df['Volume'] / ma_vol.replace(0, np.nan)
    r = _rolling_percentile_rank(vol_ratio, cap_window)
    if r.dropna().shape[0] >= 20:
        cap_ranks.append(1 - r)
    price_ret = df['Close'].pct_change()
    vol_ret = df['Volume'].pct_change()
    corr_rolling = price_ret.rolling(10, min_periods=5).corr(vol_ret)
    r = _rolling_percentile_rank(corr_rolling, cap_window)
    if r.dropna().shape[0] >= 20:
        cap_ranks.append(1 - r)
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
    from technical_indicators import _add_technical_indicators
    from technical_financial import load_financial_series

    test_ticker = "0700.HK"
    file_path = OHLCV_DIR / f"{test_ticker}_daily.csv"

    if not file_path.exists():
        print(f"⚠️ 找不到 {test_ticker} 的量价数据: {file_path}")
    else:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(ascending=True, inplace=True)
        df = _add_technical_indicators(df)

        eps_series, bvps_series = load_financial_series(test_ticker)

        print(f"⚙️ 正在计算 {test_ticker} 的多因子风险评估...")

        long_risk = calc_multifactor_risk(df, term="long", eps_series=eps_series, bvps_series=bvps_series)
        short_risk = calc_multifactor_risk(df, term="short", eps_series=eps_series, bvps_series=bvps_series)

        print("\n长线风险评估:")
        print(json.dumps(long_risk, indent=4, ensure_ascii=False))
        print("\n短线风险评估:")
        print(json.dumps(short_risk, indent=4, ensure_ascii=False))
