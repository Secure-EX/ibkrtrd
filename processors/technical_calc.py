"""
technical_calc.py — 主入口模块（特征提取 + 多周期分析流程编排）

包含内容：
    - _extract_latest_features   : 提取 DataFrame 最后一行，拼装成目标 JSON Schema 结构
          涵盖：收盘价/成交量/VWAP、风控指标、趋势研判信号、近期走势摘要、
          均线系统、MACD/RSI/KDJ、布林带/ATR
          仅日线传入 cycle_risk_block 和 market_correlation
    - generate_technical_analysis: 主调用入口
          流程：读取日线 CSV → 加技术指标 → 加载大盘指数 → 计算大盘相关性 →
          计算自身周期 → 多因子风险评估（长/短线）→ 重采样周/月线 → 拼装结构体

子模块依赖关系：
    technical_utils       ← 被所有子模块引用
    technical_indicators  ← 技术指标计算
    technical_risk        ← 风控指标 + 多周期共振
    technical_multifactor ← 多因子风险评估
    technical_market      ← 大盘指数加载 + 自身周期 + 相关性
    technical_financial   ← 财报 EPS/BVPS 加载
"""

import sys
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import OHLCV_DIR

try:
    from .technical_utils import _safe_get, _get_dynamic_col, RESAMPLE_AGG, RESAMPLE_RULES
    from .technical_indicators import (
        _add_technical_indicators,
        _calc_price_percentile_rank,
        _calc_trend_signals,
    )
    from .technical_risk import _calc_1y_risk_metrics, _assess_resonance
    from .technical_multifactor import calc_multifactor_risk
    from .technical_market import _load_index_data, _calc_own_cycle, _calc_market_correlation
    from .technical_financial import load_financial_series
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from technical_utils import _safe_get, _get_dynamic_col, RESAMPLE_AGG, RESAMPLE_RULES
    from technical_indicators import (
        _add_technical_indicators,
        _calc_price_percentile_rank,
        _calc_trend_signals,
    )
    from technical_risk import _calc_1y_risk_metrics, _assess_resonance
    from technical_multifactor import calc_multifactor_risk
    from technical_market import _load_index_data, _calc_own_cycle, _calc_market_correlation
    from technical_financial import load_financial_series


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
        # 量比定义：当日成交量 / 前 20 日均量（不含今日，避免异常放量自我稀释）
        "volume_ratio_20d": (
            float(latest['Volume'] / df['Volume'].iloc[-21:-1].mean())
            if len(df) >= 21 and df['Volume'].iloc[-21:-1].mean() > 0
            else None
        ),
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


def generate_technical_analysis(
    ticker_symbol: str,
    eps_series: pd.Series = None,
    bvps_series: pd.Series = None,
    financial_dir: Path = None,
) -> dict:
    """
    读取生数据，重采样日、周、月线，返回完整的 technical_analysis 字典。

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
    df_weekly = df_daily.resample(RESAMPLE_RULES['weekly']).agg(RESAMPLE_AGG).dropna(subset=['Close'])
    df_weekly = _add_technical_indicators(df_weekly)
    weekly_features = _extract_latest_features(df_weekly)

    # 4. 重采样计算月线 (Monthly - 以月末为界)
    df_monthly = df_daily.resample(RESAMPLE_RULES['monthly']).agg(RESAMPLE_AGG).dropna(subset=['Close'])
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
