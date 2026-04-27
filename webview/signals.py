from __future__ import annotations

from typing import Any

Tone = str  # "good" | "bad" | "neutral"


def _badge(label: str, value: str, tone: Tone) -> dict[str, str]:
    return {"label": label, "value": value, "tone": tone}


_MA_ALIGN = {
    "bullish": ("多头排列", "good"),
    "bearish": ("空头排列", "bad"),
    "mixed": ("均线纠缠", "neutral"),
}
_RSI_ZONE = {
    "oversold": ("超卖", "good"),
    "overbought": ("超买", "bad"),
    "neutral": ("中性", "neutral"),
}
_KDJ_ZONE = {
    "oversold": ("超卖", "good"),
    "overbought": ("超买", "bad"),
    "neutral": ("中性", "neutral"),
}
_BOLL_POS = {
    "within_bands": ("带内运行", "neutral"),
    "above_upper": ("突破上轨", "bad"),
    "below_lower": ("跌破下轨", "good"),
}
_MACD_CROSS = {
    "golden": ("金叉", "good"),
    "dead": ("死叉", "bad"),
    "none": ("无交叉", "neutral"),
}


def _pct(x: float | None, decimals: int = 2) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def _num(x: float | None, decimals: int = 2) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


def format_trend(payload: dict | None, timeframe: str = "daily") -> dict[str, Any]:
    if not payload:
        return {"available": False, "groups": []}
    tech = (payload.get("technicals") or {}).get(timeframe) or {}
    if not tech:
        return {"available": False, "groups": []}

    ts: dict[str, Any] = tech.get("trend_signals") or {}
    rm: dict[str, Any] = tech.get("risk_metrics") or {}
    rt: dict[str, Any] = tech.get("recent_trend") or {}
    mom: dict[str, Any] = tech.get("momentum") or {}
    mc: dict[str, Any] = tech.get("market_correlation") or {}
    tr: dict[str, Any] = tech.get("trend") or {}
    vol: dict[str, Any] = tech.get("volatility") or {}
    last_close = tech.get("close")

    trend_badges: list[dict[str, str]] = []
    if "ma_alignment" in ts:
        v, tone = _MA_ALIGN.get(ts["ma_alignment"], (str(ts["ma_alignment"]), "neutral"))
        trend_badges.append(_badge("均线形态", v, tone))

    # MA 上方/下方：MA5/20/30/60/250。优先用 trend_signals 里的布尔，缺失时用 close vs trend.maN 推断。
    ma_specs = (
        ("MA5", "above_ma5", "ma5"),
        ("MA20", "above_ma20", "ma20"),
        ("MA30", "above_ma30", "ma30"),
        ("MA60", "above_ma60", "ma60"),
        ("MA250", "above_ma250", "ma250"),
    )
    for label, ts_key, ma_key in ma_specs:
        above: bool | None = None
        if ts_key in ts:
            above = bool(ts[ts_key])
        elif last_close is not None and tr.get(ma_key) is not None:
            try:
                above = float(last_close) > float(tr[ma_key])
            except (TypeError, ValueError):
                above = None
        if above is not None:
            trend_badges.append(_badge(label, "上方" if above else "下方", "good" if above else "bad"))
    if "macd_cross" in ts:
        v, tone = _MACD_CROSS.get(ts["macd_cross"], (str(ts["macd_cross"]), "neutral"))
        trend_badges.append(_badge("MACD 交叉", v, tone))
    if "macd_above_zero" in ts:
        above = bool(ts["macd_above_zero"])
        trend_badges.append(_badge("MACD 零轴", "上方" if above else "下方", "good" if above else "bad"))
    if "rsi_zone" in ts:
        v, tone = _RSI_ZONE.get(ts["rsi_zone"], (str(ts["rsi_zone"]), "neutral"))
        trend_badges.append(_badge("RSI 区间", v, tone))
    if "kdj_zone" in ts:
        v, tone = _KDJ_ZONE.get(ts["kdj_zone"], (str(ts["kdj_zone"]), "neutral"))
        trend_badges.append(_badge("KDJ 区间", v, tone))
    if "boll_position" in ts:
        v, tone = _BOLL_POS.get(ts["boll_position"], (str(ts["boll_position"]), "neutral"))
        trend_badges.append(_badge("BOLL 位置", v, tone))
    if "boll_upper" in vol:
        trend_badges.append(_badge("BOLL 上轨", _num(vol["boll_upper"]), "neutral"))
    if "boll_mid" in vol:
        trend_badges.append(_badge("BOLL 中轨", _num(vol["boll_mid"]), "neutral"))
    if "boll_lower" in vol:
        trend_badges.append(_badge("BOLL 下轨", _num(vol["boll_lower"]), "neutral"))

    momentum_badges: list[dict[str, str]] = []
    if "rsi_14" in mom:
        rsi = mom["rsi_14"]
        tone = "bad" if rsi >= 70 else ("good" if rsi <= 30 else "neutral")
        momentum_badges.append(_badge("RSI(14)", _num(rsi, 1), tone))
    if "macd_dif" in mom:
        momentum_badges.append(_badge("MACD DIF", _num(mom["macd_dif"], 3), "neutral"))
    if "macd_dea" in mom:
        momentum_badges.append(_badge("MACD DEA", _num(mom["macd_dea"], 3), "neutral"))
    if "macd_hist" in mom:
        h = mom["macd_hist"]
        momentum_badges.append(_badge("MACD HIST", _num(h, 3), "good" if h > 0 else "bad"))

    risk_badges: list[dict[str, str]] = []
    if "high_52w" in rm:
        risk_badges.append(_badge("52周高", _num(rm["high_52w"]), "neutral"))
    if "low_52w" in rm:
        risk_badges.append(_badge("52周低", _num(rm["low_52w"]), "neutral"))
    if "price_position_52w_ratio" in rm:
        p = rm["price_position_52w_ratio"]
        tone = "good" if p >= 0.7 else ("bad" if p <= 0.3 else "neutral")
        risk_badges.append(_badge("52周分位", _pct(p, 1), tone))
    if "price_position_3y_ratio" in rm:
        risk_badges.append(_badge("3年分位", _pct(rm["price_position_3y_ratio"], 1), "neutral"))
    if "sharpe_ratio_1y" in rm:
        s = rm["sharpe_ratio_1y"]
        tone = "good" if s >= 1 else ("bad" if s < 0 else "neutral")
        risk_badges.append(_badge("夏普(1Y)", _num(s), tone))
    if "max_drawdown_1y_ratio" in rm:
        dd = rm["max_drawdown_1y_ratio"]
        tone = "bad" if dd <= -0.2 else "neutral"
        risk_badges.append(_badge("最大回撤(1Y)", _pct(dd, 1), tone))

    recent_badges: list[dict[str, str]] = []
    if "period" in rt:
        recent_badges.append(_badge("近期区间", str(rt["period"]), "neutral"))
    if "change_pct" in rt:
        cp = rt["change_pct"]
        recent_badges.append(_badge("区间涨跌", _pct(cp, 2), "good" if cp > 0 else ("bad" if cp < 0 else "neutral")))
    if "close" in rt:
        recent_badges.append(_badge("最新收盘", _num(rt["close"]), "neutral"))

    corr_badges: list[dict[str, str]] = []
    for index_name, friendly in (("HSI", "恒指"), ("HSTECH_3033", "恒科")):
        block = mc.get(index_name) if isinstance(mc, dict) else None
        if not isinstance(block, dict):
            continue
        if "correlation_250d" in block:
            corr_badges.append(_badge(f"{friendly} 250d 相关", _num(block["correlation_250d"], 3), "neutral"))
        trend60 = block.get("correlation_trend_60d") or {}
        if "delta" in trend60:
            d = trend60["delta"]
            corr_badges.append(_badge(f"{friendly} 60d Δ", _num(d, 3), "good" if d > 0 else ("bad" if d < 0 else "neutral")))

    groups = []
    if recent_badges:
        groups.append({"title": "近期表现", "badges": recent_badges})
    if trend_badges:
        groups.append({"title": "趋势信号", "badges": trend_badges})
    if momentum_badges:
        groups.append({"title": "动量指标", "badges": momentum_badges})
    if risk_badges:
        groups.append({"title": "风险/位置", "badges": risk_badges})
    if corr_badges:
        groups.append({"title": "大盘相关性", "badges": corr_badges})

    meta = payload.get("meta") or {}
    return {
        "available": True,
        "groups": groups,
        "ticker": meta.get("ticker"),
        "generation_date": meta.get("generation_date"),
        "timeframe": timeframe,
    }
