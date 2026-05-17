from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from markupsafe import Markup, escape

# Ensure project root on sys.path so `from config import ...` works when
# Flask reloads or when launched from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from webview import data_io, signals  # noqa: E402

VALID_RANGES = ("1Y", "3Y", "5Y", "All")
VALID_TFS = ("daily", "weekly", "monthly")

# Map raw pandas_ta column names → webview/JS friendly keys.
# Bollinger 列 (BBL_20_2.0_2.0 / BBM / BBU) 因 pandas_ta 后缀重复，按前缀匹配。
_TECH_RENAME = {
    "MACD_12_26_9": "macd",
    "MACDs_12_26_9": "macd_signal",
    "MACDh_12_26_9": "macd_hist",
    "RSI_14": "rsi",
    "K_9_3": "k",
    "D_9_3": "d",
    "J_9_3": "j",
    "ATRr_14": "atr",
    "VWAP_Custom": "vwap",
}

# 前端图表默认显示的 MA 周期（parquet 里 SMA_5/10/20/30/60/120/250 全部都有；这里挑显示 3 条）。
# 周/月线下 ma60 / ma250 已经覆盖很长跨度，无需另算 13/26/52。
_DEFAULT_MA_PERIODS = [20, 60, 250]


def _normalize_technical_columns(df: "pd.DataFrame") -> "pd.DataFrame":
    """Rename pandas_ta cols to JS-friendly names + match BB_* prefix variants."""
    if df.empty:
        return df
    rename = {k: v for k, v in _TECH_RENAME.items() if k in df.columns}
    # SMA_<n> → ma<n>
    for col in df.columns:
        if col.startswith("SMA_"):
            try:
                n = int(col.split("_", 1)[1])
                rename[col] = f"ma{n}"
            except (ValueError, IndexError):
                continue
    # BBL_*/BBM_*/BBU_* (后缀变化最大) → bb_lower/bb_mid/bb_upper
    for col in df.columns:
        if col.startswith("BBL_"): rename[col] = "bb_lower"
        elif col.startswith("BBM_"): rename[col] = "bb_mid"
        elif col.startswith("BBU_"): rename[col] = "bb_upper"
    return df.rename(columns=rename)


def _slice_range(df: "pd.DataFrame", choice: str) -> "pd.DataFrame":
    """Range-切片：基于索引最大日期回溯 N 年。All → 全量。"""
    if df.empty or choice == "All":
        return df
    end = df.index.max()
    years = {"1Y": 1, "3Y": 3, "5Y": 5}.get(choice)
    if years is None:
        return df
    start = end - pd.DateOffset(years=years)
    return df.loc[df.index >= start]


def _df_to_records(df) -> list[dict]:
    df = df.reset_index()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df = df.replace({np.nan: None})
    return df.to_dict(orient="records")


def _build_position_meta_html(position: dict | None, generation_date: str | None) -> Markup | None:
    """Compose the sample-style position meta line for the chart page sticky head."""
    if not position:
        return None
    qty = position.get("qty") or 0
    cost = position.get("avg_price") or 0
    last = position.get("last") or 0
    pnl = position.get("unrealized_pnl") or 0
    pnl_pct = position.get("unrealized_pnl_ratio") or 0
    weight = position.get("weight_ratio") or 0
    sign = "+" if pnl_pct >= 0 else "−"
    cls = "up" if pnl_pct >= 0 else "down"
    parts = []
    if generation_date:
        parts.append(f"生成时间: <b>{escape(generation_date)}</b>")
    parts.append(f"当前价: <b>HKD {last:.2f}</b>")
    parts.append(f"持仓: <b>{int(qty):,} 股 @ 均价 HKD {cost:.2f}</b>")
    parts.append(
        f'盈亏 <b class="{cls}">HKD {pnl:,.2f} ({sign}{abs(pnl_pct) * 100:.2f}%)</b>'
    )
    parts.append(f"占组合 {weight * 100:.2f}%")
    sep = '<span class="sep">|</span>'
    return Markup(f' {sep} '.join(parts))


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

    @app.context_processor
    def inject_globals():
        return {
            "latest_date": data_io.latest_data_date(),
        }

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            active_nav="home",
            reports=data_io.list_reports(),
            tickers=data_io.list_tickers(),
            positions=data_io.load_positions(),
            summary=(data_io.load_portfolio_summary() or {}).get("portfolio_summary"),
            parse_date=data_io.parse_report_date,
        )

    @app.route("/reports")
    def reports():
        files = data_io.list_reports()
        if not files:
            return render_template(
                "reports.html",
                active_nav="reports",
                files=[],
                current=None,
                html_body=None,
                parse_date=data_io.parse_report_date,
            )
        requested = request.args.get("file")
        current = None
        if requested:
            for f in files:
                if f.name == requested:
                    current = f
                    break
        if current is None:
            current = files[0]
        md_text = data_io.read_report_md(current)
        html_body = data_io.render_markdown(md_text)
        return render_template(
            "reports.html",
            active_nav="reports",
            files=files,
            current=current,
            html_body=html_body,
            parse_date=data_io.parse_report_date,
        )

    @app.route("/charts")
    def charts_index():
        tickers = data_io.list_tickers()
        if not tickers:
            return render_template(
                "chart.html",
                active_nav="charts",
                tickers=[],
                ticker=None,
                trend=None,
            )
        return redirect(url_for("chart_page", ticker=tickers[0]))

    @app.route("/charts/<ticker>")
    def chart_page(ticker: str):
        tickers = data_io.list_tickers()
        if ticker not in tickers:
            abort(404)
        payload = data_io.load_payload(ticker)
        trend = signals.format_trend(payload, timeframe="daily")

        stage1 = data_io.load_stage1_full(ticker)
        if stage1 is None:
            stage1_html = None
            stage1_source = None
            stage1_title = None
        else:
            md_text, src_path = stage1
            title, _, body = data_io.split_stage1_head(md_text)
            stage1_title = title
            stage1_html = data_io.render_markdown(body)
            stage1_source = src_path.name

        position = data_io.get_position(ticker)
        generation_date = (payload or {}).get("meta", {}).get("generation_date")
        position_meta_html = _build_position_meta_html(position, generation_date)

        return render_template(
            "chart.html",
            active_nav="charts",
            tickers=tickers,
            ticker=ticker,
            trend=trend,
            stage1_html=stage1_html,
            stage1_source=stage1_source,
            stage1_title=stage1_title,
            position_meta_html=position_meta_html,
        )

    @app.route("/api/ohlcv/<ticker>")
    def api_ohlcv(ticker: str):
        if ticker not in data_io.list_tickers():
            return jsonify({"error": "unknown ticker"}), 404
        rng = request.args.get("range", "1Y")
        if rng not in VALID_RANGES:
            rng = "1Y"
        tf = request.args.get("tf", "daily")
        if tf not in VALID_TFS:
            tf = "daily"

        df = data_io.load_technical(ticker, tf)
        df = _slice_range(df, rng)
        df = _normalize_technical_columns(df)

        ma_periods = _DEFAULT_MA_PERIODS
        if df.empty:
            trades = []
        else:
            trades = data_io.get_trades(
                ticker,
                start=df.index.min().to_pydatetime(),
                end=df.index.max().to_pydatetime(),
            )
        return jsonify(
            {
                "ticker": ticker,
                "range": rng,
                "timeframe": tf,
                "ma_periods": ma_periods,
                "rows": _df_to_records(df),
                "trades": trades,
            }
        )

    @app.route("/api/pe/<ticker>")
    def api_pe(ticker: str):
        if ticker not in data_io.list_tickers():
            return jsonify({"error": "unknown ticker"}), 404
        rng = request.args.get("range", "1Y")
        if rng not in VALID_RANGES:
            rng = "1Y"
        full = data_io.load_valuation(ticker)
        if full.empty or "PE_TTM" not in full.columns:
            return jsonify({"available": False, "ticker": ticker, "range": rng, "rows": [], "summary": {}})

        sliced = _slice_range(full, rng).dropna(subset=["PE_TTM"])
        if sliced.empty:
            return jsonify({"available": False, "ticker": ticker, "range": rng, "rows": [], "summary": {}})

        rows = [
            {
                "date": d.strftime("%Y-%m-%d"),
                "close": float(c) if pd.notna(c) else None,
                "ttm_eps": float(e) if pd.notna(e) else None,
                "pe": float(p) if pd.notna(p) else None,
            }
            for d, c, e, p in sliced[["Close", "EPS_TTM", "PE_TTM"]].itertuples()
        ]
        arr = sliced["PE_TTM"].to_numpy(dtype=float)
        cur_pe = float(arr[-1])
        summary = {
            "current": cur_pe,
            "percentile": float((arr <= cur_pe).mean() * 100.0),
            "p25": float(np.percentile(arr, 25)),
            "p50": float(np.percentile(arr, 50)),
            "p75": float(np.percentile(arr, 75)),
            "min": float(arr.min()),
            "max": float(arr.max()),
            "count": int(arr.size),
        }

        # === 当前快照对比（自算 vs yfinance）===
        last = sliced.iloc[-1]
        current_price = float(last["Close"])
        our_eps_ttm = float(last["EPS_TTM"]) if pd.notna(last["EPS_TTM"]) else None
        info = data_io.load_company_info(ticker)
        yf_pe = info.get("trailingPE")
        yf_eps = info.get("trailingEps")
        yf_pe = float(yf_pe) if yf_pe else None
        yf_eps = float(yf_eps) if yf_eps else None

        def _percentile_of(pe_value: float | None) -> float | None:
            if pe_value is None or not np.isfinite(pe_value) or pe_value <= 0:
                return None
            return float((arr <= pe_value).mean() * 100.0)

        current = {
            "price": current_price,
            "our_eps_ttm": our_eps_ttm,
            "our_pe": cur_pe,
            "our_percentile": summary["percentile"],
            "yf_eps_ttm": yf_eps,
            "yf_pe": yf_pe,
            "yf_percentile": _percentile_of(yf_pe),
        }

        # === 价格情景表：±30/20/10% ===
        scenarios = []
        for pct in [-30, -20, -10, 0, 10, 20, 30]:
            scen_price = current_price * (1 + pct / 100.0)
            our_scen_pe = scen_price / our_eps_ttm if our_eps_ttm and our_eps_ttm > 0 else None
            yf_scen_pe = scen_price / yf_eps if yf_eps and yf_eps > 0 else None
            scenarios.append({
                "pct": pct,
                "price": scen_price,
                "our_pe": our_scen_pe,
                "our_percentile": _percentile_of(our_scen_pe),
                "yf_pe": yf_scen_pe,
            })

        # === 反向目标表：历史 P10/P25/P50/P75/P90 PE → 对应目标价 ===
        targets = []
        for label, pct_q in [("P10", 10), ("P25", 25), ("P50", 50), ("P75", 75), ("P90", 90)]:
            target_pe = float(np.percentile(arr, pct_q))
            req_price = target_pe * our_eps_ttm if our_eps_ttm and our_eps_ttm > 0 else None
            pct_change = ((req_price - current_price) / current_price * 100.0) if req_price else None
            targets.append({
                "label": label,
                "target_pe": target_pe,
                "required_price": req_price,
                "pct_change": pct_change,
            })

        return jsonify(
            {
                "available": True,
                "ticker": ticker,
                "range": rng,
                "rows": rows,
                "summary": summary,
                "current": current,
                "scenarios": scenarios,
                "targets": targets,
            }
        )

    @app.route("/api/signals/<ticker>")
    def api_signals(ticker: str):
        tf = request.args.get("tf", "daily")
        if tf not in VALID_TFS:
            tf = "daily"
        payload = data_io.load_payload(ticker)
        return jsonify(signals.format_trend(payload, timeframe=tf))

    @app.route("/admin/clear-cache", methods=["POST", "GET"])
    def clear_cache():
        data_io.clear_caches()
        return jsonify({"ok": True})

    return app


if __name__ == "__main__":
    create_app().run(host="127.0.0.1", port=5000, debug=True)
