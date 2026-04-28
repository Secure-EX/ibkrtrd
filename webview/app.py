from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from markupsafe import Markup, escape

# Ensure project root on sys.path so `from config import ...` works when
# Flask reloads or when launched from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from webview import data_io, indicators, signals  # noqa: E402

VALID_RANGES = ("1Y", "3Y", "5Y", "All")
VALID_TFS = ("daily", "weekly", "monthly")


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
        df = data_io.load_ohlcv(ticker)
        df = indicators.build_full(df, tf=tf)
        df = indicators.slice_range(df, rng)
        ma_periods = list(indicators.MA_PERIODS_BY_TF.get(tf, indicators.MA_PERIODS_BY_TF["daily"]))
        return jsonify(
            {
                "ticker": ticker,
                "range": rng,
                "timeframe": tf,
                "ma_periods": ma_periods,
                "rows": _df_to_records(df),
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
