from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

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


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            reports=data_io.list_reports(),
            tickers=data_io.list_tickers(),
            parse_date=data_io.parse_report_date,
        )

    @app.route("/reports")
    def reports():
        files = data_io.list_reports()
        if not files:
            return render_template("reports.html", files=[], current=None, html_body=None)
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
            files=files,
            current=current,
            html_body=html_body,
            parse_date=data_io.parse_report_date,
        )

    @app.route("/charts")
    def charts_index():
        tickers = data_io.list_tickers()
        if not tickers:
            return render_template("chart.html", tickers=[], ticker=None, trend=None)
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
        else:
            md_text, src_path = stage1
            stage1_html = data_io.render_markdown(md_text)
            stage1_source = src_path.name
        return render_template(
            "chart.html",
            tickers=tickers,
            ticker=ticker,
            trend=trend,
            stage1_html=stage1_html,
            stage1_source=stage1_source,
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
