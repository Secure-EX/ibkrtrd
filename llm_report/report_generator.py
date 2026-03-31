"""
llm_report/report_generator.py
================================
Automates the multi-turn Claude CLI conversation that mirrors the manual
web-chat workflow:
  01_全局设定与指令.txt  → acknowledgment (turn 1)
  02_*_个股数据_*.txt   → per-stock analysis (turns 2-N)
  03_终极决断与操作计划.txt → final action plan (last turn)

Uses `claude -p --output-format json` (non-interactive print mode) so it
runs entirely via the existing Claude Code subscription — no API key cost.
Session continuity is achieved with `--resume <session_id>` extracted from
the first response's JSON payload.

Usage:
  # Standalone
  python -m llm_report.report_generator
  python -m llm_report.report_generator --dir data/output/latest/web_prompts_20260329

  # From main.py (Phase 4)
  from llm_report.report_generator import generate_report
  generate_report()
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Project-level config (BASE_DIR already on sys.path when called from main.py;
# when run as __main__ we add it explicitly below)
try:
    from config import LATEST_DIR, FINAL_REPORTS_DIR
except ImportError:
    _BASE = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(_BASE))
    from config import LATEST_DIR, FINAL_REPORTS_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_MODEL         = "claude-opus-4-6"
_TIMEOUT_TURN1 = 180   # seconds — acknowledgment is a short reply
_TIMEOUT_STOCK = 600   # seconds — deep analysis per stock (~5 min)
_TIMEOUT_FINAL = 600   # seconds — consolidated action plan
_INTER_TURN_SLEEP = 2  # seconds between turns


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_claude_cli() -> str:
    """Return path to claude binary, or raise EnvironmentError."""
    path = shutil.which("claude")
    if not path:
        raise EnvironmentError(
            "claude CLI not found in PATH.\n"
            "Ensure Claude Code is installed and accessible from this shell.\n"
            "Try: claude --version"
        )
    return path


def _send_message(
    prompt_text: str,
    session_id: str | None = None,
    timeout: int = 300,
    cli_path: str = "claude",
) -> tuple[str, str]:
    """
    Send one message to the Claude CLI in non-interactive print mode.

    Returns
    -------
    (response_text, session_id)
        response_text : the assistant's reply as a plain string
        session_id    : the session ID to pass to the next --resume call

    Raises
    ------
    subprocess.TimeoutExpired   if the CLI doesn't respond within `timeout`
    RuntimeError                if the CLI exits with a non-zero return code
    """
    cmd = [
        cli_path, "-p",
        "--model", _MODEL,
        "--output-format", "json",
    ]
    if session_id:
        cmd += ["--resume", session_id]

    result = subprocess.run(
        cmd,
        input=prompt_text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(
            f"claude exited with code {result.returncode}: {stderr[:600]}"
        )

    # Parse JSON output to extract response text and session ID
    try:
        data = json.loads(result.stdout)
        text = data.get("result") or data.get("content") or result.stdout
        sid  = data.get("session_id") or session_id or ""
    except json.JSONDecodeError:
        # Fallback: raw stdout is better than nothing
        text = result.stdout
        sid  = session_id or ""

    return text, sid


def _assemble_report(
    responses: list[dict],
    source_dir: Path,
    errors: list[str],
    session_id: str | None,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Claude Opus 4.6 深度分析报告\n",
        f"- **生成时间**: {now}",
        f"- **数据来源**: `{source_dir.name}`",
        f"- **模型**: `{_MODEL}`",
        f"- **Session ID**: `{session_id or 'unknown'}`",
    ]
    if errors:
        lines += [
            "\n> ⚠️ **生成过程中存在以下错误，相关章节需手动补充:**"
        ]
        lines += [f"> - {e}" for e in errors]
    lines += ["\n---\n"]

    for r in responses:
        lines += [f"## {r['label']}\n", r["text"], "\n---\n"]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_latest_web_prompts(base_dir: Path | None = None) -> Path:
    """Return the most recently dated web_prompts_YYYYMMDD/ directory."""
    search_dir = base_dir or LATEST_DIR
    dirs = sorted(search_dir.glob("web_prompts_*"), reverse=True)
    if not dirs:
        raise FileNotFoundError(
            f"No web_prompts_* directory found in {search_dir}.\n"
            "Run generate_consolidated_api_prompt() first."
        )
    return dirs[0]


def generate_report(web_prompts_dir: Path | None = None) -> Path | None:
    """
    Orchestrate the full multi-turn conversation and save the report.

    Parameters
    ----------
    web_prompts_dir : Path, optional
        Path to a specific web_prompts_YYYYMMDD directory.
        If None, the most recent one under LATEST_DIR is used.

    Returns
    -------
    Path to the saved markdown report, or None if the run failed entirely.
    """
    cli_path = _check_claude_cli()

    # --- Locate prompt files ---
    if web_prompts_dir is None:
        web_prompts_dir = find_latest_web_prompts()

    all_files   = sorted(web_prompts_dir.glob("*.txt"))
    global_file = next((f for f in all_files if f.name.startswith("01_")), None)
    stock_files = sorted(f for f in all_files if f.name.startswith("02_"))
    final_file  = next((f for f in all_files if f.name.startswith("03_")), None)

    if not global_file or not final_file:
        raise FileNotFoundError(
            f"Expected 01_*.txt and 03_*.txt in {web_prompts_dir}.\n"
            f"Found: {[f.name for f in all_files]}"
        )

    total_turns = 2 + len(stock_files)
    responses:  list[dict]     = []
    errors:     list[str]      = []
    session_id: str | None     = None

    print(f"\n{'='*54}")
    print(f"  Claude Opus 自动报告生成")
    print(f"  来源: {web_prompts_dir.name}")
    print(f"  轮次: {total_turns} 轮 ({len(stock_files)} 只个股)")
    print(f"{'='*54}\n")

    # -------------------------------------------------------------------
    # Turn 1 — Global context; establishes the session
    # -------------------------------------------------------------------
    print(f"[1/{total_turns}] 发送全局设定，等待确认...")
    try:
        text, session_id = _send_message(
            global_file.read_text(encoding="utf-8"),
            timeout=_TIMEOUT_TURN1,
            cli_path=cli_path,
        )
        responses.append({"label": "全局设定确认 (Acknowledgment)", "text": text})
        print(f"  ✓ Session 建立: {session_id[:20]}...")
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            "Turn 1 (global context) timed out — cannot continue without a session."
        )
    except RuntimeError:
        raise   # Re-raise; no session → nothing to resume

    # -------------------------------------------------------------------
    # Turns 2-N — Per-stock deep analysis
    # -------------------------------------------------------------------
    for i, stock_file in enumerate(stock_files, start=2):
        # Filename pattern: 02_01_个股数据_0700.HK.txt
        # Extract ticker from the last underscore-delimited segment of the stem
        stem_parts = stock_file.stem.split("_")
        # Rejoin the last segment that looks like a ticker (may contain dots)
        ticker = stem_parts[-1] if len(stem_parts) >= 4 else stock_file.stem

        print(f"[{i}/{total_turns}] 分析 {ticker}...")
        try:
            text, session_id = _send_message(
                stock_file.read_text(encoding="utf-8"),
                session_id=session_id,
                timeout=_TIMEOUT_STOCK,
                cli_path=cli_path,
            )
            responses.append({"label": f"个股深度分析: {ticker}", "text": text})
            print(f"  ✓ {ticker} 完成 ({len(text):,} 字符)")
        except subprocess.TimeoutExpired:
            msg = f"[分析超时 ({_TIMEOUT_STOCK}s) — 请手动补充 {ticker} 的分析]"
            responses.append({"label": f"个股深度分析: {ticker}", "text": msg})
            errors.append(f"{ticker}: timeout after {_TIMEOUT_STOCK}s")
            print(f"  ⚠ {ticker} 超时，跳过继续...")
        except RuntimeError as e:
            msg = f"[分析失败: {e}]"
            responses.append({"label": f"个股深度分析: {ticker}", "text": msg})
            errors.append(f"{ticker}: {e}")
            print(f"  ⚠ {ticker} 出错: {e}，跳过继续...")

        time.sleep(_INTER_TURN_SLEEP)

    # -------------------------------------------------------------------
    # Final turn — Consolidated action plan
    # -------------------------------------------------------------------
    print(f"[{total_turns}/{total_turns}] 请求最终操作计划...")
    try:
        text, _ = _send_message(
            final_file.read_text(encoding="utf-8"),
            session_id=session_id,
            timeout=_TIMEOUT_FINAL,
            cli_path=cli_path,
        )
        responses.append({"label": "最终操作计划 (Final Action Plan)", "text": text})
        print(f"  ✓ 最终计划已生成 ({len(text):,} 字符)")
    except subprocess.TimeoutExpired:
        responses.append({
            "label": "最终操作计划 (Final Action Plan)",
            "text": f"[最终计划超时 ({_TIMEOUT_FINAL}s) — 请手动补充]",
        })
        errors.append(f"Final plan: timeout after {_TIMEOUT_FINAL}s")
    except RuntimeError as e:
        responses.append({
            "label": "最终操作计划 (Final Action Plan)",
            "text": f"[最终计划生成失败: {e}]",
        })
        errors.append(f"Final plan: {e}")

    # -------------------------------------------------------------------
    # Assemble & save
    # -------------------------------------------------------------------
    today_str   = datetime.now().strftime("%Y%m%d")
    output_path = FINAL_REPORTS_DIR / f"CLAUDE_opus_{today_str}.md"
    report      = _assemble_report(responses, web_prompts_dir, errors, session_id)
    output_path.write_text(report, encoding="utf-8")

    status = "⚠️ 部分错误" if errors else "✅"
    print(f"\n{status} 报告已保存: {output_path}")
    if errors:
        print("   错误摘要:")
        for e in errors:
            print(f"   - {e}")

    return output_path


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    parser = argparse.ArgumentParser(
        description="Generate Claude Opus analysis report from web prompt files."
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to a specific web_prompts_YYYYMMDD directory. "
             "Defaults to the most recent one in data/output/latest/.",
    )
    args = parser.parse_args()

    try:
        path = generate_report(args.dir)
        print(f"\nDone: {path}")
    except Exception as exc:
        print(f"\nFatal: {exc}", file=sys.stderr)
        sys.exit(1)
