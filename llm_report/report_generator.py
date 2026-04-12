"""
llm_report/report_generator.py
================================
Multi-stage Claude CLI report generator for HK stock portfolio analysis.

Each stage opens an independent `claude -p` session (non-interactive print
mode), writes a full MD file to disk, then compresses it to a compact summary
via Haiku before passing it to the next stage. This keeps each session's
context small and focused, letting extended-thinking models reason deeply
on a single task rather than scanning a giant undifferentiated context.

Pipeline (generate_staged_report):
  Stage 0  Haiku               — portfolio status table (00_*.txt)
  Stage 1  Sonnet + effort high — global framework ack (stage0 + 01_*.txt)
  Stage 2  Opus  + effort max   — per-stock deep analysis (one fresh session per stock)
  Stage 3  Opus  + effort max   — final action plan (stage1_compact + stage2 compacts + 03_*.txt)
  Stage 4  Python only          — local assembly → CLAUDE_staged_YYYYMMDD.md

Intermediate files are written to web_prompts_YYYYMMDD/stages/ for inspection.

Legacy single-session mode (generate_report) is kept for reference.

Usage:
  # Staged mode (default)
  python -m llm_report.report_generator
  python -m llm_report.report_generator --dir data/output/latest/web_prompts_20260412

  # Legacy single-session mode
  python -m llm_report.report_generator --legacy

  # From main.py (Phase 4)
  from llm_report.report_generator import generate_staged_report
  generate_staged_report()
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
_MODEL_POSITION   = "claude-haiku-4-5-20251001"  # turn 0: portfolio table summary
_MODEL_STOCK      = "claude-sonnet-4-6"          # turns 1-N: global context + per-stock
_MODEL_FINAL      = "claude-opus-4-6"            # final turn only: decision & action plan
_TIMEOUT_STOCK    = 600   # seconds — deep analysis per stock (~5 min)
_TIMEOUT_FINAL    = 600   # seconds — consolidated action plan
_INTER_TURN_SLEEP = 2     # seconds between turns

# ---------------------------------------------------------------------------
# Staged-mode constants
# ---------------------------------------------------------------------------
_MODEL_COMPACT    = "claude-haiku-4-5-20251001"  # compact summarizer (all stages)
_EFFORT_STAGE1    = "high"   # Sonnet: global context acknowledgment
_EFFORT_STAGE2    = "max"    # Opus: per-stock deep analysis
_EFFORT_STAGE3    = "max"    # Opus: final action plan
_TIMEOUT_STAGE0   = 180      # Haiku portfolio table
_TIMEOUT_STAGE1   = 360      # Sonnet global context ack
_TIMEOUT_STAGE2   = 900      # Opus per-stock (effort max — allow up to 15 min)
_TIMEOUT_STAGE3   = 900      # Opus final plan (effort max)
_TIMEOUT_COMPACT  = 180      # Haiku compact summarization


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
    model: str = _MODEL_STOCK,
    session_id: str | None = None,
    timeout: int = 300,
    cli_path: str = "claude",
    effort: str | None = None,
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
        "--model", model,
        "--output-format", "json",
    ]
    if effort:
        cmd += ["--effort", effort]
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


def _write_stage_file(stage_dir: Path, filename: str, content: str) -> Path:
    """Write content to stage_dir/filename and return the path."""
    path = stage_dir / filename
    path.write_text(content, encoding="utf-8")
    return path


def _compress_to_compact(
    full_text: str,
    compress_prompt: str,
    cli_path: str,
    timeout: int = _TIMEOUT_COMPACT,
) -> str:
    """
    Use Haiku to compress a full analysis MD to a compact summary.
    Falls back to the first 2000 characters if compression fails.
    """
    prompt = compress_prompt + "\n\n---\n\n" + full_text
    try:
        text, _ = _send_message(
            prompt,
            model=_MODEL_COMPACT,
            session_id=None,
            timeout=timeout,
            cli_path=cli_path,
        )
        return text
    except Exception as exc:
        return f"[Compact generation failed: {exc}]\n\n{full_text[:2000]}"


def _compact_prompt_stage1() -> str:
    return (
        "以下是分析框架确认回复。请将其压缩为精简版框架摘要，包含：\n"
        "①用户投资档案（2-3行）\n"
        "②本次分析的持仓列表（股票代码+公司名）\n"
        "③核心分析框架要点（5点以内）\n"
        "④关键风险指标阈值摘要（3-5行）\n"
        "总长度不超过500字，保留所有数字和股票代码。"
    )


def _compact_prompt_stage2(ticker: str) -> str:
    return (
        f"以下是{ticker}的完整分析报告。请提炼为compact摘要，包含：\n"
        "①核心估值结论：PR/PEG/DCF评级（3行）\n"
        "②技术信号：日/周/月综合判断（3行）\n"
        "③情绪与新闻：利好/利空倾向（2行）\n"
        "④推荐操作：[加仓/减仓/持有/止损]（1行）\n"
        "⑤关键价位：目标价/参考价/止损价（1行）\n"
        "总长度不超过600字，数字必须精确保留。"
    )


def _assemble_report(
    responses: list[dict],
    source_dir: Path,
    errors: list[str],
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Claude 深度分析报告\n",
        f"- **生成时间**: {now}",
        f"- **数据来源**: `{source_dir.name}`",
        f"- **持仓表格模型**: `{_MODEL_POSITION}`",
        f"- **个股分析模型**: `{_MODEL_STOCK}`",
        f"- **决策模型**: `{_MODEL_FINAL}`",
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


def _assemble_staged_report(
    stage_dir: Path,
    web_prompts_dir: Path,
    errors: list[str],
) -> str:
    """Combine all stage MD files into the final report markdown."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Claude 深度分析报告 (多阶段版)\n",
        f"- **生成时间**: {now}",
        f"- **数据来源**: `{web_prompts_dir.name}`",
        f"- **Stage 0**: `{_MODEL_POSITION}` — 持仓情况表格",
        f"- **Stage 1**: `claude-sonnet-4-6` (effort: {_EFFORT_STAGE1}) — 全局框架确认",
        f"- **Stage 2**: `{_MODEL_FINAL}` (effort: {_EFFORT_STAGE2}) — 个股深度分析",
        f"- **Stage 3**: `{_MODEL_FINAL}` (effort: {_EFFORT_STAGE3}) — 终极决断与操作计划",
    ]
    if errors:
        lines += ["\n> ⚠️ **生成过程中存在以下错误，相关章节需手动补充:**"]
        lines += [f"> - {e}" for e in errors]
    lines += ["\n---\n"]

    # Stage 0: portfolio table
    stage0 = stage_dir / "stage0_portfolio.md"
    if stage0.exists():
        lines += ["## 持仓情况总览表格\n", stage0.read_text(encoding="utf-8"), "\n---\n"]

    # Load ticker map (safe_ticker → original ticker, e.g. "0700_HK" → "0700.HK")
    tickers_map: dict[str, str] = {}
    tickers_json = stage_dir / "tickers.json"
    if tickers_json.exists():
        tickers_map = json.loads(tickers_json.read_text(encoding="utf-8"))

    # Stage 2: per-stock full analyses
    for full_md in sorted(stage_dir.glob("stage2_*_full.md")):
        safe_ticker = full_md.stem.replace("stage2_", "").replace("_full", "")
        ticker = tickers_map.get(safe_ticker, safe_ticker)
        lines += [f"## 个股深度分析: {ticker}\n", full_md.read_text(encoding="utf-8"), "\n---\n"]

    # Stage 3: final action plan
    stage3 = stage_dir / "stage3_final_plan.md"
    if stage3.exists():
        lines += ["## 最终操作计划 (Final Action Plan)\n", stage3.read_text(encoding="utf-8"), "\n---\n"]

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

    all_files     = sorted(web_prompts_dir.glob("*.txt"))
    position_file = next((f for f in all_files if f.name.startswith("00_")), None)
    global_file   = next((f for f in all_files if f.name.startswith("01_")), None)
    stock_files   = sorted(f for f in all_files if f.name.startswith("02_"))
    final_file    = next((f for f in all_files if f.name.startswith("03_")), None)

    if not global_file or not final_file:
        raise FileNotFoundError(
            f"Expected 01_*.txt and 03_*.txt in {web_prompts_dir}.\n"
            f"Found: {[f.name for f in all_files]}"
        )

    total_turns = 1 + len(stock_files) + (1 if position_file else 0)
    responses:  list[dict] = []
    errors:     list[str]  = []

    print(f"\n{'='*54}")
    print(f"  Claude 自动报告生成")
    print(f"  来源: {web_prompts_dir.name}")
    position_desc = "1 Haiku 持仓表格 + " if position_file else ""
    print(f"  轮次: {total_turns} 轮 (Sonnet {position_desc}{len(stock_files)} 只个股 + 1 Opus 终决)")
    print(f"{'='*54}\n")

    # -------------------------------------------------------------------
    # Turn 0 — Portfolio status table (Haiku, 独立首轮，仅持仓汇总)
    # -------------------------------------------------------------------
    turn_offset = 0
    if position_file:
        turn_offset = 1
        print(f"[1/{total_turns}] 生成持仓情况表格 (Haiku, 独立 Session)...")
        try:
            text, _ = _send_message(
                position_file.read_text(encoding="utf-8"),
                model=_MODEL_POSITION,
                session_id=None,
                timeout=_TIMEOUT_STOCK,
                cli_path=cli_path,
            )
            responses.append({"label": "持仓情况总览表格", "text": text})
            print(f"  ✓ 持仓表格已生成 ({len(text):,} 字符)")
        except subprocess.TimeoutExpired:
            msg = f"[持仓表格生成超时 ({_TIMEOUT_STOCK}s)]"
            responses.append({"label": "持仓情况总览表格", "text": msg})
            errors.append(f"Portfolio table: timeout after {_TIMEOUT_STOCK}s")
            print(f"  ⚠ 持仓表格超时，跳过继续...")
        except RuntimeError as e:
            msg = f"[持仓表格生成失败: {e}]"
            responses.append({"label": "持仓情况总览表格", "text": msg})
            errors.append(f"Portfolio table: {e}")
            print(f"  ⚠ 持仓表格出错: {e}，跳过继续...")
        time.sleep(_INTER_TURN_SLEEP)

    # -------------------------------------------------------------------
    # Per-stock deep analysis (Sonnet) — each stock uses a FRESH session
    # Global context is prepended to every stock prompt to avoid cumulative
    # context buildup that would cause token limit errors on later stocks.
    # -------------------------------------------------------------------
    global_text = global_file.read_text(encoding="utf-8")
    stock_analyses: list[tuple[str, str]] = []   # (ticker, analysis_text)

    for i, stock_file in enumerate(stock_files, start=1 + turn_offset):
        # Filename pattern: 02_01_个股数据_0700.HK.txt
        # Extract ticker from the last underscore-delimited segment of the stem
        stem_parts = stock_file.stem.split("_")
        ticker = stem_parts[-1] if len(stem_parts) >= 4 else stock_file.stem

        print(f"[{i}/{total_turns}] 分析 {ticker} (Sonnet, 独立 Session)...")
        combined_prompt = (
            global_text
            + "\n\n---\n\n"
            + stock_file.read_text(encoding="utf-8")
        )
        try:
            text, _ = _send_message(
                combined_prompt,
                model=_MODEL_STOCK,
                session_id=None,   # fresh session per stock — no context accumulation
                timeout=_TIMEOUT_STOCK,
                cli_path=cli_path,
            )
            responses.append({"label": f"个股深度分析: {ticker}", "text": text})
            stock_analyses.append((ticker, text))
            print(f"  ✓ {ticker} 完成 ({len(text):,} 字符)")
        except subprocess.TimeoutExpired:
            msg = f"[分析超时 ({_TIMEOUT_STOCK}s) — 请手动补充 {ticker} 的分析]"
            responses.append({"label": f"个股深度分析: {ticker}", "text": msg})
            stock_analyses.append((ticker, msg))
            errors.append(f"{ticker}: timeout after {_TIMEOUT_STOCK}s")
            print(f"  ⚠ {ticker} 超时，跳过继续...")
        except RuntimeError as e:
            msg = f"[分析失败: {e}]"
            responses.append({"label": f"个股深度分析: {ticker}", "text": msg})
            stock_analyses.append((ticker, msg))
            errors.append(f"{ticker}: {e}")
            print(f"  ⚠ {ticker} 出错: {e}，跳过继续...")

        time.sleep(_INTER_TURN_SLEEP)

    # -------------------------------------------------------------------
    # Final turn — Consolidated action plan (Opus, fresh session)
    # Inject global instructions + all Sonnet analyses as context so Opus
    # gets full information without carrying the accumulated session weight.
    # -------------------------------------------------------------------
    print(f"[{total_turns}/{total_turns}] 请求最终操作计划 (Opus, 新 Session)...")

    analyses_block = "\n\n".join(
        f"### {ticker} 分析结果\n{analysis}"
        for ticker, analysis in stock_analyses
    )
    opus_prompt = (
        f"{global_file.read_text(encoding='utf-8')}\n\n"
        f"---\n\n"
        f"以下是各个股的深度分析结果（由 Sonnet 完成）：\n\n"
        f"{analyses_block}\n\n"
        f"---\n\n"
        f"{final_file.read_text(encoding='utf-8')}"
    )

    try:
        text, _ = _send_message(
            opus_prompt,
            model=_MODEL_FINAL,
            session_id=None,   # fresh session — no accumulated context
            timeout=_TIMEOUT_FINAL,
            cli_path=cli_path,
            effort="high",
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
    output_path = FINAL_REPORTS_DIR / f"CLAUDE_hybrid_{today_str}.md"
    report      = _assemble_report(responses, web_prompts_dir, errors)
    output_path.write_text(report, encoding="utf-8")

    status = "⚠️ 部分错误" if errors else "✅"
    print(f"\n{status} 报告已保存: {output_path}")
    if errors:
        print("   错误摘要:")
        for e in errors:
            print(f"   - {e}")

    return output_path


def generate_staged_report(web_prompts_dir: Path | None = None) -> Path | None:
    """
    Multi-stage report generation using separate CLI chat sessions per stage.

    Pipeline:
      Stage 0  Haiku          — portfolio table (00_*.txt)
      Stage 1  Sonnet high    — global context ack (stage0_compact + 01_*.txt)
      Stage 2  Opus max       — per-stock analysis (stage1_compact + 02_*.txt each)
      Stage 3  Opus max       — final action plan (stage1_compact + stage2 compacts + 03_*.txt)
      Stage 4  Python only    — local assembly → CLAUDE_staged_YYYYMMDD.md

    Intermediate files are written to web_prompts_YYYYMMDD/stages/ for review.

    Parameters
    ----------
    web_prompts_dir : Path, optional
        Specific web_prompts_YYYYMMDD directory. Defaults to most recent.

    Returns
    -------
    Path to the saved markdown report, or None if the run failed entirely.
    """
    cli_path = _check_claude_cli()

    if web_prompts_dir is None:
        web_prompts_dir = find_latest_web_prompts()

    all_files     = sorted(web_prompts_dir.glob("*.txt"))
    position_file = next((f for f in all_files if f.name.startswith("00_")), None)
    global_file   = next((f for f in all_files if f.name.startswith("01_")), None)
    stock_files   = sorted(f for f in all_files if f.name.startswith("02_"))
    final_file    = next((f for f in all_files if f.name.startswith("03_")), None)

    if not global_file or not final_file:
        raise FileNotFoundError(
            f"Expected 01_*.txt and 03_*.txt in {web_prompts_dir}.\n"
            f"Found: {[f.name for f in all_files]}"
        )

    stage_dir = web_prompts_dir / "stages"
    stage_dir.mkdir(exist_ok=True)

    total_stages = 4
    errors: list[str] = []

    print(f"\n{'='*58}")
    print(f"  Claude 多阶段报告生成")
    print(f"  来源: {web_prompts_dir.name}")
    print(f"  阶段: Stage0(Haiku) → Stage1(Sonnet/{_EFFORT_STAGE1}) "
          f"→ Stage2(Opus/{_EFFORT_STAGE2} × {len(stock_files)}只) "
          f"→ Stage3(Opus/{_EFFORT_STAGE3})")
    print(f"  中间文件: {stage_dir}")
    print(f"{'='*58}\n")

    # -----------------------------------------------------------------------
    # Stage 0 — Portfolio table (Haiku, no effort)
    # -----------------------------------------------------------------------
    print(f"[Stage 0/{total_stages}] 持仓情况表格 (Haiku)...")
    stage0_text = ""
    if position_file:
        try:
            stage0_text, _ = _send_message(
                position_file.read_text(encoding="utf-8"),
                model=_MODEL_POSITION,
                session_id=None,
                timeout=_TIMEOUT_STAGE0,
                cli_path=cli_path,
            )
            _write_stage_file(stage_dir, "stage0_portfolio.md", stage0_text)
            # Portfolio table is already compact — reuse as compact directly
            _write_stage_file(stage_dir, "stage0_compact.md", stage0_text)
            print(f"  ✓ Stage 0 完成 ({len(stage0_text):,} 字符)")
        except subprocess.TimeoutExpired:
            errors.append(f"Stage0: timeout after {_TIMEOUT_STAGE0}s")
            print(f"  ⚠ Stage 0 超时，跳过继续...")
        except RuntimeError as exc:
            errors.append(f"Stage0: {exc}")
            print(f"  ⚠ Stage 0 出错: {exc}，跳过继续...")
    else:
        print("  — 无 00_*.txt，跳过 Stage 0")
    time.sleep(_INTER_TURN_SLEEP)

    # -----------------------------------------------------------------------
    # Stage 1 — Global context acknowledgment (Sonnet, effort high)
    # -----------------------------------------------------------------------
    print(f"[Stage 1/{total_stages}] 全局框架确认 (Sonnet, effort={_EFFORT_STAGE1})...")
    stage1_compact = ""
    try:
        stage0_prefix = (
            f"# 当前持仓情况概览\n\n{stage0_text}\n\n---\n\n"
            if stage0_text else ""
        )
        stage1_prompt = stage0_prefix + global_file.read_text(encoding="utf-8")
        stage1_text, _ = _send_message(
            stage1_prompt,
            model=_MODEL_STOCK,
            session_id=None,
            timeout=_TIMEOUT_STAGE1,
            cli_path=cli_path,
            effort=_EFFORT_STAGE1,
        )
        _write_stage_file(stage_dir, "stage1_global_ack.md", stage1_text)
        print(f"  ✓ Stage 1 主体完成 ({len(stage1_text):,} 字符)，生成 compact...")

        stage1_compact = _compress_to_compact(
            stage1_text,
            _compact_prompt_stage1(),
            cli_path,
        )
        _write_stage_file(stage_dir, "stage1_compact.md", stage1_compact)
        print(f"  ✓ Stage 1 compact 完成 ({len(stage1_compact):,} 字符)")
    except subprocess.TimeoutExpired:
        errors.append(f"Stage1: timeout after {_TIMEOUT_STAGE1}s")
        stage1_compact = global_file.read_text(encoding="utf-8")[:3000]
        print(f"  ⚠ Stage 1 超时，使用原始指令作为 compact 继续...")
    except RuntimeError as exc:
        errors.append(f"Stage1: {exc}")
        stage1_compact = global_file.read_text(encoding="utf-8")[:3000]
        print(f"  ⚠ Stage 1 出错: {exc}，使用原始指令作为 compact 继续...")
    time.sleep(_INTER_TURN_SLEEP)

    # -----------------------------------------------------------------------
    # Stage 2 — Per-stock deep analysis (Opus, effort max, fresh session each)
    # -----------------------------------------------------------------------
    stock_compacts: list[tuple[str, str]] = []  # (ticker, compact_text)

    for idx, stock_file in enumerate(stock_files, start=1):
        stem_parts = stock_file.stem.split("_")
        ticker = stem_parts[-1] if len(stem_parts) >= 4 else stock_file.stem

        print(f"[Stage 2/{total_stages}] 个股分析 [{idx}/{len(stock_files)}] {ticker} "
              f"(Opus, effort={_EFFORT_STAGE2})...")
        try:
            stock_prompt = (
                f"# 分析框架摘要（已确认）\n\n{stage1_compact}\n\n"
                f"---\n\n"
                + stock_file.read_text(encoding="utf-8")
            )
            stock_text, _ = _send_message(
                stock_prompt,
                model=_MODEL_FINAL,
                session_id=None,
                timeout=_TIMEOUT_STAGE2,
                cli_path=cli_path,
                effort=_EFFORT_STAGE2,
            )
            safe_ticker = ticker.replace(".", "_")
            _write_stage_file(stage_dir, f"stage2_{safe_ticker}_full.md", stock_text)
            print(f"  ✓ {ticker} 分析完成 ({len(stock_text):,} 字符)，生成 compact...")

            compact_text = _compress_to_compact(
                stock_text,
                _compact_prompt_stage2(ticker),
                cli_path,
            )
            _write_stage_file(stage_dir, f"stage2_{safe_ticker}_compact.md", compact_text)
            stock_compacts.append((ticker, compact_text))
            print(f"  ✓ {ticker} compact 完成 ({len(compact_text):,} 字符)")
            time.sleep(_INTER_TURN_SLEEP)
        except subprocess.TimeoutExpired:
            msg = f"[分析超时 ({_TIMEOUT_STAGE2}s)]"
            stock_compacts.append((ticker, msg))
            errors.append(f"Stage2/{ticker}: timeout after {_TIMEOUT_STAGE2}s")
            print(f"  ⚠ {ticker} 超时，跳过继续...")
        except RuntimeError as exc:
            msg = f"[分析失败: {exc}]"
            stock_compacts.append((ticker, msg))
            errors.append(f"Stage2/{ticker}: {exc}")
            print(f"  ⚠ {ticker} 出错: {exc}，跳过继续...")

        time.sleep(_INTER_TURN_SLEEP)

    # Save safe_ticker → original_ticker mapping for assembly step
    tickers_map = {t.replace(".", "_"): t for t, _ in stock_compacts}
    _write_stage_file(stage_dir, "tickers.json",
                      json.dumps(tickers_map, ensure_ascii=False, indent=2))

    # -----------------------------------------------------------------------
    # Stage 3 — Final action plan (Opus, effort max)
    # -----------------------------------------------------------------------
    print(f"[Stage 3/{total_stages}] 终极决断与操作计划 (Opus, effort={_EFFORT_STAGE3})...")
    compacts_block = "\n\n".join(
        f"### {ticker} 核心结论摘要\n{compact}"
        for ticker, compact in stock_compacts
    )
    stage3_prompt = (
        f"# 分析框架摘要（已确认）\n\n{stage1_compact}\n\n"
        f"---\n\n"
        f"# 各个股核心结论摘要（由 Opus 深度分析后压缩）\n\n"
        f"{compacts_block}\n\n"
        f"---\n\n"
        + final_file.read_text(encoding="utf-8")
    )
    try:
        stage3_text, _ = _send_message(
            stage3_prompt,
            model=_MODEL_FINAL,
            session_id=None,
            timeout=_TIMEOUT_STAGE3,
            cli_path=cli_path,
            effort=_EFFORT_STAGE3,
        )
        _write_stage_file(stage_dir, "stage3_final_plan.md", stage3_text)
        print(f"  ✓ Stage 3 完成 ({len(stage3_text):,} 字符)")
    except subprocess.TimeoutExpired:
        _write_stage_file(stage_dir, "stage3_final_plan.md",
                          f"[最终计划超时 ({_TIMEOUT_STAGE3}s) — 请手动补充]")
        errors.append(f"Stage3: timeout after {_TIMEOUT_STAGE3}s")
        print(f"  ⚠ Stage 3 超时")
    except RuntimeError as exc:
        _write_stage_file(stage_dir, "stage3_final_plan.md",
                          f"[最终计划生成失败: {exc}]")
        errors.append(f"Stage3: {exc}")
        print(f"  ⚠ Stage 3 出错: {exc}")

    # -----------------------------------------------------------------------
    # Stage 4 — Local assembly (no LLM)
    # -----------------------------------------------------------------------
    print(f"[Stage 4/{total_stages}] 本地组合最终报告...")
    today_str   = datetime.now().strftime("%Y%m%d")
    output_path = FINAL_REPORTS_DIR / f"CLAUDE_staged_{today_str}.md"
    report      = _assemble_staged_report(stage_dir, web_prompts_dir, errors)
    output_path.write_text(report, encoding="utf-8")

    status = "⚠️ 部分错误" if errors else "✅"
    print(f"\n{status} 报告已保存: {output_path}")
    print(f"   中间文件: {stage_dir}")
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
        description="Generate Claude analysis report from web prompt files (staged mode by default)."
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to a specific web_prompts_YYYYMMDD directory. "
             "Defaults to the most recent one in data/output/latest/.",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Use legacy single-session mode (Haiku→Sonnet→Opus in one process). "
             "Faster but shallower — no extended thinking, no intermediate files.",
    )
    args = parser.parse_args()

    try:
        if args.legacy:
            path = generate_report(args.dir)
        else:
            path = generate_staged_report(args.dir)
        print(f"\nDone: {path}")
    except Exception as exc:
        print(f"\nFatal: {exc}", file=sys.stderr)
        sys.exit(1)
