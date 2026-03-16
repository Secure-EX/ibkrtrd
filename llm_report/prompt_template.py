import sys
import json
import glob
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from processors.json_assembler import sanitize_for_web

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import LATEST_DIR, PORTFOLIO_DIR

USER_NOTES_FILE = BASE_DIR / "user_notes.json"

def _load_user_notes():
    """安全读取用户外部备忘录"""
    if USER_NOTES_FILE.exists():
        with open(USER_NOTES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def _get_latest_file(directory: Path, prefix: str) -> Path:
    search_pattern = str(directory / f"{prefix}*.csv")
    files = glob.glob(search_pattern)
    return Path(max(files, key=os.path.getmtime)) if files else None

def generate_consolidated_api_prompt() -> str:
    """将全局账户、所有持仓明细和所有个股切片，合并为单个 API-ready 的 JSON"""
    print("\n📦 正在聚合全量数据，生成终极 API Prompt...")

    today_str = datetime.now().strftime("%Y%m%d")

    # 动态加载外部备忘录
    user_notes_dict = _load_user_notes()

    # ==========================================
    # 1. 组装全局上下文 (Global Context)
    # ==========================================
    global_context = {
        "portfolio_risk_report": {},
        "current_all_positions": []
    }

    # 读风控
    risk_file = LATEST_DIR / "portfolio_risk.json"
    if risk_file.exists():
        with open(risk_file, 'r', encoding='utf-8') as f:
            global_context["portfolio_risk_report"] = json.load(f)

    # 读全局持仓表 (一次性喂给大模型所有的持仓成本和比例，个股里就不用再传了)
    positions_file = _get_latest_file(PORTFOLIO_DIR, "current_positions_")
    if positions_file:
        df_pos = pd.read_csv(positions_file)
        # 用我们刚写的过滤器清洗一下浮点数，直接转字典
        global_context["current_all_positions"] = sanitize_for_web(df_pos.to_dict(orient='records'))

    # ==========================================
    # 2. 聚合并挂载所有个股切片 (Stock Queue)
    # ==========================================
    stock_analysis_queue = []

    # 扫描所有生成的 Payload JSON
    payload_files = glob.glob(str(LATEST_DIR / "*_LLM_Payload.json"))
    for file_path in payload_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            stock_data = json.load(f)

            ticker = stock_data['meta']['ticker']
            ibkr_symbol = ticker.split('.')[0].lstrip('0') if '.' in ticker else ticker
            note = user_notes_dict.get(ibkr_symbol, "无特定主观备注。")

            stock_analysis_queue.append({
                "target_ticker": ticker,
                "user_subjective_note": note,
                "quantitative_payload": stock_data
            })

    # ==========================================
    # 3. 终极 API Prompt 结构 (System + Context + Data + Task)
    # ==========================================
    # 高阶量化指标说明书 (Metric Dictionary)
    metric_definitions = {
        "price_to_earnings_to_roe_pr": "修正版市赚率 (PR = 修正系数n * PE / (ROE*100))。本系统加入了分红惩罚机制。评判标准：PR=1 为合理估值，PR < 0.4 为极度低估的巴菲特买入区间，PR > 2 意味着估值透支或盈利能力过低。",
        "price_to_dream_ps_adjusted": "量化市梦率 (Price-to-Dream = PS / 营收增速比率)。类似 PEG，但使用市销率和营收增速，专门用于评估尚未盈利或高增长科技股（如理想汽车、泡泡玛特）。数值越低，说明'梦想'越有业绩支撑。",
        "dcf_intrinsic_value_proxy": "格雷厄姆防守底线 (sqrt(22.5 * EPS * BVPS))。这是一种极度苛刻的防守型估值法，代表在毫无增长预期下的清算级价值底线。如果股价跌破此值，意味着处于极度错杀状态。",
        "net_income_cash_content_ratio": "净利润现金含量 (经营现金流 / 净利润)。排雷核心指标。评判标准：大于 1 极其优秀（印钞机），0.8 - 1 为正常，持续低于 0.8 则存在严重的财务造假或利润调节风险（纸面富贵）。",
        "price_position_52w_ratio": "52周水位线百分位。范围 0-1。0 代表当前价格处于过去一年最低点，1 代表处于一年最高点。0.5代表在中间位置。用于辅助判断目前是破位寻底还是突破创新高。",
        "beta": "贝塔系数。衡量个股相对大盘的波动性。Beta > 1 代表比大盘波动更剧烈（高弹性），Beta < 1 代表比大盘抗跌（防御性）。"
    }

    master_prompt = {
        "instructions": {
            "task": "港股股票持仓深度分析和配置建议",
            "role": "你是一位华尔街顶级的资深金融分析师与量化投资组合经理",
            "language": "中文",
            "objective": "你需要对下方提供的结构化量化数据进行深度拆解，根据用户提供的持仓数据和风险偏好，提供盈亏分析、风险评估及操作建议，并输出一份极其专业的中文分析报告",
            "note": "如果任何数据不可得，必须标注来源并说明假设。Strictly follow this structure in the analysis framework."
        },
        "user_profile": {
            "investment_style": "稳健增长型",
            "investment_frequency": "1周1次统一交易，不做日内交易，非必要不交易",
            "risk_tolerance": "高 (可承受账户总净值 30% 的回撤)",
            "time_horizon": "长期 (3-5年)",
            "investment_goal": "年化 15%-20%"
        },
        "metric_definitions": metric_definitions,
        "global_portfolio_context": global_context,
        "stocks_analysis_queue": stock_analysis_queue,
        "analysis_requirements": [
            "请严格遵循以下框架使用“马斯克的第一性原理 Elon Musk's First Principles”进行输出，每一个分析内容都需要分成专业角度和狗都能看懂的角度进行输出：",
            "1. 资产核心状态速览: 评估全局账户安全度，及各个标的的仓位健康度，并制作表格。",
            "2. 每只股票的基本面与估值穿透: 重点评估市赚率(price_to_earnings_to_roe_pr)及净利润现金(net_income_cash_content)含量(防造假)，对其余每个独立指标进行专业和狗都能看懂的角度进行解析，并制作表格。",
            "3. 每只股票的技术面与多周期共振: 结合日/周/月线判断支撑阻力与当前动能，对每个独立指标进行专业和狗都能看懂的角度进行解析，并制作表格。",
            "4. 每只股票的情绪面搜索: 网络搜索近一个月该公司的信息，提供思考分析时使用的参考链接，并制作表格，重点对雪球，同花顺，东方财富的相关内容进行搜索。",
            "5. 指出组合中最大的潜在风险点：如果股市强烈回调20%会发生什么。包括但不限于关税战，贸易战，热战，瘟疫等",
            "6. 牛熊指引：如果一切顺利，股价能到多少？逻辑是什么？如果风险爆发，股价底线在哪里？",
            "7. 最终决断与操作计划: 基于用户的特定备忘录和全局资金，给出明确的[加仓/减仓/持有/止损]建议（需精确到参考价位和数量比例）。"
        ]
    }

    # 落盘为统一的 JSON 文件
    output_path = LATEST_DIR / f"prompt_{today_str}.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(master_prompt, f, indent=4, ensure_ascii=False)

    # ==========================================
    # 4. 网页端投喂切片器 (Web Prompt Splitter)
    # ==========================================
    web_dir = LATEST_DIR / f"web_prompts_{today_str}"
    web_dir.mkdir(exist_ok=True) # 创建专属切片文件夹

    # --- 第 1 口：全局设定与任务 ---
    global_slice = {
        "instructions": master_prompt["instructions"],
        "user_profile": master_prompt["user_profile"],
        "metric_definitions": master_prompt["metric_definitions"],
        "global_portfolio_context": master_prompt["global_portfolio_context"],
        "analysis_requirements": master_prompt["analysis_requirements"]
    }
    with open(web_dir / "01_全局设定与指令.txt", 'w', encoding='utf-8') as f:
        f.write("[System Instructions & Global Context]\n")
        f.write("请阅读以下全局设定、账户资金状态以及最终的分析任务要求：\n```json\n")
        # f.write(json.dumps(global_slice, indent=4, ensure_ascii=False))
        f.write(json.dumps(global_slice, ensure_ascii=False, separators=(',', ':')))
        f.write("\n```\n[重要指令]\n")
        f.write("这是我的全局账户状态和你的分析任务。请回复：“收到，我已经清楚账户资金和风控底线。请提供个股数据，我将逐一进行极度深度的硬核拆解。”\n")
        f.write("注意：在收到后续的个股数据前，请不要做任何分析！")

    # --- 第 2 到 N 口：个股数据切片 ---
    total_stocks = len(stock_analysis_queue)
    for i, stock in enumerate(stock_analysis_queue):
        ticker = stock['target_ticker']
        safe_ticker = ticker.replace(":", "_")
        with open(web_dir / f"02_{i+1:02d}_个股数据_{safe_ticker}.txt", 'w', encoding='utf-8') as f:
            f.write(f"[Stock Data {i+1}/{total_stocks}]\n")
            f.write(f"这是第 {i+1} 只股票的数据（{ticker}）。\n```json\n")
            # f.write(json.dumps(stock, indent=4, ensure_ascii=False))
            f.write(json.dumps(stock, ensure_ascii=False, separators=(',', ':')))
            f.write("\n```\n[重要指令]\n")
            f.write("请严格按照刚才确认的框架要求，动用全部算力，不惜字数地对这只股票进行深度剖析（包括市赚率、现金流排雷、多周期技术面共振和牛熊推演等）。\n")
            f.write("写完后，请提示我发送下一只股票的数据。")

    # --- 最终口：终极决断 ---
    with open(web_dir / "03_终极决断与操作计划.txt", 'w', encoding='utf-8') as f:
        f.write("[Final Actionable Plan]\n")
        f.write("所有标的已投喂完毕！\n[重要指令]\n")
        f.write("现在，请你调取最初的“全局资金状态(Global Portfolio Context)”，结合你刚才进行的所有单股深度分析，给我出具一份包含明确股数、价位以及买卖逻辑的[最终操作计划表]。\n")
        f.write("请确保总动用资金绝不超过我的可用现金，并且严格遵守马斯克的第一性原理。")

    print(f"✅ 终极 API 聚合完毕！仅需发送此单一文件至大模型: {output_path.name}")
    print(f"📦 网页端投喂切片已生成至: {web_dir.name} (请按文件编号顺序复制给 AI 网页端)")
    return master_prompt

if __name__ == "__main__":
    generate_consolidated_api_prompt()
