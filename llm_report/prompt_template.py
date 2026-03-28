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
        # ==========================================
        # 基本面评估系统
        # ==========================================
        "price_to_earnings_to_roe_pr": "修正版市赚率 (PR = 修正系数N × PE / (ROE×100))。仅在公司盈利(PE>0)时计算，亏损公司返回空值。分红惩罚机制：派息率≥50%时N=1.0（鼓励回馈股东），派息率25%-50%时N=0.50/派息率（线性惩罚），派息率≤25%或不分红时N=2.0（最严厉惩罚）。额外规则：派息率超过100%（分红吃老本）时N至少为1.5。评判标准：PR=1为合理估值，PR<0.4为极度低估的巴菲特买入区间，PR>2意味着估值透支或盈利能力过低。",
        "price_to_dream_ps_adjusted": "量化市梦率 (Price-to-Dream = PS / 营收增速百分比)。类似 PEG 的思路，用市销率替代PE、营收增速替代盈利增速，专为尚未盈利或高增长公司设计。评判标准：<0.5 极度便宜(增长远超估值)，0.5-1 合理，1-2 偏贵(需增长加速)，>2 严重透支。负数是最危险信号：高估值叠加营收萎缩。",
        "dcf_intrinsic_value_proxy": "格雷厄姆防守底线 (sqrt(22.5 * EPS * BVPS))。这是一种极度苛刻的防守型估值法，代表在毫无增长预期下的清算级价值底线。如果股价跌破此值，意味着处于极度错杀状态。",
        "graham_growth_value": "格雷厄姆成长修正估值 (V = EPS × (8.5 + 2g))，其中 g 为预期年化盈利增长率(%)。8.5 代表零增长公司的合理PE，每1%的增长率额外给予2倍PE溢价。增长率封顶25%防止失真。与 dcf_intrinsic_value_proxy(零增长清算底线) 配合使用：底线价值代表最悲观情景，成长修正值代表合理情景，两者构成估值区间。股价低于底线 = 极度错杀，股价在两者之间 = 合理偏保守，股价高于成长值 = 透支未来增长。",
        "altman_z_score": "Altman Z''-Score（非制造企业+新兴市场版）。纯粹基于财报账面数据，不依赖市值，每一期独立计算。Z'' = 6.56×(营运资金/总资产) + 3.26×(留存收益/总资产) + 6.72×(经营溢利/总资产) + 1.05×(股东权益/总负债)。判定标准：>2.6 安全区，1.1-2.6 灰色地带需警惕，<1.1 财务困境高危。",
        "net_income_cash_content_ratio": "净利润现金含量 (经营现金流 / 净利润)。排雷核心指标。评判标准：大于 1 极其优秀（印钞机），0.8 - 1 为正常，持续低于 0.8 则存在严重的财务造假或利润调节风险（纸面富贵）。",
        "price_position_52w_ratio": "52周水位线百分位。范围 0-1。0 代表当前价格处于过去一年最低点，1 代表处于一年最高点。0.5代表在中间位置。用于辅助判断目前是破位寻底还是突破创新高。",
        "beta": "贝塔系数。衡量个股相对大盘的波动性。Beta > 1 代表比大盘波动更剧烈（高弹性），Beta < 1 代表比大盘抗跌（防御性）。",
        # ==========================================
        # 多因子风险水平评估系统
        # ==========================================
        "long_term_risk_level": "长线多因子风险水平 (0~1)。由5个子因子加权合成后做历史百分位归一化。<0.05 为机会区（可逐步建仓），>0.95 为风险区（需逐步减仓甚至清仓），0.05-0.20 为偏低有吸引力，0.80-0.95 为偏高需谨慎。与旧版单因子 price_percentile 的区别：旧版只看价格位置，新版综合了估值/动量/波动率/技术/资金五个维度，抗单一因子噪声能力更强。",
        "short_term_risk_level": "短线多因子风险水平 (0~1)。同框架但用短窗口因子（5/10日动量、ATR波动率、日线RSI/KDJ）。<0.01 为短线机会点（可博短线反弹），>0.99 为短线风险点（短线避险）。个人不建议短线操作。正常情况下长线0.15 < 短线0.20是合理的：长线因子（主要是估值）显示历史低位，短线因子叠加了近期价格动量和资金流有轻微抬头，两者方向一致（都远低于风险区0.95）说明多周期共振看多。",
        "composite_raw": "加权合成原始分 (0~1)。5个子因子加权平均后、做历史百分位归一化之前的原始值。用于对比归一化前后差异，判断当前分数在历史中是否处于极端位置。",
        "factor_valuation": "估值因子百分位 (0~1)。有财报数据时用 PE/PB 的历史百分位均值；无财报数据时退化为 Close/SMA250 偏离度百分位。接近0表示当前估值处于历史最低区间，接近1表示最贵。长线权重30%（核心因子），短线权重10%。",
        "factor_momentum": "动量因子百分位 (0~1)。长线用12个月涨跌幅的历史百分位，短线用5日/10日涨跌幅百分位均值。接近0表示近期跌幅在历史中最深（超跌），接近1表示涨幅在历史中最猛（超涨）。长线权重15%，短线权重30%（核心因子）。",
        "factor_volatility": "波动率因子百分位 (0~1)。长线用20日滚动年化波动率的历史百分位，短线用ATR/日内波幅比率百分位。接近0表示当前波动极低（市场平静），接近1表示波动极高（恐慌或狂热）。高波动率本身是风险信号。长线/短线权重均15%。",
        "factor_technical": "技术因子百分位 (0~1)。RSI_14 和 KDJ-J 值各自做历史百分位后取均值。接近0表示技术指标在历史中最超卖，接近1表示最超买。长线权重20%，短线权重30%（核心因子）。",
        "factor_capital_flow": "资金因子百分位 (0~1)。由量比（当日成交量/20日均量）百分位和价量相关性（10日滚动Pearson）百分位取均值。用于替代机构持仓数据。接近0表示资金极度冷清，接近1表示资金极度活跃。量价正相关说明主力推动，负相关说明散户出货。长线权重20%，短线权重15%。",
        "investment_win_rate": "投资胜率 = 1 - 长线风险水平。纯数学推导，含义是如果长线风险水平为0.15，则历史上85%的时间比现在风险更高——相当于85%的概率当前是更好的买入时点。",
        "data_quality": "多因子数据质量标记。full = 5个因子全部有效，partial = 3-4个因子有效（缺失因子权重自动重分配），limited = 仅1-2个因子可用（结果参考价值有限）。",
        # ==========================================
        # 大盘相关性
        # ==========================================
        "correlation_250d": "个股与大盘指数的250日（约1年）滚动皮尔逊相关系数 (-1~1)。>0.8 高度正相关（跟大盘同涨同跌），0.5-0.8 中度正相关，0.3-0.5 低度正相关，<0.3 极弱相关（独立走势）。负值表示反向运动。对于做组合对冲和仓位管理有直接意义：高相关性个股在大盘下跌时难以独善其身。",
        "correlation_500d": "个股与大盘指数的500日（约2年）滚动皮尔逊相关系数。与250d版本配合看：如果500d低但250d高，说明近一年相关性在增强（可能因板块轮动）；反之说明在脱钩。",
        "correlation_trend_60d": "近60个交易日内250d滚动相关系数的变化趋势。包含 start（60日前的值）、end（最新值）、delta（变化量）。delta为正表示相关性走强，为负表示走弱/脱钩。",
        # ==========================================
        # 公司自身周期系数（剥离大盘）
        # ==========================================
        "own_cycle_level": "剥离大盘周期后公司自身的周期位置 (0~1)。方法：OLS回归 R_stock = α + β·R_index + ε，取残差ε的累计曲线做历史百分位。接近0表示公司自身处于历史最低谷（周期机会区），接近1表示历史最高峰（周期风险区）。<0.05 为周期机会区，>0.95 为周期风险区。大白话：把大盘涨跌的影响扣掉之后，这家公司自己的'体温'在历史上处于什么位置。",
        "regression_beta": "回归贝塔系数。OLS回归中个股对大盘的弹性。β>1 表示大盘涨1%该股涨超1%（高弹性/进攻型），β<1 表示涨不到1%（防御型），β≈1 表示与大盘同步。与 market_correlation 的区别：correlation 衡量方向一致性，beta 衡量幅度倍数。",
        "regression_alpha_annualized": "年化阿尔法（超额收益率）。OLS回归截距α年化后的值。正值表示扣除大盘影响后公司本身还有正向超额收益（公司质地好），负值表示跑输大盘（公司拖后腿）。",
        "residual_cumulative": "累计残差值。OLS残差的cumsum终值，代表公司'纯净价格路径'偏离零轴的程度。正值表示公司自身走势偏强，负值偏弱。own_cycle_level 就是对这个值做历史百分位排名的结果。",
        # ==========================================
        # 多周期共振判断
        # ==========================================
        "multi_timeframe_resonance": "长短线多周期共振判断。direction 字段：bullish = 长短线风险均偏低（强机会信号），bearish = 长短线风险均偏高（强风险信号），divergent = 长短背离（如长线低估但短线偏高，说明短期可能回调但长期有价值），neutral = 均在中性区间。长短方向一致时信号最强，背离时需根据自身投资周期决策。",
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
