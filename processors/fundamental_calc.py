import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import FINANCIALS_DIR, FINANCIAL_REPORT_YEARS, FINANCIAL_REPORT_QTERS
from data_pull.yfinance_api import fetch_treasury_yield

def _get_quarter_string(date_str):
    """将日期转换为财报季度字符串，例如 '2025-Q3'"""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"

def _safe_div(numerator, denominator):
    """安全除法，防止除以 0 或 NaN"""
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return None
    return float(numerator) / float(denominator)

def _safe_get_col(df, possible_cols):
    """动态列名获取：按优先级寻找存在的列"""
    for col in possible_cols:
        if col in df.columns:
            return df[col]
    return pd.Series([None] * len(df), index=df.index)

def _calc_adjusted_pr(pe, roe_decimal, payout_ratio):
    """
    计算独家修正版市赚率 (Price-to-Earnings-to-ROE Ratio)
    参数:
    pe: 市盈率 (TTM)
    roe_decimal: 小数形式的 ROE (例如 0.15 代表 15%)
    payout_ratio: 小数形式的分红率 (例如 0.50 代表 50%)
    """
    # 亏损或无效数据直接放弃 — 负 PE 的 PR 没有分析意义
    if None in (pe, roe_decimal) or pe <= 0 or roe_decimal <= 0:
        return None

    if payout_ratio is None or payout_ratio < 0:
        # 分红率为负 (亏损仍派息) 或缺失，按最严厉的不分红处理
        payout_ratio = 0

    # 计算修正系数 N
    if payout_ratio >= 0.50:
        n = 1.0
    elif payout_ratio <= 0.25:
        n = 2.0
    else:
        n = 0.50 / payout_ratio

    # 分红超过利润 (吃老本)，额外加罚 — N 至少为 1.5
    if payout_ratio > 1.0:
        n = max(n, 1.5)

    # PR 算法本身要求 ROE 以整数百分比形态参与公式（如 15 代入计算）
    roe_pct = roe_decimal * 100

    # 终极市赚率公式
    pr = n * (pe / roe_pct)
    return pr

def _calc_conservative_dcf_proxy(eps_ttm, bvps):
    """
    格雷厄姆防守底线 (Graham Number)，基于格雷厄姆稳健公式的简化版 DCF 估值替代
    公式: 根号下 (22.5 * EPS * BVPS)
    这是零增长假设下的清算级价值底线，是一种极度苛刻的防守型估值法。
    """
    if None in (eps_ttm, bvps) or eps_ttm <= 0 or bvps <= 0:
        return None

    # 22.5 = 15 (合理市盈率) * 1.5 (合理市净率)
    intrinsic_value = (22.5 * eps_ttm * bvps) ** 0.5
    return intrinsic_value

def _calc_graham_growth_value(eps_ttm, growth_rate_decimal, bond_yield=None):
    """
    格雷厄姆成长修正估值 (Graham Growth Formula) — 进阶版
    基础公式: V = EPS × (8.5 + 2g)
    进阶公式: V = EPS × (8.5 + 2g) × (4.4 / Y)

    8.5 = 格雷厄姆认为零增长公司的合理 PE
    2g  = 每 1% 的增长率值 2 倍 PE 的溢价
    4.4 = 格雷厄姆时代 AAA 级公司债平均收益率 (%)
    Y   = 当前债券收益率 (百分比数字，如 4.5 代表 4.5%)

    利率修正的意义：利率越高，股票的机会成本越高，合理估值越低，反之亦然。
    当 bond_yield 不可用时，退化为基础版公式。

    注意：此公式对 g > 25 的超高增长公司会过于乐观，封顶 25 防止失真。
    """
    if None in (eps_ttm, growth_rate_decimal) or eps_ttm <= 0:
        return None

    # 转为百分比数字 (0.15 → 15)
    g = growth_rate_decimal * 100

    # 增长率不可能永续维持超高水平，封顶防失真
    if g <= 0:
        return None
    g = min(g, 25)

    intrinsic_value = eps_ttm * (8.5 + 2 * g)

    # 进阶版：加入利率环境修正因子
    if bond_yield and bond_yield > 0:
        intrinsic_value *= (4.4 / bond_yield)

    return intrinsic_value

def _calc_price_to_dream(ps_ratio, revenue_growth_decimal):
    """
    量化市梦率：市销率 / 营收增速百分比数字。
    类似 PEG 的思路，只不过把盈利换成了营收，专为尚未盈利或高增长公司设计。

    数值越低，说明"梦想"越有业绩支撑：
      < 0.5  极度便宜，增长远超市场定价
      0.5-1  合理，增长能支撑当前估值
      1-2    偏贵，需要增长加速才能消化估值
      > 2    严重透支，梦想远超现实
      负数   最危险：高估值 + 营收萎缩
    """
    if None in (ps_ratio, revenue_growth_decimal) or revenue_growth_decimal == 0:
        return None

    # 转为百分比数字 (0.127 → 12.7)，与 PEG 的 PE/g 保持同一量纲
    growth_pct = revenue_growth_decimal * 100

    return ps_ratio / growth_pct

def _calc_altman_z_score(row):
    """
    计算 Altman Z''-Score (非制造企业 + 新兴市场版四变量模型)

    Z'' = 6.56*X1 + 3.26*X2 + 6.72*X3 + 1.05*X4

    X1 = 营运资金 / 总资产          (短期偿债能力)
    X2 = 留存收益 / 总资产          (累积盈利能力)
    X3 = 经营溢利 / 总资产          (资产盈利效率)
    X4 = 账面股东权益 / 总负债       (资本结构)

    判定标准: > 2.6 安全, 1.1-2.6 灰色地带, < 1.1 财务困境
    """
    # 严格取值，找不到就 None，不再默认 0
    ca = row.get('Current Assets', row.get('Total Current Assets'))
    cl = row.get('Current Liabilities', row.get('Total Current Liabilities'))

    if ca is not None and cl is not None:
        working_capital = row.get('Working Capital', ca - cl)
    else:
        working_capital = row.get('Working Capital')

    total_assets = row.get('Total Assets')
    x1 = _safe_div(working_capital, total_assets)

    retained_earnings = row.get('Retained Earnings')
    x2 = _safe_div(retained_earnings, total_assets)

    ebit = row.get('EBIT', row.get('Operating Income'))
    x3 = _safe_div(ebit, total_assets)

    # Z-Score 的 X4 是账面股东权益/总负债，不是市值/总负债
    book_equity = row.get('Stockholders Equity', row.get('Total Equity Gross Minority Interest'))
    total_liabilities = row.get('Total Liabilities Net Minority Interest', row.get('Total Liabilities'))
    x4 = _safe_div(book_equity, total_liabilities)

    if None in (x1, x2, x3, x4):
        return None

    z_score = 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4
    return z_score

def _safe_growth_rate(current, previous):
    """
    计算安全的增长率，完美处理前一期盈利为负数的数学扭曲陷阱。
    公式: (本期 - 前期) / abs(前期)
    """
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return None
    return float((current - previous) / abs(previous))

def _safe_cagr(current, previous, years: int):
    """
    计算复合年增长率 (CAGR)。
    公式: (current / previous) ^ (1/years) - 1
    安全处理前期为负或零的情况。
    """
    if pd.isna(current) or pd.isna(previous) or previous <= 0 or current <= 0:
        return None
    return float((current / previous) ** (1.0 / years) - 1)

def _process_financial_statements(inc_file: Path, bal_file: Path, cash_file: Path, market_cap: float, is_annual: bool = True) -> list:
    """
    通用财务数据处理器：合并利润表和资产负债表以及现金流表，计算核心指标，返回格式化的列表。
    """
    # 利润表和资产负债表是底线，必须有；现金流量表改为可选
    if not inc_file.exists() or not bal_file.exists():
        return []

    df_inc = pd.read_csv(inc_file)
    df_bal = pd.read_csv(bal_file)

    # 先以外连接合并利润表和资产负债表
    df_merged = pd.merge(df_inc, df_bal, on='Date', how='outer')

    # 容错处理：如果现金流量表存在，则合并；如果不存在，安全跳过
    if cash_file.exists():
        df_cash = pd.read_csv(cash_file)
        df_merged = pd.merge(df_merged, df_cash, on='Date', how='outer')
    else:
        # 如果是季报缺失现金流，稍微打印一句提示即可
        period_type = "年报" if is_annual else "季报"
        print(f"   ⚠️ 提示: 缺失 {period_type}现金流量表 ({cash_file.name})，相关现金流指标将为空。")

    df_merged.sort_values('Date', ascending=False, inplace=True) # 时间倒序

    # 提前提取绝对数值序列，并向下偏移(shift)获取历史参照值
    rev_series = _safe_get_col(df_merged, ['Total Revenue', 'Operating Revenue'])
    # 优先取归母净利润，没有则取总净利
    net_common_series = _safe_get_col(df_merged, ['Net Income Common Stockholders', 'Net Income Applicable To Common Shares', 'Net Income'])
    net_income_series = _safe_get_col(df_merged, ['Net Income'])
    ocf_series = _safe_get_col(df_merged, ['Operating Cash Flow', 'Total Cash From Operating Activities', 'Cash Flow From Continuing Operating Activities', 'Net Cash Provided By Operating Activities'])
    gross_profit_series = _safe_get_col(df_merged, ['Gross Profit'])  # 毛利序列
    capex_series = _safe_get_col(df_merged, ['Capital Expenditure'])  # 资本开支序列
    interest_expense_series = _safe_get_col(df_merged, ['Interest Expense'])                # 利息覆盖
    depreciation_series = _safe_get_col(df_merged, ['Depreciation And Amortization'])       # EBITDA
    accounts_receivable_series = _safe_get_col(df_merged, ['Accounts Receivable'])          # 应收周转率

    # 历史参照量：上一期 (向下 1 行)
    rev_prev_1 = rev_series.shift(-1)
    net_common_prev_1 = net_common_series.shift(-1)
    gross_profit_prev_1 = gross_profit_series.shift(-1)
    ocf_prev_1 = ocf_series.shift(-1)
    net_common_prev_3 = net_common_series.shift(-3) if is_annual else None  # 3年CAGR

    # 历史参照量：去年同期 (向下 4 行，仅供季报使用)
    rev_prev_4 = rev_series.shift(-4) if not is_annual else None
    net_common_prev_4 = net_common_series.shift(-4) if not is_annual else None
    gross_profit_prev_4 = gross_profit_series.shift(-4) if not is_annual else None
    ocf_prev_4 = ocf_series.shift(-4) if not is_annual else None

    reports_list = []

    for idx, row in df_merged.iterrows():
        date_str = str(row['Date'])[:10]

        # --- 基础财务数据提取 ---
        revenue = rev_series.loc[idx]
        net_to_common = net_common_series.loc[idx]
        net_income = net_income_series.loc[idx]
        operating_cash_flow = ocf_series.loc[idx]

        gross_profit = gross_profit_series.loc[idx]  # 使用预提取的序列（和增速计算保持一致）
        operating_income = _safe_get_col(df_merged, ['Operating Income', 'EBIT']).loc[idx]
        total_equity = _safe_get_col(df_merged, ['Stockholders Equity', 'Total Equity Gross Minority Interest']).loc[idx]
        total_assets = _safe_get_col(df_merged, ['Total Assets']).loc[idx]  # ROA 需要
        current_assets = _safe_get_col(df_merged, ['Current Assets', 'Total Current Assets']).loc[idx]
        current_liabilities = _safe_get_col(df_merged, ['Current Liabilities', 'Total Current Liabilities']).loc[idx]
        total_liabilities = _safe_get_col(df_merged, ['Total Liabilities Net Minority Interest', 'Total Liabilities']).loc[idx]
        capex = capex_series.loc[idx]  # 资本开支
        interest_expense = interest_expense_series.loc[idx]
        depreciation = depreciation_series.loc[idx]
        accounts_receivable = accounts_receivable_series.loc[idx]

        ## --- 1. 比率计算 (Profitability & Efficiency) ---
        gross_margin = _safe_div(gross_profit, revenue)
        op_margin = _safe_div(operating_income, revenue)
        net_margin = _safe_div(net_income, revenue)
        roe = _safe_div(net_income, total_equity)  # Return on Equity 股东权益报酬率
        roa = _safe_div(net_income, total_assets)  # Return on Assets 资产回报率

        # 自由现金流 = 经营现金流 - 资本开支 (capex 通常为负数，所以用加法)
        fcf = None
        if pd.notna(operating_cash_flow) and pd.notna(capex):
            fcf = float(operating_cash_flow) + float(capex)  # capex 是负数，相加即为减去

        # 利息覆盖倍数 = 经营溢利 / 融资成本 (融资成本通常为负数，取绝对值)
        interest_coverage = None
        if pd.notna(operating_income) and pd.notna(interest_expense) and interest_expense != 0:
            interest_coverage = float(abs(operating_income)) / float(abs(interest_expense))

        # EBITDA = 经营溢利 + 折旧摊销
        ebitda = None
        ebitda_margin = None
        if pd.notna(operating_income) and pd.notna(depreciation):
            ebitda = float(operating_income) + float(abs(depreciation))
            ebitda_margin = _safe_div(ebitda, revenue)

        # 应收账款周转率 = 营收 / 应收账款 (越高越好，说明回款快)
        ar_turnover = _safe_div(revenue, accounts_receivable)

        # 资本开支强度 = |Capex| / 营收 (低 = 轻资产模式)
        capex_to_revenue = _safe_div(abs(capex) if pd.notna(capex) else None, revenue)

        # FCF 收益率 = FCF / 市值 (比股息率更真实的回馈潜力)
        fcf_yield = _safe_div(fcf, market_cap) if fcf else None

        current_ratio = _safe_div(current_assets, current_liabilities)
        debt_to_equity = _safe_div(total_liabilities, total_equity)

        # 造假排雷指标：净利润现金含量
        net_income_cash_content = _safe_div(operating_cash_flow, net_income)

        # Z-Score
        z_score = _calc_altman_z_score(row)

        # --- 2. 成长性跨期计算 (Growth) ---
        if is_annual:
            # 年报：只有 YoY (同比)
            rev_yoy = _safe_growth_rate(revenue, rev_prev_1.loc[idx])
            net_yoy = _safe_growth_rate(net_to_common, net_common_prev_1.loc[idx])
            gp_yoy = _safe_growth_rate(gross_profit, gross_profit_prev_1.loc[idx])               # 毛利增速
            ocf_yoy = _safe_growth_rate(operating_cash_flow, ocf_prev_1.loc[idx])                # 经营现金流增速
            net_income_cagr_3y = _safe_cagr(net_to_common, net_common_prev_3.loc[idx], 3)  # 净利3年CAGR
            rev_qoq, net_qoq, gp_qoq, ocf_qoq = None, None, None, None
        else:
            # 季报：QoQ (环比) 找上一行，YoY (同比) 找上四行
            rev_qoq = _safe_growth_rate(revenue, rev_prev_1.loc[idx])
            net_qoq = _safe_growth_rate(net_to_common, net_common_prev_1.loc[idx])
            gp_qoq = _safe_growth_rate(gross_profit, gross_profit_prev_1.loc[idx])
            ocf_qoq = _safe_growth_rate(operating_cash_flow, ocf_prev_1.loc[idx])
            rev_yoy = _safe_growth_rate(revenue, rev_prev_4.loc[idx])
            net_yoy = _safe_growth_rate(net_to_common, net_common_prev_4.loc[idx])
            gp_yoy = _safe_growth_rate(gross_profit, gross_profit_prev_4.loc[idx])
            ocf_yoy = _safe_growth_rate(operating_cash_flow, ocf_prev_4.loc[idx])
            net_income_cagr_3y = None  # 季报不计算 CAGR

        # --- 3. 组装单期报告 (JSON 乐高模块) ---
        report = {
            "report_period": date_str,
            # 绝对规模指标 (防伪存真)
            "absolute_metrics": {
                "total_revenue": revenue if pd.notna(revenue) else None,
                "net_income": net_income if pd.notna(net_income) else None,
                "net_income_to_common": net_to_common if pd.notna(net_to_common) else None,
                "operating_cash_flow": operating_cash_flow if pd.notna(operating_cash_flow) else None,
                "free_cash_flow": fcf                # 自由现金流
            },
            # 成长性指标 (戴维斯双击的引擎)
            "growth": {
                "revenue_yoy_ratio": rev_yoy,
                "net_income_yoy_ratio": net_yoy,
                "gross_profit_yoy_ratio": gp_yoy,                 # 毛利增速（与营收增速的剪刀差 = 竞争力信号）
                "operating_cashflow_yoy_ratio": ocf_yoy,          # 经营现金流增速（与净利增速背离 = 造假信号）
                "net_income_cagr_3y_ratio": net_income_cagr_3y,   # 净利润3年复合增长率
                "revenue_qoq_ratio": rev_qoq,
                "net_income_qoq_ratio": net_qoq,
                "gross_profit_qoq_ratio": gp_qoq,
                "operating_cashflow_qoq_ratio": ocf_qoq
            },
            "profitability": {
                "gross_margin_ratio": gross_margin,
                "operating_margin_ratio": op_margin,
                "net_margin_ratio": net_margin,
                "ebitda_margin_ratio": ebitda_margin              # EBITDA利润率
            },
            "efficiency": {
                "roe_ratio": roe,
                "roa_ratio": roa,    # 资产回报率（排除杠杆幻觉，看真实盈利能力）
                "accounts_receivable_turnover": ar_turnover,      # 应收周转率
                "capex_to_revenue_ratio": capex_to_revenue        # 资本开支强度
            },
            "risk_and_cashflow": {
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "altman_z_score": z_score,
                "net_income_cash_content_ratio": net_income_cash_content,
                "interest_coverage": interest_coverage,           # 利息覆盖倍数
                "fcf_yield_ratio": fcf_yield                      # 自由现金流收益率
            }
        }

        # 根据周期类型添加特定的标签
        if is_annual:
            report["fiscal_year"] = date_str[:4]
        else:
            report["fiscal_quarter"] = _get_quarter_string(date_str)

        reports_list.append(report)

    return reports_list

def generate_fundamental_analysis(ticker_symbol: str) -> dict:
    """生成包含 Annual 和 Quarterly 的完整基础面字典"""
    print(f"⚙️ 正在拼装 {ticker_symbol} 的基本面与估值数据...")

    info_file = FINANCIALS_DIR / f"{ticker_symbol}_info.json"

    # 1. 加载基础画像 (Info)
    info_data = {}
    if info_file.exists():
        with open(info_file, 'r', encoding='utf-8') as f:
            info_data = json.load(f)

    currency = info_data.get('financialCurrency', 'Unknown')
    market_cap = info_data.get('marketCap', None)

    fundamentals = {
        "currency": currency,
        "annual_reports": [],
        "quarterly_reports": []
    }

    # 2. 调用通用处理器，生成年报列表
    a_inc_file = FINANCIALS_DIR / f"{ticker_symbol}_annual_income.csv"
    a_bal_file = FINANCIALS_DIR / f"{ticker_symbol}_annual_balance.csv"
    a_cash_file = FINANCIALS_DIR / f"{ticker_symbol}_annual_cashflow.csv"
    fundamentals["annual_reports"] = _process_financial_statements(a_inc_file, a_bal_file, a_cash_file, market_cap, is_annual=True)

    # 3. 调用通用处理器，生成季报列表
    q_inc_file = FINANCIALS_DIR / f"{ticker_symbol}_quarterly_income.csv"
    q_bal_file = FINANCIALS_DIR / f"{ticker_symbol}_quarterly_balance.csv"
    q_cash_file = FINANCIALS_DIR / f"{ticker_symbol}_quarterly_cashflow.csv"
    fundamentals["quarterly_reports"] = _process_financial_statements(q_inc_file, q_bal_file, q_cash_file, market_cap, is_annual=False)

    # 截取最近 N 年的年度报告喂给 LLM，原始 CSV 全量保留不动
    # annual_reports 已经按日期倒序排列（最新在前），直接切片
    fundamentals["annual_reports"] = fundamentals["annual_reports"][:FINANCIAL_REPORT_YEARS]
    # 截取最近 N 个季度的季度报告喂给 LLM，原始 CSV 全量保留不动
    fundamentals["quarterly_reports"] = fundamentals["quarterly_reports"][:FINANCIAL_REPORT_QTERS]

    # 4. 在最新的年报中注入静态估值指标与股息指标 (包含你独创的三大核心指标)
    if fundamentals["annual_reports"]:
        latest_report = fundamentals["annual_reports"][0]

        # 补丁：如果 akshare 近期年份缺少现金流明细，用 yfinance info.json 的 TTM 数据填补
        # 先确定 yfinance 数据对应哪一期，只补丁匹配的报告
        yf_fiscal_end_ts = info_data.get('mostRecentQuarter') or info_data.get('lastFiscalYearEnd')
        yf_fiscal_date = None
        if yf_fiscal_end_ts:
            yf_fiscal_date = datetime.fromtimestamp(yf_fiscal_end_ts).strftime('%Y-%m-%d')

        # 在年报列表中找到与 yfinance 日期匹配的那一期
        patch_target = None
        if yf_fiscal_date:
            for report in fundamentals["annual_reports"]:
                if report.get("report_period") == yf_fiscal_date:
                    patch_target = report
                    break

        # 如果没有精确匹配，退回到最新一期（容忍几天的日期偏差）
        if patch_target is None and yf_fiscal_date:
            # 只在年份匹配时才补丁，防止跨年错配
            yf_year = yf_fiscal_date[:4]
            if latest_report.get("report_period", "")[:4] == yf_year:
                patch_target = latest_report

        if patch_target:
            target_abs = patch_target.get("absolute_metrics", {})
            target_prof = patch_target.get("profitability", {})
            target_risk = patch_target.get("risk_and_cashflow", {})

            if target_abs.get("free_cash_flow") is None and info_data.get("freeCashflow"):
                target_abs["free_cash_flow"] = info_data.get("freeCashflow")
                target_abs["free_cash_flow_source"] = f"yfinance_ttm_{yf_fiscal_date}"

            if target_abs.get("operating_cash_flow") is None and info_data.get("operatingCashflow"):
                target_abs["operating_cash_flow"] = info_data.get("operatingCashflow")
                target_abs["operating_cash_flow_source"] = f"yfinance_ttm_{yf_fiscal_date}"

            # 如果 EBITDA 利润率为空，用 yfinance 的 EBITDA 补算
            if target_prof.get("ebitda_margin_ratio") is None and info_data.get("ebitda"):
                yf_ebitda = info_data.get("ebitda")
                yf_revenue = info_data.get("totalRevenue")
                if yf_ebitda and yf_revenue and yf_revenue > 0:
                    target_prof["ebitda_margin_ratio"] = yf_ebitda / yf_revenue
                    target_prof["ebitda_margin_source"] = f"yfinance_ttm_{yf_fiscal_date}"

            # 如果 FCF 被补上了，重新计算 FCF Yield
            if target_risk.get("fcf_yield_ratio") is None and target_abs.get("free_cash_flow") and market_cap:
                target_risk["fcf_yield_ratio"] = target_abs["free_cash_flow"] / market_cap

            # 补上净利润现金含量（如果 OCF 刚被补上）
            if target_risk.get("net_income_cash_content_ratio") is None:
                ocf = target_abs.get("operating_cash_flow")
                ni = target_abs.get("net_income")
                if ocf and ni and ni != 0:
                    target_risk["net_income_cash_content_ratio"] = ocf / ni

        # 提取基础组件以计算高阶指标
        pe_ttm = info_data.get('trailingPE')
        roe_decimal = latest_report.get("efficiency", {}).get("roe_ratio")
        payout_ratio = info_data.get('payoutRatio', 0)

        eps = info_data.get('trailingEps')
        bvps = info_data.get('bookValue')

        ps_ratio = info_data.get('priceToSalesTrailing12Months')
        rev_growth = info_data.get('revenueGrowth')

        # 获取当前国债收益率，用于格雷厄姆进阶公式的利率修正
        bond_yield = fetch_treasury_yield()

        # 计算三大高阶指标
        adjusted_pr = _calc_adjusted_pr(pe_ttm, roe_decimal, payout_ratio)
        intrinsic_val = _calc_conservative_dcf_proxy(eps, bvps)
        graham_growth_val = _calc_graham_growth_value(eps, info_data.get('earningsGrowth'), bond_yield)
        price_to_dream = _calc_price_to_dream(ps_ratio, rev_growth if rev_growth else None)

        # 提取股息与分红数据 (如果前瞻股息没有，就用过去 12 个月的滚动股息)
        # yfinance 的 dividendYield 返回百分比数字 (如 1.04 代表 1.04%)，除以 100 转为小数比率
        div_yield = info_data.get('dividendYield', info_data.get('trailingAnnualDividendYield'))
        div_yield_ratio = (div_yield / 100) if div_yield else None
        # 每股绝对分红金额
        div_rate = info_data.get('dividendRate', info_data.get('trailingAnnualDividendRate'))

        # yfinance 的 fiveYearAvgDividendYield 返回百分比数字 (如 0.63 代表 0.63%)，除以 100 转为小数比率
        five_yr_avg_div = info_data.get('fiveYearAvgDividendYield')
        five_yr_avg_div_ratio = (five_yr_avg_div / 100) if five_yr_avg_div else None

        # 提取 Beta 和华尔街分析师预期
        beta = info_data.get('beta')
        wall_street_target = info_data.get('targetMeanPrice')
        recommendation = info_data.get('recommendationKey') # 例如 "buy", "hold", "underperform"

        latest_report["valuation"] = {
            "pe_ttm": pe_ttm,
            "pe_fwd": info_data.get('forwardPE'),
            "peg": info_data.get('trailingPegRatio', info_data.get('pegRatio')),
            "pb": info_data.get('priceToBook'),
            "ps": ps_ratio,
            "dcf_intrinsic_value_proxy": intrinsic_val,       # 极其苛刻的格雷厄姆防守估值
            "graham_growth_value": graham_growth_val,         # 成长修正估值
            "price_to_earnings_to_roe_pr": adjusted_pr,       # 修正版市赚率
            "price_to_dream_ps_adjusted": price_to_dream      # 市梦率
        }

        # 将股息组件作为独立模块挂载到最近一期的财报中
        latest_report["dividends"] = {
            "dividend_yield_ratio": div_yield_ratio,              # 当前股息率 (如 0.0087 代表 0.87%)
            "dividend_rate_per_share": div_rate,                  # 每股绝对分红金额
            "five_year_avg_yield_ratio": five_yr_avg_div_ratio,   # 五年平均股息率 (用来判断当前股息是否具有吸引力)
            "payout_ratio": payout_ratio                          # 派息比率 (用来判断分红是否吃老本)
        }

        # 将市场预期与波动率作为独立模块挂载
        latest_report["market_context"] = {
            "beta": beta,
            "wall_street_target_price": wall_street_target,
            "analyst_recommendation": recommendation
        }


    print(f"✅ {ticker_symbol} 基本面数据清洗完成！包含 {len(fundamentals['annual_reports'])} 份年报, {len(fundamentals['quarterly_reports'])} 份季报。")
    return fundamentals

# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    test_ticker = "0700.HK"
    fund_data = generate_fundamental_analysis(test_ticker)

    if fund_data:
        print("\n最终输出的 Fundamental JSON 结构片段:")
        print(json.dumps(fund_data, indent=4, ensure_ascii=False))
