import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import FINANCIALS_DIR

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
    roe_decimal: 小数形式的 ROE (例如 0.15 代表 15%)
    payout_ratio: 小数形式的分红率 (例如 0.50 代表 50%)
    """
    if None in (pe, roe_decimal) or roe_decimal <= 0:
        return None

    if payout_ratio is None:
        payout_ratio = 0

    # 计算修正系数 N
    if payout_ratio >= 0.50:
        n = 1.0
    # 当一家公司不分红时（N = 2.0），这意味着它的市赚率直接翻倍，估值瞬间变贵。
    elif payout_ratio <= 0.25:
        n = 2.0
    else:
        n = 0.50 / payout_ratio

    # PR 算法本身要求 ROE 以整数百分比形态参与公式（如 15 代入计算）
    roe_pct = roe_decimal * 100

    # 终极市赚率公式
    pr = n * (pe / roe_pct)
    return pr

def _calc_conservative_dcf_proxy(eps_ttm, bvps):
    """
    基于格雷厄姆稳健公式的简化版 DCF 估值替代
    公式: 根号下 (22.5 * EPS * BVPS)
    这是一种极度苛刻的防守型估值法
    """
    if None in (eps_ttm, bvps) or eps_ttm <= 0 or bvps <= 0:
        return None

    # 22.5 = 15 (合理市盈率) * 1.5 (合理市净率)
    intrinsic_value = (22.5 * eps_ttm * bvps) ** 0.5
    return intrinsic_value

def _calc_price_to_dream(ps_ratio, revenue_growth_decimal):
    """
    量化市梦率：市销率 / 营收增速百分比。
    类似 PEG，只不过把盈利换成了营收。数值越低，说明“梦想”越有支撑。
    """
    if None in (ps_ratio, revenue_growth_decimal) or revenue_growth_decimal <= 0:
        return None
    return ps_ratio / revenue_growth_decimal

def _calc_altman_z_score(row, market_cap):
    """计算 Altman Z-Score (非制造企业版四变量模型)"""
    ca = row.get('Current Assets', row.get('Total Current Assets', 0))
    cl = row.get('Current Liabilities', row.get('Total Current Liabilities', 0))
    working_capital = row.get('Working Capital', ca - cl)

    total_assets = row.get('Total Assets', None)
    x1 = _safe_div(working_capital, total_assets)

    retained_earnings = row.get('Retained Earnings', None)
    x2 = _safe_div(retained_earnings, total_assets)

    ebit = row.get('EBIT', row.get('Operating Income', None))
    x3 = _safe_div(ebit, total_assets)

    total_liabilities = row.get('Total Liabilities Net Minority Interest', row.get('Total Liabilities', None))
    x4 = _safe_div(market_cap, total_liabilities)

    if None in (x1, x2, x3, x4):
        return None

    z_score = 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4
    return z_score

def _get_quarter_string(date_str):
    """将日期转换为财报季度字符串，例如 '2025-Q3'"""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"

def _safe_growth_rate(current, previous):
    """
    计算安全的增长率，完美处理前一期盈利为负数的数学扭曲陷阱。
    公式: (本期 - 前期) / abs(前期)
    """
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return None
    return float((current - previous) / abs(previous))

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

    # 历史便宜量：上一期 (向下 1 行)
    rev_prev_1 = rev_series.shift(-1)
    net_common_prev_1 = net_common_series.shift(-1)

    # 历史便宜量：去年同期 (向下 4 行，仅供季报使用)
    rev_prev_4 = rev_series.shift(-4) if not is_annual else None
    net_common_prev_4 = net_common_series.shift(-4) if not is_annual else None

    reports_list = []

    for idx, row in df_merged.iterrows():
        date_str = str(row['Date'])[:10]

        # --- 基础财务数据提取 ---
        revenue = rev_series.loc[idx]
        net_to_common = net_common_series.loc[idx]
        net_income = net_income_series.loc[idx]
        operating_cash_flow = ocf_series.loc[idx]

        gross_profit = _safe_get_col(df_merged, ['Gross Profit']).loc[idx]
        operating_income = _safe_get_col(df_merged, ['Operating Income', 'EBIT']).loc[idx]
        total_equity = _safe_get_col(df_merged, ['Stockholders Equity', 'Total Equity Gross Minority Interest']).loc[idx]
        current_assets = _safe_get_col(df_merged, ['Current Assets', 'Total Current Assets']).loc[idx]
        current_liabilities = _safe_get_col(df_merged, ['Current Liabilities', 'Total Current Liabilities']).loc[idx]
        total_liabilities = _safe_get_col(df_merged, ['Total Liabilities Net Minority Interest', 'Total Liabilities']).loc[idx]

        ## --- 1. 比率计算 (Profitability & Efficiency) ---
        gross_margin = _safe_div(gross_profit, revenue)
        op_margin = _safe_div(operating_income, revenue)
        net_margin = _safe_div(net_income, revenue)
        roe = _safe_div(net_income, total_equity)
        current_ratio = _safe_div(current_assets, current_liabilities)
        debt_to_equity = _safe_div(total_liabilities, total_equity)

        # 造假排雷指标：净利润现金含量
        net_income_cash_content = _safe_div(operating_cash_flow, net_income)

        # Z-Score
        z_score = _calc_altman_z_score(row, market_cap) if market_cap else None

        # --- 2. 成长性跨期计算 (Growth) ---
        if is_annual:
            # 年报：只有 YoY (同比)
            rev_yoy = _safe_growth_rate(revenue, rev_prev_1.loc[idx])
            net_yoy = _safe_growth_rate(net_to_common, net_common_prev_1.loc[idx])
            rev_qoq, net_qoq = None, None
        else:
            # 季报：QoQ (环比) 找上一行，YoY (同比) 找上四行
            rev_qoq = _safe_growth_rate(revenue, rev_prev_1.loc[idx])
            net_qoq = _safe_growth_rate(net_to_common, net_common_prev_1.loc[idx])
            rev_yoy = _safe_growth_rate(revenue, rev_prev_4.loc[idx])
            net_yoy = _safe_growth_rate(net_to_common, net_common_prev_4.loc[idx])

        # --- 3. 组装单期报告 (JSON 乐高模块) ---
        report = {
            "report_period": date_str,
            # 绝对规模指标 (防伪存真)
            "absolute_metrics": {
                "total_revenue": revenue if pd.notna(revenue) else None,
                "net_income": net_income if pd.notna(net_income) else None,
                "net_income_to_common": net_to_common if pd.notna(net_to_common) else None,
                "operating_cash_flow": operating_cash_flow if pd.notna(operating_cash_flow) else None
            },
            # 成长性指标 (戴维斯双击的引擎)
            "growth": {
                "revenue_yoy_ratio": rev_yoy,
                "net_income_yoy_ratio": net_yoy,
                "revenue_qoq_ratio": rev_qoq,
                "net_income_qoq_ratio": net_qoq
            },
            "profitability": {
                "gross_margin_ratio": gross_margin,
                "operating_margin_ratio": op_margin,
                "net_margin_ratio": net_margin
            },
            "efficiency": {
                "roe_ratio": roe
            },
            "risk_and_cashflow": {
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "altman_z_score": z_score,
                "net_income_cash_content_ratio": net_income_cash_content
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

    # 4. 在最新的年报中注入静态估值指标与股息指标 (包含你独创的三大核心指标)
    if fundamentals["annual_reports"]:
        latest_report = fundamentals["annual_reports"][0]

        # 提取基础组件以计算高阶指标
        pe_ttm = info_data.get('trailingPE')
        roe_decimal = latest_report.get("efficiency", {}).get("roe_ratio")
        payout_ratio = info_data.get('payoutRatio', 0)

        eps = info_data.get('trailingEps')
        bvps = info_data.get('bookValue')

        ps_ratio = info_data.get('priceToSalesTrailing12Months')
        rev_growth = info_data.get('revenueGrowth')

        # 计算三大高阶指标
        adjusted_pr = _calc_adjusted_pr(pe_ttm, roe_decimal, payout_ratio)
        intrinsic_val = _calc_conservative_dcf_proxy(eps, bvps)
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
