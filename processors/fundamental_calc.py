import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from config import FINANCIALS_DIR

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

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

    # ROE 转换为百分数进行计算
    roe_pct = roe_decimal * 100

    # 终极市赚率公式
    pr = n * (pe / roe_pct)
    return round(pr, 2)

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
    return round(intrinsic_value, 2)

def _calc_price_to_dream(ps_ratio, revenue_growth_pct):
    """
    量化市梦率：市销率 / 营收增速百分比。
    类似 PEG，只不过把盈利换成了营收。数值越低，说明“梦想”越有支撑。
    """
    if None in (ps_ratio, revenue_growth_pct) or revenue_growth_pct <= 0:
        return None
    return round(ps_ratio / revenue_growth_pct, 2)

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
    return round(z_score, 2)

def _get_quarter_string(date_str):
    """将日期转换为财报季度字符串，例如 '2025-Q3'"""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"

def _process_financial_statements(inc_file: Path, bal_file: Path, market_cap: float, is_annual: bool = True) -> list:
    """
    通用财务数据处理器：合并利润表和资产负债表，计算核心指标，返回格式化的列表。
    """
    if not inc_file.exists() or not bal_file.exists():
        return []

    df_inc = pd.read_csv(inc_file)
    df_bal = pd.read_csv(bal_file)

    # 以外连接合并，确保日期对齐
    df_merged = pd.merge(df_inc, df_bal, on='Date', how='outer')
    df_merged.sort_values('Date', ascending=False, inplace=True) # 时间倒序

    reports_list = []

    for idx, row in df_merged.iterrows():
        date_str = str(row['Date'])[:10]

        # --- 基础财务数据提取 ---
        revenue = _safe_get_col(df_merged, ['Total Revenue', 'Operating Revenue']).loc[idx]
        gross_profit = _safe_get_col(df_merged, ['Gross Profit']).loc[idx]
        operating_income = _safe_get_col(df_merged, ['Operating Income', 'EBIT']).loc[idx]
        net_income = _safe_get_col(df_merged, ['Net Income', 'Net Income Common Stockholders']).loc[idx]
        total_equity = _safe_get_col(df_merged, ['Stockholders Equity', 'Total Equity Gross Minority Interest']).loc[idx]
        current_assets = _safe_get_col(df_merged, ['Current Assets', 'Total Current Assets']).loc[idx]
        current_liabilities = _safe_get_col(df_merged, ['Current Liabilities', 'Total Current Liabilities']).loc[idx]
        total_liabilities = _safe_get_col(df_merged, ['Total Liabilities Net Minority Interest', 'Total Liabilities']).loc[idx]

        # --- 比率计算 ---
        gross_margin = _safe_div(gross_profit, revenue)
        op_margin = _safe_div(operating_income, revenue)
        net_margin = _safe_div(net_income, revenue)
        roe = _safe_div(net_income, total_equity)
        current_ratio = _safe_div(current_assets, current_liabilities)
        debt_to_equity = _safe_div(total_liabilities, total_equity)

        # Z-Score
        z_score = _calc_altman_z_score(row, market_cap) if market_cap else None

        # --- 组装单期报告 ---
        report = {
            "report_period": date_str,
            "profitability": {
                "gross_margin_pct": round(gross_margin * 100, 2) if gross_margin else None,
                "operating_margin_pct": round(op_margin * 100, 2) if op_margin else None,
                "net_margin_pct": round(net_margin * 100, 2) if net_margin else None
            },
            "efficiency": {
                "roe_pct": round(roe * 100, 2) if roe else None
            },
            "risk_and_cashflow": {
                "debt_to_equity": round(debt_to_equity, 2) if debt_to_equity else None,
                "current_ratio": round(current_ratio, 2) if current_ratio else None,
                "altman_z_score": z_score
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
    fundamentals["annual_reports"] = _process_financial_statements(a_inc_file, a_bal_file, market_cap, is_annual=True)

    # 3. 调用通用处理器，生成季报列表
    q_inc_file = FINANCIALS_DIR / f"{ticker_symbol}_quarterly_income.csv"
    q_bal_file = FINANCIALS_DIR / f"{ticker_symbol}_quarterly_balance.csv"
    fundamentals["quarterly_reports"] = _process_financial_statements(q_inc_file, q_bal_file, market_cap, is_annual=False)

    # 4. 在最新的年报中注入静态估值指标 (包含你独创的三大核心指标)
    if fundamentals["annual_reports"]:
        latest_report = fundamentals["annual_reports"][0]

        # 提取基础组件以计算高阶指标
        pe_ttm = info_data.get('trailingPE')
        roe_pct = latest_report.get("efficiency", {}).get("roe_pct")
        roe_decimal = roe_pct / 100 if roe_pct else None
        payout_ratio = info_data.get('payoutRatio', 0)

        eps = info_data.get('trailingEps')
        bvps = info_data.get('bookValue')

        ps_ratio = info_data.get('priceToSalesTrailing12Months')
        rev_growth = info_data.get('revenueGrowth')

        # 计算三大高阶指标
        adjusted_pr = _calc_adjusted_pr(pe_ttm, roe_decimal, payout_ratio)
        intrinsic_val = _calc_conservative_dcf_proxy(eps, bvps)
        price_to_dream = _calc_price_to_dream(ps_ratio, rev_growth * 100 if rev_growth else None)

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
