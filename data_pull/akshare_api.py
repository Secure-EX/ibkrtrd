import sys
import pandas as pd
import akshare as ak
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import FINANCIALS_DIR

# ==========================================
# 中文科目 → yfinance 英文列名 映射表
# 只映射 fundamental_calc.py 实际使用的字段
# ==========================================
INCOME_MAP = {
    "营业额": "Total Revenue",
    "营运收入": "Operating Revenue",
    "毛利": "Gross Profit",
    "经营溢利": "Operating Income",
    "除税前溢利": "Pretax Income",
    "税项": "Income Tax Expense",
    "除税后溢利": "Net Income",
    "股东应占溢利": "Net Income Common Stockholders",
    "每股基本盈利": "Basic EPS",
    "每股摊薄盈利": "Diluted EPS",
    "融资成本": "Interest Expense",         # 计算利息覆盖倍数
    "利息收入": "Interest Income",          # 计算净利息支出
}

BALANCE_MAP = {
    "总资产": "Total Assets",
    "流动资产合计": "Current Assets",
    "流动负债合计": "Current Liabilities",
    "总负债": "Total Liabilities Net Minority Interest",
    "股东权益": "Stockholders Equity",
    "总权益": "Total Equity Gross Minority Interest",
    "保留溢利(累计亏损)": "Retained Earnings",
    "净流动资产": "Working Capital",
    "现金及等价物": "Cash And Cash Equivalents",
    "应收帐款": "Accounts Receivable",       # 计算应收周转率
}

CASHFLOW_MAP = {
    "经营业务现金净额": "Operating Cash Flow",
    "投资业务现金净额": "Investing Cash Flow",
    "融资业务现金净额": "Financing Cash Flow",
    "现金净额": "Changes In Cash",
    "购建固定资产": "Capital Expenditure",
    "已付股息(融资)": "Dividends Paid",
    "加:折旧及摊销": "Depreciation And Amortization",  # 计算 EBITDA
}

def _pivot_long_to_wide(df: pd.DataFrame, name_map: dict) -> pd.DataFrame:
    """
    将 akshare 的长格式 (每行一个科目) 转换为 yfinance 兼容的宽格式 (每行一个报告期)。
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 只保留我们需要的科目
    df_filtered = df[df['STD_ITEM_NAME'].isin(name_map.keys())].copy()

    if df_filtered.empty:
        return pd.DataFrame()

    # 标准化日期
    df_filtered['Date'] = pd.to_datetime(df_filtered['REPORT_DATE']).dt.strftime('%Y-%m-%d')

    # 将中文科目名替换为英文列名
    df_filtered['STD_ITEM_NAME'] = df_filtered['STD_ITEM_NAME'].map(name_map)

    # 长转宽：每行一个日期，每列一个科目
    df_wide = df_filtered.pivot_table(
        index='Date',
        columns='STD_ITEM_NAME',
        values='AMOUNT',
        aggfunc='first'
    ).reset_index()

    df_wide.columns.name = None  # 去掉 pivot 产生的列名层级
    df_wide.sort_values('Date', ascending=True, inplace=True)

    return df_wide

def fetch_financials_akshare(ticker_symbol: str) -> bool:
    """
    通过 akshare (东方财富) 拉取港股财报三表，输出格式与 yfinance 完全兼容。
    数据源更新速度通常比 yfinance 快 2-3 周。

    参数:
        ticker_symbol: 标准代码 (如 "0700.HK")
    """
    print(f"🔄 [AkShare] 开始抓取 {ticker_symbol} 的财务报表 (东方财富数据源)...")

    # 0700.HK → 00700
    ak_symbol = ticker_symbol.split('.')[0].zfill(5)

    # ==========================================
    # 定义拉取任务: (symbol参数, 报表类型, indicator, 映射表, 输出文件后缀, 标签)
    # ==========================================
    tasks = [
        ("利润表", "年度", INCOME_MAP, "annual_income", "年报利润表"),
        ("利润表", "报告期", INCOME_MAP, "quarterly_income", "季报利润表"),
        ("资产负债表", "年度", BALANCE_MAP, "annual_balance", "年报资产负债表"),
        ("资产负债表", "报告期", BALANCE_MAP, "quarterly_balance", "季报资产负债表"),
        ("现金流量表", "年度", CASHFLOW_MAP, "annual_cashflow", "年报现金流量表"),
        ("现金流量表", "报告期", CASHFLOW_MAP, "quarterly_cashflow", "季报现金流量表"),
    ]

    success_count = 0

    for report_type, indicator, name_map, suffix, label in tasks:
        try:
            df_raw = ak.stock_financial_hk_report_em(
                stock=ak_symbol, symbol=report_type, indicator=indicator
            )

            df_wide = _pivot_long_to_wide(df_raw, name_map)

            if df_wide.empty:
                print(f"  ⚠️ {label} 数据为空，跳过。")
                continue

            file_path = FINANCIALS_DIR / f"{ticker_symbol}_{suffix}.csv"
            df_wide.to_csv(file_path, index=False, encoding='utf-8')
            print(f"  ✅ 成功提取 {label}: {file_path.name} (共 {len(df_wide)} 期)")
            success_count += 1

        except Exception as e:
            print(f"  ❌ 提取 {label} 时发生错误: {e}")

    print(f"🎉 [AkShare] {ticker_symbol} 财报拉取完毕！成功 {success_count}/6")
    return success_count > 0

# ==========================================
# 测试入口
# ==========================================
if __name__ == "__main__":
    test_ticker = "0700.HK"
    fetch_financials_akshare(test_ticker)
