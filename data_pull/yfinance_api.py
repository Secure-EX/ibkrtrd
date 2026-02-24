import sys
import json
import pandas as pd
import yfinance as yf
from pathlib import Path
from config import FINANCIALS_DIR

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

def fetch_financials(ticker_symbol: str) -> bool:
    """
    通过 yfinance 拉取股票的财务报表（三大表）和基础画像。
    包含年度(Annual)和季度(Quarterly)数据。
    """
    print(f"🔄 开始抓取 {ticker_symbol} 的财务报表与基本面数据 (yfinance)...")

    # 注意：yfinance 认的港股代码就是 "0700.HK"，不需要像 AkShare 那样去转换
    ticker = yf.Ticker(ticker_symbol)

    # ==========================================
    # 1. 抓取基础画像 (Info) -> 保存为 JSON
    # ==========================================
    try:
        info = ticker.info
        info_file = FINANCIALS_DIR / f"{ticker_symbol}_info.json"

        # 将静态的字典信息落盘
        with open(info_file, 'w', encoding='utf-8') as f:
            json.dump(info, f, indent=4, ensure_ascii=False)
        print(f"  ✅ 基础画像 (Info) 已保存: {info_file.name}")
    except Exception as e:
        print(f"  ❌ 获取基础画像失败: {e}")

    # ==========================================
    # 2. 抓取三大财报 -> 转置并保存为 CSV
    # ==========================================
    # 映射字典：将 yfinance 的属性对象与我们要保存的文件名后缀对应起来
    financial_statements = {
        "annual_income": ticker.financials,
        "quarterly_income": ticker.quarterly_financials,
        "annual_balance": ticker.balance_sheet,
        "quarterly_balance": ticker.quarterly_balance_sheet,
        "annual_cashflow": ticker.cashflow,
        "quarterly_cashflow": ticker.quarterly_cashflow
    }

    for name, df in financial_statements.items():
        try:
            # yfinance 如果没有数据，可能会返回 None 或空的 DataFrame
            if df is None or df.empty:
                print(f"  ⚠️ {name} 数据为空，跳过。")
                continue

            # 🌟 核心动作：矩阵转置 (Transpose)
            # 原本：列名是日期 (2025-12-31, 2024-12-31...)，行名是指标 (Total Revenue...)
            # 转置后：行变日期，列变指标，这才是量化数据该有的样子！
            df_transposed = df.T

            # 将转置后的索引 (原本的日期) 变成真实的数据列，并命名为 'Date'
            df_transposed.reset_index(inplace=True)
            df_transposed.rename(columns={'index': 'Date'}, inplace=True)

            # 清洗日期格式为标准的 YYYY-MM-DD
            df_transposed['Date'] = pd.to_datetime(df_transposed['Date']).dt.strftime('%Y-%m-%d')

            # 按日期时间线正向排序 (最老的数据在第一行，最新的在最后一行)
            df_transposed.sort_values('Date', ascending=True, inplace=True)

            # 落盘保存为 CSV
            file_path = FINANCIALS_DIR / f"{ticker_symbol}_{name}.csv"
            df_transposed.to_csv(file_path, index=False, encoding='utf-8')
            print(f"  ✅ 成功提取 {name}: {file_path.name} (共 {len(df_transposed)} 期)")

        except Exception as e:
            print(f"  ❌ 提取 {name} 时发生错误: {str(e)}")

    print(f"🎉 {ticker_symbol} 财务数据流水线执行完毕！")
    return True

# ==========================================
# 本地单例测试模块
# ==========================================
if __name__ == "__main__":
    test_ticker = "0700.HK"
    fetch_financials(test_ticker)
