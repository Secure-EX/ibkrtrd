import pandas as pd
# 导入配置和模块
from config import PORTFOLIO_DIR, TRANSACTIONS_DIR, MARKET_DIR, STOCK_DIR, FUNDAMENTAL_DIR, EMOTIONAL_DIR, TECHNICAL_DIR, RISK_DIR, SUMMARY_DIR, ACCOUNT_ID
from data_pull.data_pull import IBKRClient
from strategies.technical_analysis import LocalAnalyzer
from strategies.fundamental_analysis import get_valuation_metrics

def main():
    # 1. 初始化连接
    client = IBKRClient()
    client.connect()

    try:
        # --- 任务 A: 分析个股 (腾讯) ---
        symbol_ib = "700"      # IBKR 代码
        symbol_yf = "0700.HK"  # Yahoo 代码

        # 2. 拉取数据
        df = client.get_hk_stock_data(symbol_ib)

        if df is not None:
            # 3. 保存原始数据到 CSV (使用 config 中定义的路径)
            csv_path = STOCK_DIR / "stock_data_700.csv"
            df.to_csv(csv_path)
            print(f"💾 数据已保存至: {csv_path}")

            # 4. 技术分析
            tech_analyzer = LocalAnalyzer(df)
            tech_analyzer.add_indicators()
            trend = tech_analyzer.analyze_trend()
            val_status, val_score = tech_analyzer.analyze_valuation_technical()
            decision, reasons = tech_analyzer.generate_signal()

            # 5. 基本面分析
            fund_data = get_valuation_metrics(symbol_yf)

            # 6. 打印报告
            print("\n" + "="*40)
            print(f"📊 综合分析报告: 腾讯控股 ({symbol_yf})")
            print("="*40)
            print(f"1️⃣ 走势: {trend}")
            print(f"2️⃣ 技术估值: {val_status} ({val_score})")
            print(f"3️⃣ 基本面: PE={fund_data['PE']}, PB={fund_data['PB']}")
            print(f"4️⃣ 信号: {reasons}")
            print(f"🚦 建议: 【{decision}】")
            print("="*40)

        # --- 任务 B: 获取持仓 ---
        my_portfolio = client.???(account_id=ACCOUNT_ID)
        if not my_portfolio.empty:
            # 保存持仓数据
            port_path = PORTFOLIO_DIR / "portfolio_data.csv"
            my_portfolio.to_csv(port_path, index=False)
            print(f"\n💼 持仓数据已保存至: {port_path}")
            print(my_portfolio)
        else:
            print("\n💼 当前账户无持仓")

    except KeyboardInterrupt:
        print("程序被手动中断")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
