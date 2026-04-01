import sys
import time
from pathlib import Path

# ==========================================
# 提升项目根目录优先级，确保能导入所有模块
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

# [1] 数据拉取层 (Extract)
from data_pull.ibkr_api import pull_all_ibkr_data, fetch_ibkr_ohlcv  # IBKR 持仓快照 K线拉取主引擎
from data_pull.yfinance_api import fetch_financials, fetch_index_ohlcv, fallback_to_yfinance  # yfinance 底线财务报表(info.json)+财报副引擎-akshare挂掉 大盘指数拉取 K线副引擎-IBKR挂掉
from data_pull.akshare_api import fetch_financials_akshare  # akshare 财报主引擎
from config import LOOKBACK_YEARS, INDEX_SYMBOLS

# [2] 数据处理和分析层 (Transform & Calculate)
from processors.risk_calc import generate_portfolio_risk_report
from processors.json_assembler import assemble_llm_payload

# [3] 报告与 Prompt 生成层 (Load & Output)
from llm_report.prompt_template import generate_consolidated_api_prompt

def main():
    print("🌟" + "="*50 + "🌟")
    print("      启动终极量化投研流水线 (Quant Pipeline)")
    print("🌟" + "="*50 + "🌟\n")

    # ---------------------------------------------------------
    # 第零阶段：拉取大盘指数数据 (yfinance，不依赖 IBKR 连接)
    # ---------------------------------------------------------
    print("📊 [第零阶段] 拉取大盘指数参照数据 (yfinance)...\n")
    for idx_symbol in INDEX_SYMBOLS:
        try:
            fetch_index_ohlcv(idx_symbol)
        except Exception as e:
            print(f"   ⚠️ {idx_symbol} 指数拉取异常: {e}")
        time.sleep(1)

    # ---------------------------------------------------------
    # 第一阶段：账户与风控全局扫描
    # ---------------------------------------------------------
    ib = None  # 预声明，确保 finally 能安全访问
    try:
        ib, ibkr_data, symbols_for_yf = pull_all_ibkr_data()  # 接收 ib 连接对象
    except Exception as e:
        print(f"\n❌ 致命错误: IBKR 数据拉取失败，流水线终止。({e})")
        return

    # 用 try/finally 包裹后续全部流程，确保无论如何都能断开连接
    try:
        if not ibkr_data:
            print("\n⚠️ 账户当前无持仓数据，流水线安全结束。")
            return

        # 2. 生成全局风控报告 (portfolio_risk.json)
        try:
            generate_portfolio_risk_report()
        except Exception as e:
            print(f"\n❌ 风控计算发生错误: {e}")

        # ---------------------------------------------------------
        # 第二阶段：持仓标的逐个击破 (自动批处理)
        # ---------------------------------------------------------
        print("\n🎯 账户扫描完毕，开始批量生成单股深度分析报告...\n")

        unique_holdings = {item['Symbol']: item for item in ibkr_data}.values()

        for item in unique_holdings:
            raw_symbol = str(item['Symbol'])
            currency = item['Currency']
            company_name = item.get('Company Name (EN)', 'Unknown')

            if currency == 'HKD':
                standard_symbol = raw_symbol.zfill(4) + ".HK"
            else:
                standard_symbol = raw_symbol

            print(f"\n" + "▼"*50)
            print(f"  🚀 开始处理: {standard_symbol} ({company_name})")
            print("▲"*50)

            try:
                # --- 1. 数据拉取层 (Extract) ---
                # 主引擎: IBKR 拉取 OHLCV，失败自动降级到 yfinance
                print(f"   ▶ [1/3] 拉取历史量价数据 (IBKR)...")
                try:
                    fetch_ibkr_ohlcv(ib, standard_symbol, currency)
                except Exception as e:
                    print(f"   ⚠️ IBKR 历史数据拉取失败: {e}")
                    print(f"   🔄 启动 yfinance 备用引擎...")
                    # 副引擎：yfinance 拉取 OHLCV
                    fallback_to_yfinance(standard_symbol, LOOKBACK_YEARS)

                ib.sleep(2)  # IBKR pacing 礼貌间隔

                # 先跑 yfinance 拉 info.json + 三表 CSV (作为底线)
                print(f"   ▶ [2/3a] 拉取公司画像与基础财报 (yfinance)...")
                try:
                    fetch_financials(standard_symbol)
                except Exception as e:
                    print(f"   ⚠️ yfinance 拉取失败: {e}")

                # 再跑 akshare 覆盖三表 CSV (更新更快，会覆盖 yfinance 的旧数据)
                print(f"   ▶ [2/3b] 用东方财富最新财报覆盖 (AkShare)...")
                try:
                    fetch_financials_akshare(standard_symbol)
                except Exception as e:
                    print(f"   ⚠️ AkShare 财报覆盖失败，将使用 yfinance 数据: {e}")

                time.sleep(1)

                print(f"   ▶ [3/3] 组装终极 LLM 数据载荷 (JSON)...")
                assemble_llm_payload(standard_symbol)

                print(f"   ✅ {standard_symbol} 专属研报材料准备就绪！")

            except Exception as e:
                print(f"   ❌ {standard_symbol} 处理过程中发生异常: {e}")
                continue

        # ---------------------------------------------------------
        # 第三阶段：终极聚合 (Consolidate into API Prompt)
        # ---------------------------------------------------------
        print("\n【第三阶段】合成终极 API Prompt...")
        try:
            generate_consolidated_api_prompt()
        except Exception as e:
            print(f"❌ 终极聚合失败: {e}")

        # ---------------------------------------------------------
        # 第四阶段：LLM 报告自动生成
        # ---------------------------------------------------------
        # print("\n【第四阶段】调用 Claude CLI 自动生成分析报告...")
        # try:
        #     from llm_report.report_generator import generate_report
        #     generate_report()
        # except Exception as e:
        #     print(
        #         f"⚠️ LLM 报告生成失败，可手动运行:\n"
        #         f"   python -m llm_report.report_generator\n"
        #         f"   错误: {e}"
        #     )

        # ---------------------------------------------------------
        # 大功告成
        # ---------------------------------------------------------
        print("\n" + "="*54)
        print("🎉 全量化流水线执行完毕！")
        print("📁 所有的 Prompt 文本已经静静地躺在 data/latest 目录下了。")
        print("👉 分析报告已保存至 data/output/final_reports/")
        print("="*54 + "\n")

    finally:
        # 无论流水线成功还是中途失败，确保 IBKR 连接被安全释放
        if ib and ib.isConnected():
            ib.disconnect()
            print("🔌 IBKR 连接已安全断开。")

if __name__ == "__main__":
    main()
