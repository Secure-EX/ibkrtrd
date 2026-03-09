import sys
import time
from pathlib import Path

# ==========================================
# 提升项目根目录优先级，确保能导入所有模块
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

# [1] 数据拉取层 (Extract)
from data_pull.ibkr_api import pull_all_ibkr_data
from data_pull.yfinance_api import fetch_financials
from data_pull.akshare_api import fetch_hk_ohlcv

# [2] 数据处理和分析层 (Transform & Calculate)
# 整合在json_assembler里面了
# from processors.fundamental_calc import generate_fundamental_analysis
# 整合在json_assembler里面了
# from processors.technical_calc import generate_technical_analysis
from processors.risk_calc import generate_portfolio_risk_report
from processors.json_assembler import assemble_llm_payload

# [3] 报告与 Prompt 生成层 (Load & Output)
from llm_report.prompt_template import generate_consolidated_api_prompt

def main():
    print("🌟" + "="*50 + "🌟")
    print("      启动终极量化投研流水线 (Quant Pipeline)")
    print("🌟" + "="*50 + "🌟\n")

    # ---------------------------------------------------------
    # 第一阶段：账户与风控全局扫描
    # ---------------------------------------------------------
    try:
        # 1. 连接盈透，拉取最新持仓和资产
        ibkr_data, symbols_for_yf = pull_all_ibkr_data()
    except Exception as e:
        print(f"\n❌ 致命错误: IBKR 数据拉取失败，流水线终止。({e})")
        return

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

    # 获取去重后的持仓列表，防止由于分批买入导致同一只股票重复跑
    unique_holdings = {item['Symbol']: item for item in ibkr_data}.values()

    for item in unique_holdings:
        raw_symbol = str(item['Symbol'])
        currency = item['Currency']
        company_name = item.get('Company Name (EN)', 'Unknown')

        # 智能代码转换器：抹平各大 API 之间的代码差异
        # 盈透(700) -> 雅虎/Akshare(0700.HK)
        if currency == 'HKD':
            # 港股必须是 4 位数字加 .HK
            standard_symbol = raw_symbol.zfill(4) + ".HK"
        else:
            # 如果你未来买了美股 (如 AAPL)，直接使用原代码
            standard_symbol = raw_symbol

        print(f"\n" + "▼"*50)
        print(f"  🚀 开始处理: {standard_symbol} ({company_name})")
        print("▲"*50)

        try:
            # --- 1. 数据拉取层 (Extract) ---
            print(f"   ▶ [1/3] 拉取历史量价数据 (AkShare)...")
            if currency == 'HKD':
                fetch_hk_ohlcv(standard_symbol)
            else:
                print(f"   ⚠️ 提示: {standard_symbol} 非港股，跳过 AkShare 抓取。")

            time.sleep(1.5) # 🛡️ 防封锁休眠

            print(f"   ▶ [2/3] 拉取财务基本面三表 (Yahoo Finance)...")
            fetch_financials(standard_symbol)

            time.sleep(2) # 🛡️ 防封锁休眠

            # --- 2. 组装与输出层 (Load & Output) ---
            print(f"   ▶ [3/3] 组装终极 LLM 数据载荷 (JSON)...")
            assemble_llm_payload(standard_symbol)

            print(f"   ✅ {standard_symbol} 专属研报材料准备就绪！")

        except Exception as e:
            # 如果某一只股票拉取失败（例如停牌、没发财报），打印错误并继续处理下一只，绝不让整个流水线崩溃
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
    # 大功告成
    # ---------------------------------------------------------
    print("\n" + "="*54)
    print("🎉 全量化流水线执行完毕！")
    print("📁 所有的 Prompt 文本已经静静地躺在 data/latest 目录下了。")
    print("👉 下一步：尽情把它们发给大模型，检验我们的火力吧！")
    print("="*54 + "\n")

if __name__ == "__main__":
    main()
