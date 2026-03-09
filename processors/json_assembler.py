import sys
import json
import math
import pandas as pd
from pathlib import Path
from datetime import datetime

# 提升根目录优先级，确保能无缝读取 config
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import PORTFOLIO_DIR, TRANSACTIONS_DIR, LATEST_DIR, CURRENT_YEAR
# 动态导入基本面引擎
from processors.fundamental_calc import generate_fundamental_analysis
# 动态导入技术面引擎
from processors.technical_calc import generate_technical_analysis


def sanitize_for_web(data, precision=6):
    """Web/API 安全展示过滤器：抹平无限浮点数，设置精度6位，转换非法 NaN"""
    if isinstance(data, dict):
        return {k: sanitize_for_web(v, precision) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_for_web(item, precision) for item in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return round(data, precision)
    else:
        return data

# ==========================================
# 核心组装引擎
# ==========================================
def assemble_llm_payload(ticker_symbol: str) -> dict:
    print(f"\n🧩 正在为 {ticker_symbol} 组装个股专属 Payload...")

    payload = {
        "meta": {
            "ticker": ticker_symbol,
            "generation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "transaction_history": [],
        "fundamentals": {},
        "technicals": {}
    }

    ibkr_symbol = ticker_symbol.split('.')[0].lstrip('0') if '.' in ticker_symbol else ticker_symbol

    # 1. 交易流水 (仅保留这只股票的流水)
    trans_file = TRANSACTIONS_DIR / f"transactions_{CURRENT_YEAR}.csv"
    if trans_file.exists():
        df_trans = pd.read_csv(trans_file)
        df_trans['Symbol'] = df_trans['Symbol'].astype(str)
        stock_trans = df_trans[df_trans['Symbol'] == ibkr_symbol].sort_values(by='Time', ascending=False)
        if not stock_trans.empty:
            payload["transaction_history"] = stock_trans.to_dict(orient='records')

    # 2. 基本面
    try:
        payload["fundamentals"] = generate_fundamental_analysis(ticker_symbol)
    except Exception as e:
        print(f"   ❌ 基本面挂载失败: {e}")

    # 3. 技术面
    try:
        tech_data = generate_technical_analysis(ticker_symbol)
        if tech_data:
            payload["technicals"] = tech_data
    except Exception as e:
        print(f"   ❌ 技术面挂载失败: {e}")

    safe_payload = sanitize_for_web(payload, precision=6)

    # 落盘为极其精简的个股 Payload
    payload_file = LATEST_DIR / f"{ticker_symbol}_LLM_Payload.json"
    with open(payload_file, 'w', encoding='utf-8') as f:
        json.dump(safe_payload, f, indent=4, ensure_ascii=False)

    print(f"   ✅ 个股切片装配完成: {payload_file.name}")
    return safe_payload

# ==========================================
# 测试入口
# ==========================================
if __name__ == "__main__":
    target_ticker = "0700.HK"
    final_payload = assemble_llm_payload(target_ticker)

    # 打印前 500 个字符预览
    if final_payload:
        preview = json.dumps(final_payload, indent=2, ensure_ascii=False)
        print(f"\n🔍 Payload 结构预览 (前 500 字符):\n{preview[:500]} ...\n")
