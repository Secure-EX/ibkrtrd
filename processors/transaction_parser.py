import csv
from datetime import datetime
import glob
import pandas as pd
from pathlib import Path
import sys

# 提升根目录优先级
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import TRANSACTIONS_DIR

def clean_ibkr_transactions():
    """
    自动扫描目录下所有的 IBKR 交易流水 CSV，只提取核心 Trades 数据，
    并进行多文件合并与自动去重。
    """
    print(f"🧹 开始扫描并清洗 IBKR 交易流水 (支持多文件合并与自动去重)...")

    # 获取所有的流水文件 (匹配 U 开头的盈透文件)
    csv_files = glob.glob(str(TRANSACTIONS_DIR / "U*.csv"))

    if not csv_files:
        print(f"❌ 找不到任何以 U 开头的 CSV 文件在目录: {TRANSACTIONS_DIR}")
        return

    # 核心：去重字典
    # 将 (股票代码, 交易时间, 数量, 价格) 作为联合主键。
    # 哪怕 1年流水和 3个月流水里有同一笔交易，由于主键一致，字典会自动覆盖，实现绝对去重。
    trades_dict = {}

    for file_path in csv_files:
        print(f"   📄 正在解析: {Path(file_path).name}")
        try:
            # 使用 utf-8-sig 以兼容可能带有 BOM 头的 CSV 文件
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                trades_header = None  # 动态捕获 Trades 区域的表头

                for row in reader:
                    if not row:
                        continue

                    # ==================================================
                    # 第一步：捕获 Trades 区域的 Header 行，建立列名映射
                    # ==================================================
                    if row[0] == 'Trades' and row[1] == 'Header':
                        trades_header = row
                        continue

                    # ==================================================
                    # 第二步：用 Header 列名安全提取 Data 行
                    # ==================================================
                    if row[0] == 'Trades' and row[1] == 'Data' and trades_header:
                        record = dict(zip(trades_header, row))

                        # 只处理股票交易，跳过期权/期货等
                        if record.get('Asset Category') != 'Stocks':
                            continue

                        symbol = record.get('Symbol', '')
                        time_str = record.get('Date/Time', '').strip()

                        # 清洗数字（去除可能存在的千位分隔符逗号，如 "2,000"）
                        qty_raw = record.get('Quantity', '0').replace(',', '')
                        price_raw = record.get('T. Price', '0').replace(',', '')
                        comm_raw = record.get('Comm/Fee', '0').replace(',', '')
                        pnl_raw = record.get('Realized P/L', '0').replace(',', '')
                        trade_code = record.get('Code', '')

                        qty = float(qty_raw) if qty_raw else 0.0
                        price = float(price_raw) if price_raw else 0.0

                        # 盈透的佣金通常是负数
                        comm = float(comm_raw) if comm_raw else 0.0
                        pnl = float(pnl_raw) if pnl_raw else 0.0

                        action = "BUY" if qty > 0 else "SELL"

                        # 生成防重复的主键 (Unique Key)
                        unique_key = (symbol, time_str, qty, price)

                        if unique_key not in trades_dict:
                            trades_dict[unique_key] = {
                                "Symbol": symbol,
                                "Time": datetime.strptime(time_str, "%Y-%m-%d, %H:%M:%S").strftime("%Y-%m-%d"),
                                "Action": action,
                                "Quantity": abs(qty),
                                "Price": price,
                                # "Commission": comm,       # 统一使用 Commission
                                "Realized_PnL": pnl,
                                # "Code": trade_code
                            }

        except Exception as e:
            print(f"   ⚠️ 解析文件 {Path(file_path).name} 时发生错误: {e}")

    # 将去重后的字典转换为列表
    parsed_data = list(trades_dict.values())

    if parsed_data:
        df = pd.DataFrame(parsed_data)

        # 按照时间从新到旧排序 (确保最新的交易记录在 CSV 的最上面)
        df['Time_Obj'] = pd.to_datetime(df['Time'])
        df = df.sort_values(by='Time_Obj', ascending=False)
        df = df.drop(columns=['Time_Obj'])

        # 生成唯一的全量流水总账
        output_csv_path = TRANSACTIONS_DIR / "transactions_master.csv"
        df.to_csv(output_csv_path, index=False, encoding='utf-8')

        print(f"✅ 清洗与去重成功！多个文件的重叠数据已被完美合并。")
        print(f"✅ 共提取 {len(df)} 条独立股票交易记录，已保存至: {output_csv_path.name}")
    else:
        print("⚠️ 未能在目录中找到任何有效的股票交易记录。")

# ==========================================
# 独立运行入口
# ==========================================
if __name__ == "__main__":
    clean_ibkr_transactions()
