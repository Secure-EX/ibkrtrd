import sys
import pandas as pd
from ib_insync import IB
from pathlib import Path
from config import PORTFOLIO_DIR, TRANSACTIONS_DIR, IBKR_HOST, IBKR_PORT, CLIENT_ID, ACCOUNT_ID, TODAY_STR, CURRENT_YEAR

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# ==========================================
# Function 1: 从 IBKR 拉取核心持仓与价格数据 (合并你的高级代码)
# ==========================================
def fetch_ibkr_base_data(ib, account_id):
    print(f"\n💰 [步骤 1] 正在拉取账户 {account_id} 的核心持仓数据...")

    ib.reqAccountUpdates(account_id)
    print("   ⏳ 正在同步 TWS 账户数据，请稍候 2 秒...")
    ib.sleep(2)

    # 1. 获取净值
    summary = ib.accountSummary(account=account_id)
    net_liq = next((float(item.value) for item in summary if item.tag == 'NetLiquidation'), 1.0)
    print(f"   📊 账户净值: {net_liq:,.2f}")

    # 🌟 将账户摘要直接落盘 (文件名带日期)
    summary_data = [{"Account": item.account, "Tag": item.tag, "Value": item.value, "Currency": item.currency} for item in summary]
    df_summary = pd.DataFrame(summary_data)
    summary_file = PORTFOLIO_DIR / f"account_summary_{TODAY_STR}.csv"
    df_summary.to_csv(summary_file, index=False, encoding='utf-8')

    portfolio_items = ib.portfolio(account_id)
    if not portfolio_items:
        print("   ⚠️ 当前无持仓")
        return [], []

    print(f"   🎯 发现 {len(portfolio_items)} 只持仓标的，正在请求实时行情...")

    tickers = {}
    pnls = {}
    symbols_for_yf = []

    # 2. 发起数据请求
    for item in portfolio_items:
        contract = item.contract
        if not contract.exchange:
            contract.exchange = contract.primaryExchange or 'SMART'

        con_id = contract.conId

        symbols_for_yf.append({
            "symbol": contract.symbol,
            "exchange": contract.primaryExchange
        })

        tickers[con_id] = ib.reqMktData(contract, snapshot=False)
        pnls[con_id] = ib.reqPnLSingle(account_id, "", con_id)

    print("   ⏳ 等待行情与盈亏数据填充 (约 3 秒)...")
    ib.sleep(3)

    # 3. 组装基础数据
    ibkr_data = []
    for item in portfolio_items:
        con_id = item.contract.conId
        ticker = tickers.get(con_id)
        pnl_obj = pnls.get(con_id)

        position = item.position
        avg_price = item.averageCost
        market_val = item.marketValue

        last_price = ticker.last if ticker and ticker.last and ticker.last > 0 else (ticker.close if ticker else item.marketPrice)
        if pd.isna(last_price) or last_price == 0:
            last_price = item.marketPrice

        prev_close = ticker.close if ticker else 0
        if prev_close and prev_close > 0:
            change = last_price - prev_close
            change_pct = (change / prev_close) * 100
        else:
            change, change_pct = 0.0, 0.0

        daily_pnl = pnl_obj.dailyPnL if pnl_obj and pnl_obj.dailyPnL else 0.0
        start_val = market_val - daily_pnl
        daily_pnl_pct = (daily_pnl / start_val * 100) if start_val != 0 else 0.0

        ibkr_data.append({
            "Symbol": item.contract.symbol,
            "% of Net Liq": round(market_val / net_liq, 4),
            "Avg Price": round(avg_price, 2),
            "Last": round(last_price, 2),
            "Change": round(change, 2),
            "Change %": round(change_pct / 100, 4),
            "Daily P&L": round(daily_pnl, 2),
            "Daily P&L %": round(daily_pnl_pct / 100, 4),
            "Market Value": round(market_val, 2),
            "Cost Basis": round(position * avg_price, 2),
            "Unrealized P&L": round(item.unrealizedPNL, 2),
            "Unrealized P&L %": round(item.unrealizedPNL / (position * avg_price), 4),
            "Position": position
        })

    # 4. 落盘当天的持仓快照
    df_positions = pd.DataFrame(ibkr_data)
    positions_file = PORTFOLIO_DIR / f"current_positions_{TODAY_STR}.csv"
    df_positions.to_csv(positions_file, index=False, encoding='utf-8')
    print(f"   ✅ 持仓明细已保存至: {positions_file.name}")

    # 清理订阅
    for t in tickers.values(): ib.cancelMktData(t.contract)
    for con_id in pnls.keys(): ib.cancelPnLSingle(account_id, "", con_id)

    return ibkr_data, symbols_for_yf

# ==========================================
# Function 2: 从 IBKR 拉取交易流水并执行 YTD 滚动追加
# ==========================================
def fetch_transactions(ib):
    print(f"\n📜 [步骤 2] 正在拉取近期交易流水...")

    # 获取近几天的执行记录
    executions = ib.reqExecutions()

    if not executions:
        print("   ⚠️ 未获取到近期的交易记录。")
        return

    trades = []
    for exec_data in executions:
        trades.append({
            "ExecId": exec_data.execution.execId, # 唯一执行ID，用于去重
            "Time": exec_data.execution.time.strftime("%Y-%m-%d %H:%M:%S") if exec_data.execution.time else "",
            "Symbol": exec_data.contract.symbol,
            "SecType": exec_data.contract.secType,
            "Side": exec_data.execution.side,
            "Shares": exec_data.execution.shares,
            "Price": exec_data.execution.price,
            "Commission": exec_data.commissionReport.commission if exec_data.commissionReport else 0.0
        })

    df_new_trades = pd.DataFrame(trades)

    # 🌟 核心逻辑：读取当年的历史数据进行追加与去重
    yearly_file = TRANSACTIONS_DIR / f"transactions_{CURRENT_YEAR}.csv"

    if yearly_file.exists():
        df_existing = pd.read_csv(yearly_file)
        # 将新旧数据合并
        df_combined = pd.concat([df_existing, df_new_trades], ignore_index=True)
        # 根据盈透唯一的 ExecId 去重，保留最新的记录
        df_combined.drop_duplicates(subset=['ExecId'], keep='last', inplace=True)
    else:
        df_combined = df_new_trades

    # 按时间降序排序（最新的交易在最上面）
    df_combined.sort_values(by="Time", ascending=False, inplace=True)

    df_combined.to_csv(yearly_file, index=False, encoding='utf-8')
    print(f"   ✅ 本年度交易流水已更新: {yearly_file.name} (总计 {len(df_combined)} 笔交易)")

# ==========================================
# 主运行入口
# ==========================================
def pull_all_ibkr_data():
    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=CLIENT_ID, readonly=True)
        print("✅ 成功连接至 IBKR TWS/Gateway!")

        # 1. 拉取并保存核心持仓
        fetch_ibkr_base_data(ib, ACCOUNT_ID)

        # 2. 拉取并覆盖更新当年的交易流水
        fetch_transactions(ib)

    except ConnectionRefusedError:
        print(f"❌ 连接 IBKR 失败：请检查 TWS/Gateway 是否已打开，且 API 端口（{IBKR_PORT}）设置正确。")
    except Exception as e:
        print(f"❌ 发生未知错误: {str(e)}")
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\n🔌 IBKR 连接已安全断开。")

if __name__ == "__main__":
    pull_all_ibkr_data()
