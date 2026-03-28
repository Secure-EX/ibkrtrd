import sys
from datetime import datetime
import pandas as pd
from ib_insync import IB, Stock, util
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import PORTFOLIO_DIR, OHLCV_DIR, IBKR_HOST, IBKR_PORT, CLIENT_ID, ACCOUNT_ID, get_today_str, LOOKBACK_YEARS

# ==========================================
# Function 1: 从 IBKR 拉取核心持仓与价格数据 (多币种隔离与汇率版)
# ==========================================
def fetch_ibkr_base_data(ib, account_id):
    print(f"\n💰 [步骤 1] 正在拉取账户 {account_id} 的核心持仓数据...")

    # 激活分币种账户数据流
    ib.reqAccountUpdates(account_id)
    print("   ⏳ 正在同步 TWS 分币种账户数据，请稍候 2 秒...")
    ib.sleep(2)

    # ---------------------------------------------------------
    # 1. 拆解各币种现金池与实时汇率
    # ---------------------------------------------------------
    account_values = ib.accountValues(account_id)
    cash_by_curr = {}
    fx_rates = {}


    for item in account_values:
        try:
            val = float(item.value)
        except (ValueError, TypeError):
            continue

        if item.tag in ['TotalCashBalance', 'TotalCashValue'] and item.currency != 'BASE':
            cash_by_curr[item.currency] = val

        elif item.tag == 'ExchangeRate' and item.currency != 'BASE':
            fx_rates[item.currency] = val

    portfolio_items = ib.portfolio(account_id)
    if not portfolio_items:
        print("   ⚠️ 当前无持仓")
        return [], []

    print(f"   🎯 发现 {len(portfolio_items)} 只持仓标的，正在请求深度数据...")

    # ---------------------------------------------------------
    # 2. 统计各原生币种的持仓总市值 (Market Value by Currency)
    # ---------------------------------------------------------
    mkt_val_by_curr = {}
    for item in portfolio_items:
        curr = item.contract.currency
        mkt_val_by_curr[curr] = mkt_val_by_curr.get(curr, 0.0) + item.marketValue

    # ---------------------------------------------------------
    # 3. 计算绝对纯净的分币种净值 (Net Liquidation by Currency)
    # 逻辑: 加币净资产 = 加币现金池 + 加币股票总市值 (零汇率折算)
    # 逻辑: 港币净资产 = 港币现金池 + 港币股票总市值 (零汇率折算)
    # 逻辑: 美元净资产 = 美元现金池 + 美元股票总市值 (零汇率折算)
    # ---------------------------------------------------------
    net_liq_by_curr = {}
    all_currencies = set(cash_by_curr.keys()).union(set(mkt_val_by_curr.keys()))
    if 'BASE' in all_currencies:
        all_currencies.remove('BASE')

    summary_rows = []

    # 用于累加计算 BASE (基础货币，通常是 CAD)
    calc_base_cash = 0.0
    calc_base_mkt = 0.0
    calc_base_net = 0.0

    for curr in all_currencies:
        if curr == 'BASE': continue # 跳过盈透自己的折算汇总

        c_cash = cash_by_curr.get(curr, 0.0)
        c_mkt = mkt_val_by_curr.get(curr, 0.0)
        c_net = c_cash + c_mkt

        # 提取汇率，如果没有则默认为 1.0 (比如基础货币本身)
        fx = fx_rates.get(curr, 1.0)
        net_liq_by_curr[curr] = c_net

        # 累加到自主计算的 BASE 总池子里
        calc_base_cash += c_cash * fx
        calc_base_mkt += c_mkt * fx
        calc_base_net += c_net * fx

        summary_rows.append({
            "Currency": curr,
            "Total Cash": c_cash,
            "Market Value": c_mkt,
            "Net Liquidation": c_net,
            "Exchange Rate": fx   # 对 BASE 的汇率
        })

    # 补上基础货币总览，使用IBKR 官方汇率和基础货币价值直接用汇率加权算出的 BASE_TOTAL_CALC
    summary_rows.append({
        "Currency": "BASE_TOTAL_CALC",
        "Total Cash": calc_base_cash,
        "Market Value": calc_base_mkt,
        "Net Liquidation": calc_base_net,
        "Exchange Rate": 1.0
    })

    # 落盘 1: 账户资金摘要 (分币种绝对隔离)
    df_summary = pd.DataFrame(summary_rows)
    summary_file = PORTFOLIO_DIR / f"account_summary_{get_today_str()}.csv"
    df_summary.to_csv(summary_file, index=False, encoding='utf-8')
    print(f"   📊 账户分币种摘要已保存: {summary_file.name}")

    # ---------------------------------------------------------
    # 4. 获取个股行情与盈亏
    # ---------------------------------------------------------
    tickers = {}
    pnls = {}
    names = {}
    min_sizes = {}
    min_ticks = {}
    symbols_for_yf = []

    # 2. 发起数据请求
    for item in portfolio_items:
        contract = item.contract
        if not contract.exchange:
            contract.exchange = contract.primaryExchange or 'SMART'

        con_id = contract.conId
        symbols_for_yf.append({"symbol": contract.symbol, "exchange": contract.primaryExchange})

        details = ib.reqContractDetails(contract)
        names[con_id] = details[0].longName if details else "N/A"

        # 提取并保存一手股数(minSize)和最小跳动价位(minTick)
        # 如果获取不到，默认给 1 股和 0.01 的精度兜底
        min_sizes[con_id] = details[0].minSize if details else 1.0
        min_ticks[con_id] = details[0].minTick if details else 0.01

        tickers[con_id] = ib.reqMktData(contract, snapshot=False)
        pnls[con_id] = ib.reqPnLSingle(account_id, "", con_id)

    print("   ⏳ 等待行情与盈亏数据填充 (约 3 秒)...")
    ib.sleep(3)

    # ---------------------------------------------------------
    # 5. 组装持仓明细 (融入用户自定义指标与本地币种核算)
    # ---------------------------------------------------------
    ibkr_data = []
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    for item in portfolio_items:
        con_id = item.contract.conId
        ticker = tickers.get(con_id)
        pnl_obj = pnls.get(con_id)

        local_curr = item.contract.currency

        position = item.position
        avg_price = item.averageCost
        market_val = item.marketValue

        # 如果是盘中实盘交易，ticker.last 存在，用最新的成交价
        if ticker and ticker.last and ticker.last > 0:
            last_price = ticker.last
        else:
            # 如果是周末休市没有 last，绝对不能用 ticker.close(那会拿到周四的昨收)
            # 直接使用盈透风控系统给出的周末盯市结算价 (精确等于周五收盘价)
            last_price = item.marketPrice

        if pd.isna(last_price) or last_price == 0:
            last_price = 0.0 # 极端兜底

        # 反向推导：用服务器给的真实 Daily P&L 反推单日涨跌幅
        daily_pnl = pnl_obj.dailyPnL if pnl_obj and pnl_obj.dailyPnL else 0.0

        # change是模拟出来的，但是有时候会失真，不过问题不大
        if position != 0:
            change = daily_pnl / position
        else:
            change = 0.0

        # 算出精准的昨日收盘价
        prev_close = last_price - change
        change_pct = (change / prev_close) if prev_close != 0 else 0.0

        # 使用同币种的净值做分母
        local_net_liq = net_liq_by_curr.get(local_curr, 1.0)
        weight_pct = (market_val / local_net_liq) if local_net_liq > 0 else 0.0

        # 盈透官方界面上的 Daily P&L % 根本不是个股涨跌幅
        # 而是 “单日账户净值贡献率” (Portfolio Return Contribution)
        daily_pnl_pct = (daily_pnl / local_net_liq) if local_net_liq > 0 else 0.0

        ibkr_data.append({
            "Symbol": item.contract.symbol,
            "Company Name (EN)": names.get(con_id, "N/A"),
            "Currency": local_curr,
            "Net Liq Ratio": weight_pct,
            "Avg Price": avg_price,
            "Last": last_price,
            "Change": change,
            "Change Ratio": change_pct,
            "Daily P&L": daily_pnl,
            "Daily P&L Ratio": daily_pnl_pct,
            "Market Value": market_val,
            "Cost Basis": position * avg_price,
            "Unrealized P&L": item.unrealizedPNL,
            "Unrealized P&L Ratio": (item.unrealizedPNL / abs(position * avg_price)) if (position * avg_price) != 0 else 0.0,
            "Position": position,
            # 挂载交易规则防拒单字段
            "Board Lot": min_sizes.get(con_id, 1.0),
            "Min Tick": min_ticks.get(con_id, 0.01)
        })

    # 6. 落盘当天的持仓快照
    df_positions = pd.DataFrame(ibkr_data)
    positions_file = PORTFOLIO_DIR / f"current_positions_{get_today_str()}.csv"
    df_positions.to_csv(positions_file, index=False, encoding='utf-8')
    print(f"   ✅ 持仓明细已保存至: {positions_file.name}")

    # 清理订阅
    for t in tickers.values(): ib.cancelMktData(t.contract)
    for con_id in pnls.keys(): ib.cancelPnLSingle(account_id, "", con_id)

    return ibkr_data, symbols_for_yf

# ==========================================
# Function 2: 通过 IBKR 拉取历史日 K 线 (替代 AkShare 主引擎)
# ==========================================
def fetch_ibkr_ohlcv(ib, standard_symbol: str, currency: str, years: int = LOOKBACK_YEARS):
    """
    通过 IBKR TWS API 拉取历史日 K 线数据，支持首次全量拉取和后续增量填充。
    IBKR 的 bar.average 是真实 VWAP，精度优于第三方数据源的估算值。

    参数:
        ib: 已连接的 IB 实例
        standard_symbol: 标准代码 (如 "0700.HK" 或 "AAPL")
        currency: 计价货币 (如 "HKD", "USD")
        years: 回溯年限，默认 LOOKBACK_YEARS
    """
    file_path = OHLCV_DIR / f"{standard_symbol}_daily.csv"

    # 构建合约对象
    if currency == 'HKD':
        raw_symbol = standard_symbol.split('.')[0].lstrip('0')
        contract = Stock(raw_symbol, 'SEHK', 'HKD')
    else:
        contract = Stock(standard_symbol, 'SMART', currency)

    ib.qualifyContracts(contract)

    # --------------------------------------------------
    # 增量检测：如果本地已有数据，只拉缺口部分
    # --------------------------------------------------
    df_existing = None

    if file_path.exists():
        df_existing = pd.read_csv(file_path)
        last_date_str = df_existing['Date'].max()
        last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
        days_gap = (datetime.now() - last_dt).days

        if days_gap <= 1:
            print(f"   ℹ️ {standard_symbol} 本地数据已是最新，跳过拉取。")
            return True

        # 增量拉取：用天数 + 小缓冲区
        if days_gap <= 360:
            duration_str = f'{days_gap + 10} D'
        else:
            # 缺口超过一年，重新全量拉取更可靠
            duration_str = f'{years} Y'
            df_existing = None  # 放弃旧数据，全量覆盖

        print(f"   📥 [增量模式] 拉取最近 {days_gap} 天的新数据...")
    else:
        duration_str = f'{years} Y'
        print(f"   📥 [全量模式] 首次拉取过去 {years} 年的完整数据...")

    # --------------------------------------------------
    # 向 IBKR 发起历史数据请求
    # --------------------------------------------------
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr=duration_str,
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars:
        raise RuntimeError(f"IBKR 未能获取到 {standard_symbol} 的任何历史数据")

    # --------------------------------------------------
    # 转换为下游兼容的 DataFrame 格式
    # --------------------------------------------------
    df_new = util.df(bars)

    # IBKR 的 average 字段 = 当日真实 VWAP，乘以成交量即得成交额
    df_new['Turnover_Value'] = df_new['average'] * df_new['volume']
    df_new['Date'] = pd.to_datetime(df_new['date']).dt.strftime('%Y-%m-%d')

    df_new = df_new.rename(columns={
        'open': 'Open', 'high': 'High', 'low': 'Low',
        'close': 'Close', 'volume': 'Volume'
    })

    columns_to_keep = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover_Value']
    df_new = df_new[columns_to_keep].copy()

    # --------------------------------------------------
    # 如果是增量，合并新旧数据并去重
    # --------------------------------------------------
    if df_existing is not None:
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset='Date', keep='last', inplace=True)
    else:
        df_combined = df_new

    df_combined.sort_values('Date', ascending=True, inplace=True)
    df_combined.to_csv(file_path, index=False, encoding='utf-8')

    print(f"   ✅ [IBKR] {standard_symbol} 日K线已保存 (共 {len(df_combined)} 条交易日)")
    return True

# ==========================================
# 主运行入口
# ==========================================
def pull_all_ibkr_data():
    """
    连接 IBKR 并拉取全量持仓数据。
    v2.0: 返回 ib 连接对象，供后续历史数据拉取复用。

    返回:
        tuple: (ib, ibkr_data, symbols_for_yf)

    异常:
        ConnectionError: TWS/Gateway 未启动或端口错误
        其他异常向上抛出，由 main.py 捕获处理
    """
    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=CLIENT_ID, readonly=True)
        print("✅ 成功连接至 IBKR TWS/Gateway!")

        ibkr_data, symbols_for_yf = fetch_ibkr_base_data(ib, ACCOUNT_ID)
        return ib, ibkr_data, symbols_for_yf

    except ConnectionRefusedError:
        raise ConnectionError(
            f"连接 IBKR 失败：请检查 TWS/Gateway 是否已打开，且 API 端口（{IBKR_PORT}）设置正确。"
        )
    except Exception:
        if ib.isConnected():
            ib.disconnect()
        raise

if __name__ == "__main__":
    ib, data, syms = pull_all_ibkr_data()
    if ib.isConnected():
        ib.disconnect()
