# 这破玩意有问题，只能看看，目前BOLL线这边报错了，后续还要按类拆分一下


import pandas as pd
import pandas_ta as ta
import yfinance as yf
from ib_insync import *
import datetime

# ==========================================
# 1. 基础配置与连接模块
# ==========================================
class IBKRClient:
    def __init__(self, host='127.0.0.1', port=7496, client_id=1):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id

    def connect(self):
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            print("✅ 成功连接到 IBKR TWS/Gateway")
        except Exception as e:
            print(f"❌ 连接失败: {e}")
            print("请检查 TWS 是否打开，以及 API 端口设置是否正确。")
            exit()

    def disconnect(self):
        self.ib.disconnect()
        print("🔌 已断开连接")

    def get_hk_stock_data(self, symbol, duration='3 Y', bar_size='1 day'):
        """
        获取港股历史数据
        注意：IBKR 中腾讯的代码是 '700' 而不是 '00700'
        """
        # 定义合约：腾讯控股 (700), 港交所 (SEHK), 港币 (HKD)
        # 如果是美股，exchange='SMART', currency='USD'
        contract = Stock(symbol, 'SEHK', 'HKD')

        print(f"📥 正在拉取 {symbol}.HK 的历史数据...")
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=True,  # 只看常规交易时间
            formatDate=1
        )

        if not bars:
            print("⚠️ 未获取到数据，请检查代码或权限。")
            return None

        # 转换为 DataFrame
        df = util.df(bars)
        df.set_index('date', inplace=True)
        return df

    def get_portfolio(self):
        """获取当前持仓"""

        print("\n💰 正在获取当前持仓...")
        positions = self.ib.positions()
        portfolio_data = []
        for p in positions:
            portfolio_data.append({
                "代码": p.contract.symbol,
                "数量": p.position,
                "平均成本": p.avgCost,
                "当前市值": p.position * p.avgCost # 这是一个估算，准确市值需要请求实时行情
            })
        return pd.DataFrame(portfolio_data)

    def get_trades(self):
        """获取当日/近期交易记录"""
        print("\n📝 正在获取交易记录...")
        fills = self.ib.executions() # 获取执行记录
        trade_data = []
        for fill in fills:
            # 简单去重逻辑，只取关键信息
            trade_data.append({
                "时间": fill.time,
                "代码": fill.contract.symbol,
                "方向": fill.execution.side, # BOT (买入) / SLD (卖出)
                "数量": fill.execution.shares,
                "价格": fill.execution.price
            })
        return pd.DataFrame(trade_data)

# ==========================================
# 2. 本地技术面分析与策略模块
# ==========================================
class LocalAnalyzer:
    def __init__(self, df):
        self.df = df.copy()

    def add_indicators(self):
        """添加技术指标"""
        # 1. 趋势指标 (Trend)
        self.df['SMA_20'] = ta.sma(self.df['close'], length=20) # 月线
        self.df['SMA_60'] = ta.sma(self.df['close'], length=60) # 季线

        # MACD
        macd = ta.macd(self.df['close'])
        self.df = pd.concat([self.df, macd], axis=1) # MACD_12_26_9, MACDh, MACDs

        # 2. 震荡指标 (Momentum)
        self.df['RSI'] = ta.rsi(self.df['close'], length=14)

        # KDJ (Pandas TA 默认叫 K, D, J)
        kdj = ta.kdj(self.df['high'], self.df['low'], self.df['close'])
        self.df = pd.concat([self.df, kdj], axis=1)

        # 3. 波动率指标 (Volatility)
        bbands = ta.bbands(self.df['close'], length=20, std=2)
        self.df = pd.concat([self.df, bbands], axis=1)

    def analyze_trend(self):
        """分析走势结构"""
        latest = self.df.iloc[-1]
        trend = "震荡"

        # 均线多头排列
        if latest['SMA_20'] > latest['SMA_60'] and latest['close'] > latest['SMA_20']:
            trend = "上涨趋势 (Bullish)"
        elif latest['SMA_20'] < latest['SMA_60'] and latest['close'] < latest['SMA_20']:
            trend = "下跌趋势 (Bearish)"

        return trend

    def analyze_valuation_technical(self):
        """
        基于技术面的相对估值分析 (Price Percentile)
        计算当前价格在过去一年中的位置
        """
        last_price = self.df['close'].iloc[-1]
        year_high = self.df['close'].max()
        year_low = self.df['close'].min()

        percentile = (last_price - year_low) / (year_high - year_low) * 100

        status = "适中"
        if percentile < 20: status = "低估 (底部区域)"
        elif percentile > 80: status = "高估 (顶部区域)"

        return status, f"{percentile:.2f}%"

    def generate_signal(self):
        """生成交易信号 Segmentation"""
        latest = self.df.iloc[-1]
        prev = self.df.iloc[-2]

        signals = []
        score = 0 # 简单打分 -5 到 +5

        # 1. MACD 信号
        if latest['MACD_12_26_9'] > latest['MACDs_12_26_9'] and prev['MACD_12_26_9'] <= prev['MACDs_12_26_9']:
            signals.append("MACD 金叉")
            score += 2
        elif latest['MACD_12_26_9'] < latest['MACDs_12_26_9']:
            score -= 1

        # 2. RSI 信号
        if latest['RSI'] < 30:
            signals.append("RSI 超卖 (反弹机会)")
            score += 2
        elif latest['RSI'] > 70:
            signals.append("RSI 超买 (回调风险)")
            score -= 2

        # 3. 布林带信号
        if latest['close'] < latest['BBL_20_2.0']:
            signals.append("跌破布林下轨 (极度弱势或超跌)")

        # 综合评判
        final_decision = "观望 (Wait)"
        if score >= 3: final_decision = "买入 (Buy)"
        elif score <= -3: final_decision = "卖出 (Sell)"

        return final_decision, signals

# ==========================================
# 3. 主程序入口
# ==========================================
if __name__ == "__main__":
    # 1. 初始化并连接
    app = IBKRClient(port=7496) # 注意：TWS 7496 实盘 默认 7497 模拟盘, Gateway 默认 4001
    app.connect()

    try:
        # 2. 获取数据 (以腾讯为例，IBKR代码 700)
        stock_symbol = "700"
        df = app.get_hk_stock_data(stock_symbol)

        if df is not None:
            # 3. 运行本地分析
            analyzer = LocalAnalyzer(df)
            analyzer.add_indicators()

            trend = analyzer.analyze_trend()
            val_status, val_score = analyzer.analyze_valuation_technical()
            decision, reasons = analyzer.generate_signal()

            # 4. 获取基本面估值 (PE/PB) - 补充 IBKR 缺失的数据
            # yfinance 使用代码 '0700.HK'
            try:
                ticker = yf.Ticker("0700.HK")
                pe_ratio = ticker.info.get('forwardPE', 'N/A')
                pb_ratio = ticker.info.get('priceToBook', 'N/A')
            except:
                pe_ratio, pb_ratio = "N/A", "N/A"

            # 5. 输出报告
            print("\n" + "="*40)
            print(f"📊 分析报告: 腾讯控股 (00700.HK)")
            print("="*40)
            print(f"当前价格: {df['close'].iloc[-1]} HKD")
            print(f"📅 数据日期: {df.index[-1]}")
            print("-" * 20)
            print(f"1️⃣ 走势分析: {trend}")
            print(f"2️⃣ 估值分析 (技术): {val_status} (分位点: {val_score})")
            print(f"   估值分析 (基本面): Forward PE: {pe_ratio}, PB: {pb_ratio}")
            print("-" * 20)
            print(f"3️⃣ 信号监测: {reasons}")
            print(f"🚦 综合建议: 【{decision}】")
            print("="*40)

        # 6. 获取个人持仓与交易
        my_positions = app.get_portfolio()
        if not my_positions.empty:
            print("\n💼 我的持仓:")
            print(my_positions)
        else:
            print("\n💼 当前无持仓")

    except Exception as e:
        print(f"运行出错: {e}")
    finally:
        app.disconnect()
