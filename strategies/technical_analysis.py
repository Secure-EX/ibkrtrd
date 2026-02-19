import pandas as pd
import pandas_ta as ta

class LocalAnalyzer:
    def __init__(self, df):
        self.df = df.copy()

    def add_indicators(self):
        # 1. 趋势
        self.df['SMA_20'] = ta.sma(self.df['close'], length=20)
        self.df['SMA_60'] = ta.sma(self.df['close'], length=60)

        # MACD (返回三列)
        macd = ta.macd(self.df['close'])
        self.df = pd.concat([self.df, macd], axis=1)

        # 2. 震荡
        self.df['RSI'] = ta.rsi(self.df['close'], length=14)
        kdj = ta.kdj(self.df['high'], self.df['low'], self.df['close'])
        self.df = pd.concat([self.df, kdj], axis=1)

        # 3. 布林带 (返回三列: BBL, BBM, BBU)
        bbands = ta.bbands(self.df['close'], length=20, std=2)
        self.df = pd.concat([self.df, bbands], axis=1)

    def analyze_trend(self):
        latest = self.df.iloc[-1]
        if latest['SMA_20'] > latest['SMA_60'] and latest['close'] > latest['SMA_20']:
            return "上涨趋势 (Bullish)"
        elif latest['SMA_20'] < latest['SMA_60'] and latest['close'] < latest['SMA_20']:
            return "下跌趋势 (Bearish)"
        return "震荡"

    def analyze_valuation_technical(self):
        last_price = self.df['close'].iloc[-1]
        year_high = self.df['close'].max()
        year_low = self.df['close'].min()
        percentile = (last_price - year_low) / (year_high - year_low) * 100

        status = "适中"
        if percentile < 20: status = "低估 (底部)"
        elif percentile > 80: status = "高估 (顶部)"
        return status, f"{percentile:.2f}%"

    def generate_signal(self):
        latest = self.df.iloc[-1]
        prev = self.df.iloc[-2]
        signals = []
        score = 0

        # 动态获取列名 (防止 BBL_20_2.0 报错)
        macd_col = [c for c in self.df.columns if c.startswith('MACD_')][0]
        macds_col = [c for c in self.df.columns if c.startswith('MACDs_')][0]
        bbl_col = [c for c in self.df.columns if c.startswith('BBL_')][0]

        # MACD
        if latest[macd_col] > latest[macds_col] and prev[macd_col] <= prev[macds_col]:
            signals.append("MACD 金叉")
            score += 2

        # RSI
        if latest['RSI'] < 30:
            signals.append("RSI 超卖")
            score += 2
        elif latest['RSI'] > 70:
            signals.append("RSI 超买")
            score -= 2

        # BOLL
        if latest['close'] < latest[bbl_col]:
            signals.append("跌破布林下轨")

        decision = "观望 (Wait)"
        if score >= 3: decision = "买入 (Buy)"
        elif score <= -3: decision = "卖出 (Sell)"

        return decision, signals
