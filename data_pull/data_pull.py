import pandas as pd
from ib_insync import *
from config import IBKR_HOST, IBKR_PORT, CLIENT_ID

class IBKRClient:
    def __init__(self):
        self.ib = IB()
        self.host = IBKR_HOST
        self.port = IBKR_PORT
        self.client_id = CLIENT_ID

    def connect(self):
        try:
            if not self.ib.isConnected():
                self.ib.connect(self.host, self.port, clientId=self.client_id)
                print("✅ [DataPull] 成功连接到 IBKR")
        except Exception as e:
            print(f"❌ [DataPull] 连接失败: {e}")
            exit()

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            print("🔌 [DataPull] 已断开连接")

    def get_hk_stock_data(self, symbol, duration='2 Y', bar_size='1 day'):
        contract = Stock(symbol, 'SEHK', 'HKD')
        print(f"📥 [DataPull] 正在拉取 {symbol}.HK 历史数据...")

        bars = self.ib.reqHistoricalData(
            contract, endDateTime='', durationStr=duration,
            barSizeSetting=bar_size, whatToShow='TRADES', useRTH=True, formatDate=1
        )

        if not bars: return None
        df = util.df(bars)
        df.set_index('date', inplace=True)
        return df

    # def get_portfolio(self, account_id=None):
    #     print("\n💰 [DataPull] 获取持仓中...")
    #     all_positions = self.ib.positions()
    #
    #     # 如果指定了账户ID，进行过滤
    #     if account_id:
    #         target_positions = [p for p in all_positions if p.account == account_id]
    #     else:
    #         target_positions = all_positions
    #
    #     data = []
    #     for p in target_positions:
    #         data.append({
    #             "代码": p.contract.symbol,
    #             "货币": p.contract.currency,
    #             "数量": p.position,
    #             "平均成本": p.avgCost,
    #             "当前市值": p.position * p.avgCost,
    #             "": p.ti
    #         })
    #     return pd.DataFrame(data)
