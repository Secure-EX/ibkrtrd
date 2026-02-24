import yfinance as yf

def get_valuation_metrics(ticker_symbol):
    """
    获取基本面估值数据 (PE/PB)
    :param ticker_symbol: Yahoo Finance 代码 (e.g., '0700.HK')
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        return {
            "PE": info.get('forwardPE', 'N/A'),
            "PB": info.get('priceToBook', 'N/A'),
            "MarketCap": info.get('marketCap', 'N/A')
        }
    except Exception as e:
        print(f"⚠️ 基本面数据获取失败: {e}")
        return {"PE": "N/A", "PB": "N/A"}
