import sys
import json
import requests
import akshare as ak
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import SENTIMENT_DIR

# 本地缓存文件：避免每次都重新拉取 stock_hk_spot_em 全量数据（约3分钟）
_NAME_CACHE_FILE = SENTIMENT_DIR / "_hk_name_cache.json"


def _get_ak_symbol(standard_symbol: str) -> str:
    """0700.HK → 00700"""
    return standard_symbol.split('.')[0].zfill(5)


def _get_cn_name(standard_symbol: str) -> str:
    """
    获取港股简体中文名称（如"腾讯控股"）。
    优先读本地缓存，缓存未命中才拉取 stock_hk_spot_em 并更新缓存。
    """
    ak_symbol = _get_ak_symbol(standard_symbol)

    # 读缓存
    cache = {}
    if _NAME_CACHE_FILE.exists():
        try:
            with open(_NAME_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except Exception:
            pass

    if ak_symbol in cache:
        return cache[ak_symbol]

    # 缓存未命中 → 拉取全量港股名称列表（耗时约1-3分钟，只需执行一次）
    print(f"   📋 首次构建港股名称缓存 (stock_hk_spot_em，需1-3分钟，后续瞬时读取)...")
    try:
        df = ak.stock_hk_spot_em()
        for _, row in df.iterrows():
            code = str(row.get('代码', ''))
            name = str(row.get('名称', ''))
            if code and name:
                cache[code] = name
        SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)
        with open(_NAME_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"      ✅ 名称缓存已保存 ({len(cache)} 只港股)")
    except Exception as e:
        print(f"   ⚠️ 获取港股中文名称失败: {e}")

    return cache.get(ak_symbol, standard_symbol)


def _parse_date(ts) -> str:
    """将 Unix 时间戳、datetime 字符串或 RSS pubDate 统一转换为 YYYY-MM-DD"""
    try:
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        if isinstance(ts, str):
            # ISO datetime: "2026-04-01 22:15:00"
            if len(ts) >= 10 and ts[4] == '-':
                return ts[:10]
            # RSS pubDate: "Wed, 02 Apr 2026 05:00:00 GMT"
            for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
                try:
                    return datetime.strptime(ts, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
    except Exception:
        pass
    return datetime.utcnow().strftime("%Y-%m-%d")


def _fetch_google_rss_news(cn_name: str, cutoff_date: datetime) -> list[dict]:
    """用简体中文公司名从 Google News RSS 拉取新闻"""
    results = []
    try:
        query = requests.utils.quote(f"{cn_name} 股票")
        url = (
            f"https://news.google.com/rss/search?q={query}"
            f"&hl=zh-CN&gl=HK&ceid=HK:zh-Hans"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return results
        for item in channel.findall("item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            date_str = _parse_date(item.findtext("pubDate", ""))
            if datetime.strptime(date_str, "%Y-%m-%d") < cutoff_date:
                continue
            results.append({
                "title": title,
                "source": "Google News",
                "date": date_str,
                "url": link,
                "data_vendor": "google_rss"
            })
    except Exception as e:
        print(f"   ⚠️ Google News RSS 拉取异常: {e}")
    return results


def _fetch_akshare_news(ak_symbol: str, cutoff_date: datetime) -> list[dict]:
    """从 AkShare stock_news_em (东方财富) 拉取最近100条新闻并过滤日期"""
    results = []
    try:
        df = ak.stock_news_em(symbol=ak_symbol)
        if df is None or df.empty:
            return results
        for _, row in df.iterrows():
            date_str = _parse_date(str(row.get('发布时间', '')))
            if datetime.strptime(date_str, "%Y-%m-%d") < cutoff_date:
                continue
            results.append({
                "title": str(row.get('新闻标题', '')),
                "source": str(row.get('文章来源', '东方财富')),
                "date": date_str,
                "url": str(row.get('新闻链接', '')),
                "data_vendor": "akshare_em"
            })
    except Exception as e:
        print(f"   ⚠️ AkShare stock_news_em 拉取异常: {e}")
    return results


def fetch_stock_news(standard_symbol: str, days_back: int = 30) -> list[dict]:
    """
    拉取指定港股近 days_back 天的新闻。
    主引擎: AkShare stock_news_em（东方财富，及时性更好，约返回10条）。
    补充引擎: Google News RSS（始终运行，约返回10条，两源直接合并）。
    结果落盘至 data/input/sentiment/{symbol}_news.json，最多保留20条。
    """
    ak_symbol = _get_ak_symbol(standard_symbol)
    cutoff_date = datetime.utcnow() - timedelta(days=days_back)

    # 获取中文公司名
    cn_name = _get_cn_name(standard_symbol)
    print(f"   📰 正在拉取 {standard_symbol}（{cn_name}）近 {days_back} 天新闻...")

    articles: list[dict] = []

    # 主引擎：AkShare stock_news_em（东方财富，及时性更好）
    print(f"      [1/2] AkShare stock_news_em 拉取 (symbol={ak_symbol})...")
    articles.extend(_fetch_akshare_news(ak_symbol, cutoff_date))
    print(f"      AkShare 获得 {len(articles)} 条有效新闻")

    # 补充引擎：Google News RSS（始终运行，两源 URL 格式不同无法去重，直接合并）
    print(f"      [2/2] Google News RSS 补充搜索: 「{cn_name} 股票」...")
    before = len(articles)
    articles.extend(_fetch_google_rss_news(cn_name, cutoff_date))
    print(f"      Google News 新增 {len(articles) - before} 条（共 {len(articles)} 条）")

    if not articles:
        print(f"   ⚠️ {standard_symbol} 未找到任何新闻，将返回空列表")

    # 排序（日期降序）并截取前 20 条
    articles.sort(key=lambda x: x["date"], reverse=True)
    articles = articles[:20]

    # 落盘
    SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = SENTIMENT_DIR / f"{standard_symbol}_news.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(articles, f, indent=4, ensure_ascii=False)
    print(f"      ✅ 新闻数据已落盘: {output_file.name} ({len(articles)} 条)")

    return articles


# ==========================================
# 测试入口
# ==========================================
if __name__ == "__main__":
    result = fetch_stock_news("0700.HK", days_back=30)
    print(f"\n共获取 {len(result)} 条新闻")
    for item in result[:5]:
        print(f"  [{item['date']}] [{item['data_vendor']}] {item['title']}")
