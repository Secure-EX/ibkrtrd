import sys
import json
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import SENTIMENT_DIR

_EMPTY_RESULT = {
    "news_summary": {
        "total_articles": 0,
        "date_range": "N/A",
        "news_frequency": "None",
        "top_sources": [],
    },
    "headlines": []
}


def generate_sentiment_summary(standard_symbol: str) -> dict:
    """
    读取 fetch_stock_news() 落盘的新闻 JSON，计算基础统计摘要。
    不做 ML 情感评分——让 LLM 自行解读标题情绪。
    """
    news_file = SENTIMENT_DIR / f"{standard_symbol}_news.json"

    if not news_file.exists():
        print(f"   ⚠️ 未找到新闻文件: {news_file.name}，返回空摘要")
        return _EMPTY_RESULT

    try:
        with open(news_file, 'r', encoding='utf-8') as f:
            articles = json.load(f)
    except Exception as e:
        print(f"   ⚠️ 读取新闻文件失败: {e}")
        return _EMPTY_RESULT

    if not articles:
        return _EMPTY_RESULT

    # 基础统计
    total = len(articles)
    dates = sorted([a["date"] for a in articles if a.get("date")])
    date_range = f"{dates[0]} to {dates[-1]}" if len(dates) >= 2 else (dates[0] if dates else "N/A")

    if total > 10:
        news_frequency = "High (>10)"
    elif total >= 4:
        news_frequency = "Medium (4-10)"
    else:
        news_frequency = "Low (<4)"

    source_counts = Counter(a.get("source", "") for a in articles if a.get("source"))
    top_sources = [src for src, _ in source_counts.most_common(3)]

    headlines = [
        {
            "date": a.get("date", ""),
            "title": a.get("title", ""),
            "source": a.get("source", ""),
            # "url": a.get("url", "")
        }
        for a in articles
    ]

    return {
        "news_summary": {
            "total_articles": total,
            "date_range": date_range,
            "news_frequency": news_frequency,
            "top_sources": top_sources,
        },
        "headlines": headlines
    }


# ==========================================
# 测试入口
# ==========================================
if __name__ == "__main__":
    result = generate_sentiment_summary("0700.HK")
    print(json.dumps(result, indent=2, ensure_ascii=False))
