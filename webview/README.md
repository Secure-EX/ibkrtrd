# Webview · 本地浏览器视图

集中浏览 LLM 报告与个股技术图表。仅本地访问，不暴露到局域网。

## 启动

项目根目录执行：

```bash
python -m webview.app
```

或：

```bash
flask --app webview.app run --port 5000
```

浏览器打开 <http://127.0.0.1:5000>。

## 页面

- `/` 首页 — 报告与标的入口
- `/reports` — 阅读 `data/output/final_reports/CLAUDE_staged_*.md`，下拉切换日期
- `/charts/<ticker>` — 个股图表 + 走势分析徽章
- `/api/ohlcv/<ticker>?range=1Y|3Y|5Y|All` — JSON OHLCV + 指标
- `/api/signals/<ticker>` — JSON 走势信号
- `/admin/clear-cache` — 清空 lru_cache（运行 `main.py` 后无需重启 Flask）

## 数据来源（只读）

| 来源 | 路径 |
|---|---|
| OHLCV | `data/input/ohlcv/<ticker>_daily.csv` |
| 走势信号 | `data/output/latest/<ticker>_LLM_Payload.json` |
| 报告 | `data/output/final_reports/CLAUDE_staged_*.md` |

均通过 `config.py` 中的路径常量加载，未硬编码。

## 图表交互

- **折线 / K 线** 切换 — 客户端 `Plotly.react`，无需重新请求
- **1Y / 3Y / 5Y / All** — 服务端切片重新返回
- **均线 / 布林带** 复选框
- 4 行子图：价格 / 成交量 / MACD / RSI（含 30/70 参考线）
- 鼠标悬停统一展示同日所有数据

## 依赖

`requirements.txt` 已追加：

```
flask==3.1.0
markdown==3.7
```

Plotly.js 通过 CDN 加载（`cdn.plot.ly/plotly-2.35.2.min.js`）。如需离线使用，将 `plotly.min.js` 下载到 `static/js/`，修改 `chart.html` 的 `<script src>`。
