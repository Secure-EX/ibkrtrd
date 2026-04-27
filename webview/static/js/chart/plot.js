// Plotly figure construction: traces (price/MA/BB/volume/MACD/RSI) + dark-themed layout.
import { MA_COLORS, TF_LABELS } from './constants.js';
import { col, setStatus } from './dom.js';
import { state } from './state.js';

function buildTraces() {
  const rows = state.rows;
  if (!rows.length) return [];
  const dates = col(rows, 'Date');
  const close = col(rows, 'Close');
  const traces = [];

  // --- Price (line OR candlestick) ---
  if (state.mode === 'line') {
    traces.push({
      x: dates, y: close, type: 'scatter', mode: 'lines',
      name: 'Close', line: { color: '#1f77b4', width: 1.6 },
      hovertemplate: '%{x|%Y-%m-%d}<br>Close %{y:.2f}<extra></extra>',
    });
  } else {
    traces.push({
      x: dates,
      open: col(rows, 'Open'),
      high: col(rows, 'High'),
      low: col(rows, 'Low'),
      close: close,
      type: 'candlestick',
      name: 'OHLC',
      increasing: { line: { color: '#d62728' } },
      decreasing: { line: { color: '#2ca02c' } },
    });
  }

  // --- MA overlays (dynamic periods from API response) ---
  if (state.showMA) {
    state.maPeriods.forEach((p, i) => {
      const key = `ma${p}`;
      if (rows.some((r) => r[key] != null)) {
        traces.push({
          x: dates, y: col(rows, key), type: 'scatter', mode: 'lines',
          name: `MA${p}`, line: { color: MA_COLORS[i % MA_COLORS.length], width: 1 },
          hovertemplate: `MA${p} %{y:.2f}<extra></extra>`,
        });
      }
    });
  }

  // --- Bollinger Bands (with shaded fill) ---
  if (state.showBB) {
    traces.push({
      x: dates, y: col(rows, 'bb_upper'), type: 'scatter', mode: 'lines',
      name: 'BB Upper', line: { color: 'rgba(150,150,150,0.6)', width: 1, dash: 'dot' },
      hovertemplate: 'BB Upper %{y:.2f}<extra></extra>',
    });
    traces.push({
      x: dates, y: col(rows, 'bb_lower'), type: 'scatter', mode: 'lines',
      name: 'BB Lower', line: { color: 'rgba(150,150,150,0.6)', width: 1, dash: 'dot' },
      fill: 'tonexty', fillcolor: 'rgba(150,150,150,0.08)',
      hovertemplate: 'BB Lower %{y:.2f}<extra></extra>',
    });
  }

  // --- Volume (colored by close vs prev close, Chinese convention: red up / green down) ---
  const volColors = rows.map((r, i) => {
    const prev = i > 0 ? rows[i - 1].Close : null;
    if (prev == null || r.Close == null) return '#888';
    return r.Close >= prev ? 'rgba(214,39,40,0.7)' : 'rgba(44,160,44,0.7)';
  });
  traces.push({
    x: dates, y: col(rows, 'Volume'), type: 'bar',
    name: 'Volume', marker: { color: volColors },
    yaxis: 'y2', xaxis: 'x',
    hovertemplate: 'Vol %{y:,.0f}<extra></extra>',
  });

  // --- MACD (DIF + signal line + histogram) ---
  traces.push({
    x: dates, y: col(rows, 'macd'), type: 'scatter', mode: 'lines',
    name: 'MACD', line: { color: '#1f77b4', width: 1 }, yaxis: 'y3',
  });
  traces.push({
    x: dates, y: col(rows, 'macd_signal'), type: 'scatter', mode: 'lines',
    name: 'Signal', line: { color: '#ff7f0e', width: 1 }, yaxis: 'y3',
  });
  const histColors = col(rows, 'macd_hist').map((v) =>
    v == null ? '#888' : v >= 0 ? 'rgba(214,39,40,0.6)' : 'rgba(44,160,44,0.6)'
  );
  traces.push({
    x: dates, y: col(rows, 'macd_hist'), type: 'bar',
    name: 'Hist', marker: { color: histColors }, yaxis: 'y3',
  });

  // --- RSI(14) ---
  traces.push({
    x: dates, y: col(rows, 'rsi'), type: 'scatter', mode: 'lines',
    name: 'RSI(14)', line: { color: '#9467bd', width: 1 }, yaxis: 'y4',
  });

  return traces;
}

function buildLayout() {
  const grid = 'rgba(255,255,255,0.07)';
  const axisLine = 'rgba(255,255,255,0.18)';
  const fontColor = '#e6e7eb';
  return {
    autosize: true,
    height: 760,
    margin: { l: 56, r: 24, t: 20, b: 36 },
    hovermode: 'x unified',
    paper_bgcolor: '#181c25',
    plot_bgcolor: '#181c25',
    font: { color: fontColor, size: 12 },
    legend: { orientation: 'h', y: 1.06, x: 0, bgcolor: 'rgba(0,0,0,0)', font: { color: fontColor } },
    xaxis: {
      anchor: 'y4', type: 'date',
      rangeslider: { visible: false },
      showgrid: true, gridcolor: grid, linecolor: axisLine, zerolinecolor: axisLine,
      tickfont: { color: fontColor },
    },
    yaxis: { domain: [0.50, 1.00], title: '价格', showgrid: true, gridcolor: grid, linecolor: axisLine, tickfont: { color: fontColor } },
    yaxis2: { domain: [0.36, 0.49], title: '成交量', showgrid: false, linecolor: axisLine, tickfont: { color: fontColor } },
    yaxis3: { domain: [0.18, 0.34], title: 'MACD', zeroline: true, zerolinecolor: axisLine, gridcolor: grid, linecolor: axisLine, tickfont: { color: fontColor } },
    yaxis4: {
      domain: [0.00, 0.16], title: 'RSI', range: [0, 100],
      showgrid: false, linecolor: axisLine, tickfont: { color: fontColor },
    },
    shapes: [
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 70, y1: 70, line: { color: 'rgba(255,99,99,0.6)', width: 1, dash: 'dot' } },
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 30, y1: 30, line: { color: 'rgba(95,211,145,0.6)', width: 1, dash: 'dot' } },
    ],
  };
}

export function render() {
  const chartEl = document.getElementById('chart');
  if (!state.rows.length) {
    Plotly.purge(chartEl);
    setStatus('暂无数据');
    return;
  }
  Plotly.react(chartEl, buildTraces(), buildLayout(), { responsive: true, displaylogo: false });
  const tfLabel = TF_LABELS[state.tf] || state.tf;
  setStatus(`${tfLabel} · ${state.rows.length} 个 K 棒 · 区间 ${state.range} · ${state.mode === 'line' ? '折线' : 'K 线'}`);
}

export function resize() {
  const chartEl = document.getElementById('chart');
  if (state.rows.length && chartEl) Plotly.Plots.resize(chartEl);
}
