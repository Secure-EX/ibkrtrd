// Plotly figure construction. Theme-aware: every render reads CSS variables
// from the current :root data-theme so a theme toggle just calls render().
import { TF_LABELS } from './constants.js';
import { col, setStatus } from './dom.js';
import { state } from './state.js';

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function themeColors() {
  return {
    bg0: cssVar('--bg-0'),
    bg1: cssVar('--bg-1'),
    grid: cssVar('--grid'),
    line: cssVar('--line'),
    fg0: cssVar('--fg-0'),
    fg1: cssVar('--fg-1'),
    fg2: cssVar('--fg-2'),
    accent: cssVar('--accent'),
    info: cssVar('--info'),
    up: cssVar('--up'),
    down: cssVar('--down'),
    upSoft: cssVar('--up-soft'),
    downSoft: cssVar('--down-soft'),
  };
}

function buildTraces(C) {
  const rows = state.rows;
  if (!rows.length) return [];
  const dates = col(rows, 'Date');
  const close = col(rows, 'Close');
  const traces = [];

  // --- Price (line OR candlestick) ---
  if (state.mode === 'line') {
    traces.push({
      x: dates, y: close, type: 'scatter', mode: 'lines',
      name: 'Close', line: { color: C.info, width: 1.6 },
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
      increasing: { line: { color: C.up }, fillcolor: C.up },
      decreasing: { line: { color: C.down }, fillcolor: C.down },
    });
  }

  // --- MA overlays (dynamic periods from API response) ---
  const maColors = [C.down, C.up, C.accent, C.info, '#bd93f9'];
  if (state.showMA) {
    state.maPeriods.forEach((p, i) => {
      const key = `ma${p}`;
      if (rows.some((r) => r[key] != null)) {
        traces.push({
          x: dates, y: col(rows, key), type: 'scatter', mode: 'lines',
          name: `MA${p}`, line: { color: maColors[i % maColors.length], width: 1.1 },
          hovertemplate: `MA${p} %{y:.2f}<extra></extra>`,
        });
      }
    });
  }

  // --- Bollinger Bands ---
  if (state.showBB) {
    traces.push({
      x: dates, y: col(rows, 'bb_upper'), type: 'scatter', mode: 'lines',
      name: 'BB Upper', line: { color: C.fg2, width: 1, dash: 'dot' },
      hovertemplate: 'BB Upper %{y:.2f}<extra></extra>',
    });
    traces.push({
      x: dates, y: col(rows, 'bb_lower'), type: 'scatter', mode: 'lines',
      name: 'BB Lower', line: { color: C.fg2, width: 1, dash: 'dot' },
      fill: 'tonexty', fillcolor: 'rgba(150,150,150,0.06)',
      hovertemplate: 'BB Lower %{y:.2f}<extra></extra>',
    });
  }

  // --- Volume (Chinese convention: red up / green down) ---
  const volColors = rows.map((r, i) => {
    const prev = i > 0 ? rows[i - 1].Close : null;
    if (prev == null || r.Close == null) return C.fg2;
    return r.Close >= prev ? C.upSoft || C.up : C.downSoft || C.down;
  });
  traces.push({
    x: dates, y: col(rows, 'Volume'), type: 'bar',
    name: 'Volume', marker: { color: volColors },
    yaxis: 'y2', xaxis: 'x',
    hovertemplate: 'Vol %{y:,.0f}<extra></extra>',
  });

  // --- MACD (DIF + signal + histogram) ---
  traces.push({
    x: dates, y: col(rows, 'macd'), type: 'scatter', mode: 'lines',
    name: 'MACD', line: { color: C.info, width: 1 }, yaxis: 'y3',
  });
  traces.push({
    x: dates, y: col(rows, 'macd_signal'), type: 'scatter', mode: 'lines',
    name: 'Signal', line: { color: C.accent, width: 1 }, yaxis: 'y3',
  });
  const histColors = col(rows, 'macd_hist').map((v) =>
    v == null ? C.fg2 : v >= 0 ? C.up : C.down
  );
  traces.push({
    x: dates, y: col(rows, 'macd_hist'), type: 'bar',
    name: 'Hist', marker: { color: histColors }, yaxis: 'y3',
  });

  // --- RSI(14) ---
  traces.push({
    x: dates, y: col(rows, 'rsi'), type: 'scatter', mode: 'lines',
    name: 'RSI(14)', line: { color: '#bd93f9', width: 1 }, yaxis: 'y4',
  });

  return traces;
}

function buildLayout(C) {
  return {
    autosize: true,
    margin: { l: 56, r: 24, t: 16, b: 32 },
    hovermode: 'x unified',
    paper_bgcolor: C.bg0,
    plot_bgcolor: C.bg0,
    font: { color: C.fg1, size: 11, family: 'IBM Plex Mono, monospace' },
    legend: {
      orientation: 'h', y: 1.04, x: 0,
      bgcolor: 'rgba(0,0,0,0)',
      font: { color: C.fg1, size: 10 },
    },
    xaxis: {
      anchor: 'y4', type: 'date',
      rangeslider: { visible: false },
      showgrid: true, gridcolor: C.grid, linecolor: C.line, zerolinecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    yaxis: {
      domain: [0.50, 1.00], title: { text: '价格', font: { color: C.fg2, size: 10 } },
      showgrid: true, gridcolor: C.grid, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    yaxis2: {
      domain: [0.36, 0.49], title: { text: '量', font: { color: C.fg2, size: 10 } },
      showgrid: false, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    yaxis3: {
      domain: [0.18, 0.34], title: { text: 'MACD', font: { color: C.fg2, size: 10 } },
      zeroline: true, zerolinecolor: C.line, gridcolor: C.grid, linecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    yaxis4: {
      domain: [0.00, 0.16], title: { text: 'RSI', font: { color: C.fg2, size: 10 } },
      range: [0, 100], showgrid: false, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    shapes: [
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 70, y1: 70, line: { color: C.down, width: 1, dash: 'dot' } },
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 30, y1: 30, line: { color: C.up, width: 1, dash: 'dot' } },
    ],
  };
}

export function render() {
  const chartEl = document.getElementById('chart');
  if (!chartEl) return;
  if (!state.rows.length) {
    Plotly.purge(chartEl);
    setStatus('暂无数据');
    return;
  }
  const C = themeColors();
  Plotly.react(chartEl, buildTraces(C), buildLayout(C), { responsive: true, displaylogo: false });
  const tfLabel = TF_LABELS[state.tf] || state.tf;
  const last = state.rows[state.rows.length - 1];
  const lastClose = last && last.Close != null ? last.Close.toFixed(2) : '—';
  setStatus(`${tfLabel} · ${state.rows.length} 个 K 棒 · 区间 ${state.range} · ${state.mode === 'line' ? '折线' : 'K 线'} · 最新收盘 ${lastClose}`);
}

export function resize() {
  const chartEl = document.getElementById('chart');
  if (state.rows.length && chartEl) Plotly.Plots.resize(chartEl);
}
