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

function fmtNum(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return '—';
  return Number(v).toLocaleString(undefined, {
    minimumFractionDigits: digits, maximumFractionDigits: digits,
  });
}

function fmtInt(v) {
  if (v == null || Number.isNaN(v)) return '—';
  return Math.round(Number(v)).toLocaleString();
}

function tradeHover(t, includePnl) {
  const lines = [
    `<b>${t.date} ${t.action}</b>`,
    `股数 ${fmtInt(t.qty)}`,
    `价格 ${fmtNum(t.price, 2)}`,
    `金额 ${fmtNum(t.amount, 0)}`,
  ];
  if (includePnl && t.realized_pnl != null) {
    const sign = t.realized_pnl >= 0 ? '+' : '';
    lines.push(`盈亏 ${sign}${fmtNum(t.realized_pnl, 2)}`);
  }
  return lines.join('<br>') + '<extra></extra>';
}

function pushTradeTrace(traces, items, label, color, hoverFontColor, includePnl) {
  if (!items.length) return;
  traces.push({
    x: items.map((t) => t.date),
    y: items.map((t) => t.price),
    type: 'scatter',
    mode: 'markers+text',
    name: label,
    text: items.map(() => label.charAt(0)),
    textposition: 'middle center',
    textfont: { color: '#ffffff', size: 10, family: 'IBM Plex Mono, monospace' },
    marker: {
      size: 16,
      color,
      symbol: 'circle',
      line: { color: '#ffffff', width: 1.2 },
      opacity: 0.95,
    },
    hovertemplate: items.map((t) => tradeHover(t, includePnl)),
    hoverlabel: hoverFontColor
      ? { font: { color: hoverFontColor, size: 11, family: 'IBM Plex Mono, monospace' } }
      : { font: { size: 11, family: 'IBM Plex Mono, monospace' } },
    showlegend: false,
    cliponaxis: false,
  });
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

  // --- KDJ(9,3,3) ---
  traces.push({
    x: dates, y: col(rows, 'k'), type: 'scatter', mode: 'lines',
    name: 'K', line: { color: '#f1c47b', width: 1 }, yaxis: 'y5',
  });
  traces.push({
    x: dates, y: col(rows, 'd'), type: 'scatter', mode: 'lines',
    name: 'D', line: { color: '#8be9fd', width: 1 }, yaxis: 'y5',
  });
  traces.push({
    x: dates, y: col(rows, 'j'), type: 'scatter', mode: 'lines',
    name: 'J', line: { color: '#ff79c6', width: 1 }, yaxis: 'y5',
  });

  // --- Buy/Sell markers (price subplot, both line & candle modes) ---
  const trades = state.trades || [];
  if (trades.length) {
    const buys = trades.filter((t) => t.action === 'BUY');
    const sellsWin = trades.filter((t) => t.action === 'SELL' && (t.realized_pnl ?? 0) >= 0);
    const sellsLoss = trades.filter((t) => t.action === 'SELL' && (t.realized_pnl ?? 0) < 0);
    pushTradeTrace(traces, buys, 'BUY', C.info, null, false);
    pushTradeTrace(traces, sellsWin, 'SELL', C.up, C.up, true);
    pushTradeTrace(traces, sellsLoss, 'SELL', C.down, C.down, true);
  }

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
      anchor: 'y5', type: 'date',
      rangeslider: { visible: false },
      showgrid: true, gridcolor: C.grid, linecolor: C.line, zerolinecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    yaxis: {
      domain: [0.58, 1.00], title: { text: '价格', font: { color: C.fg2, size: 10 } },
      showgrid: true, gridcolor: C.grid, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    yaxis2: {
      domain: [0.45, 0.56], title: { text: '量', font: { color: C.fg2, size: 10 } },
      showgrid: false, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    yaxis3: {
      domain: [0.30, 0.43], title: { text: 'MACD', font: { color: C.fg2, size: 10 } },
      zeroline: true, zerolinecolor: C.line, gridcolor: C.grid, linecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    yaxis4: {
      domain: [0.15, 0.28], title: { text: 'RSI', font: { color: C.fg2, size: 10 } },
      range: [0, 100], showgrid: false, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    yaxis5: {
      domain: [0.00, 0.13], title: { text: 'KDJ', font: { color: C.fg2, size: 10 } },
      showgrid: false, linecolor: C.line, tickfont: { color: C.fg2, size: 10 },
    },
    shapes: [
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 70, y1: 70, line: { color: C.down, width: 1, dash: 'dot' } },
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y4', y0: 30, y1: 30, line: { color: C.up, width: 1, dash: 'dot' } },
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y5', y0: 80, y1: 80, line: { color: C.down, width: 1, dash: 'dot' } },
      { type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y5', y0: 20, y1: 20, line: { color: C.up, width: 1, dash: 'dot' } },
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
  const peEl = document.getElementById('pe-chart');
  if (state.showPE && state.peRows.length && peEl) Plotly.Plots.resize(peEl);
}

function pctTone(C, pct) {
  if (pct == null) return C.fg1;
  if (pct <= 30) return C.up;
  if (pct >= 70) return C.down;
  return C.fg1;
}

function renderPESummary(C) {
  const el = document.getElementById('pe-summary');
  if (!el) return;
  const s = state.peSummary || {};
  const cur = state.peCurrent || {};
  if (!state.peAvailable || s.current == null) {
    el.innerHTML = '<span class="muted">暂无 PE 数据（缺少季度财报或 EPS ≤ 0）</span>';
    return;
  }
  const tone = pctTone(C, s.percentile);
  const pct = s.percentile != null ? `${s.percentile.toFixed(0)}%` : '—';
  const yfPe = cur.yf_pe != null ? cur.yf_pe.toFixed(1) + 'x' : '—';
  const yfEps = cur.yf_eps_ttm != null ? cur.yf_eps_ttm.toFixed(2) : '—';
  const ourEps = cur.our_eps_ttm != null ? cur.our_eps_ttm.toFixed(2) : '—';
  el.innerHTML = [
    `<span>自算 PE <b style="color:${tone}">${s.current.toFixed(1)}x</b> <span class="muted">(EPS ${ourEps})</span></span>`,
    `<span>yfinance PE <b>${yfPe}</b> <span class="muted">(EPS ${yfEps})</span></span>`,
    `<span>历史分位 <b style="color:${tone}">${pct}</b></span>`,
    `<span>P25 <b>${s.p25.toFixed(1)}</b></span>`,
    `<span>P50 <b>${s.p50.toFixed(1)}</b></span>`,
    `<span>P75 <b>${s.p75.toFixed(1)}</b></span>`,
    `<span class="muted">区间 ${state.range} · 样本 ${s.count}</span>`,
  ].join('<span class="sep">|</span>');
}

function renderPETables(C) {
  const el = document.getElementById('pe-tables');
  if (!el) return;
  if (!state.peAvailable) {
    el.innerHTML = '';
    return;
  }
  const scenarios = state.peScenarios || [];
  const targets = state.peTargets || [];
  const fmt = (v, d = 2) => (v == null || !isFinite(v)) ? '—' : Number(v).toFixed(d);
  const fmtSign = (v) => v == null ? '' : (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  const cls = (v, tag = 'pct') => v == null ? '' : (v < 0 ? 'down' : v > 0 ? 'up' : '');

  const scenRows = scenarios.map((s) => {
    const isCur = s.pct === 0;
    const ourPctTone = pctTone(C, s.our_percentile);
    return `
      <tr class="${isCur ? 'current' : ''}">
        <td>${s.pct === 0 ? '当前' : (s.pct > 0 ? '+' : '') + s.pct + '%'}</td>
        <td>${fmt(s.price, 2)}</td>
        <td>${fmt(s.our_pe, 2)}<span class="delta muted">${s.our_percentile != null ? '(' + s.our_percentile.toFixed(0) + '%)' : ''}</span></td>
        <td class="muted">${fmt(s.yf_pe, 2)}</td>
      </tr>`;
  }).join('');

  const tgtRows = targets.map((t) => `
      <tr>
        <td>${t.label}</td>
        <td>${fmt(t.target_pe, 2)}</td>
        <td>${fmt(t.required_price, 2)}</td>
        <td class="${cls(t.pct_change)}">${t.pct_change == null ? '—' : fmtSign(t.pct_change)}</td>
      </tr>`).join('');

  el.innerHTML = `
    <div>
      <h4>价格情景 → PE</h4>
      <table>
        <thead><tr><th>涨跌</th><th>股价</th><th>自算 PE (分位)</th><th>yf PE</th></tr></thead>
        <tbody>${scenRows}</tbody>
      </table>
    </div>
    <div>
      <h4>历史分位 → 目标价</h4>
      <table>
        <thead><tr><th>分位</th><th>目标 PE</th><th>对应股价</th><th>距今</th></tr></thead>
        <tbody>${tgtRows}</tbody>
      </table>
    </div>`;
}

export function renderPE() {
  const peEl = document.getElementById('pe-chart');
  if (!peEl) return;
  const C = themeColors();
  renderPESummary(C);
  renderPETables(C);

  if (!state.showPE) {
    Plotly.purge(peEl);
    return;
  }
  if (!state.peAvailable || !state.peRows.length) {
    Plotly.purge(peEl);
    return;
  }

  const dates = state.peRows.map((r) => r.date);
  const pes = state.peRows.map((r) => r.pe);
  const s = state.peSummary || {};

  const traces = [{
    x: dates, y: pes, type: 'scatter', mode: 'lines',
    name: 'PE(TTM)', line: { color: C.accent, width: 1.4 },
    hovertemplate: '%{x|%Y-%m-%d}<br>PE %{y:.2f}<extra></extra>',
    fill: 'tozeroy', fillcolor: 'rgba(150,150,150,0.04)',
  }];

  const shapes = [];
  const annotations = [];
  ['p25', 'p50', 'p75'].forEach((k, i) => {
    if (s[k] == null) return;
    const colors = [C.up, C.fg2, C.down];
    shapes.push({
      type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y',
      y0: s[k], y1: s[k],
      line: { color: colors[i], width: 1, dash: 'dot' },
    });
    annotations.push({
      xref: 'paper', x: 1, xanchor: 'right',
      yref: 'y', y: s[k], yanchor: 'bottom',
      text: `${k.toUpperCase()} ${s[k].toFixed(1)}`,
      showarrow: false,
      font: { size: 9, color: colors[i] },
      bgcolor: 'rgba(0,0,0,0)',
    });
  });
  if (s.current != null && dates.length) {
    const tone = pctTone(C, s.percentile);
    annotations.push({
      x: dates[dates.length - 1], xanchor: 'right',
      y: s.current, yanchor: 'bottom',
      text: `当前 ${s.current.toFixed(1)}（${s.percentile != null ? s.percentile.toFixed(0) : '—'}%分位）`,
      showarrow: true, arrowhead: 0, ax: -30, ay: -18,
      font: { size: 10, color: tone },
      bgcolor: C.bg1, bordercolor: tone, borderwidth: 1, borderpad: 3,
    });
  }

  const layout = {
    autosize: true,
    margin: { l: 48, r: 24, t: 12, b: 28 },
    hovermode: 'x unified',
    paper_bgcolor: C.bg0,
    plot_bgcolor: C.bg0,
    font: { color: C.fg1, size: 11, family: 'IBM Plex Mono, monospace' },
    showlegend: false,
    xaxis: {
      type: 'date', rangeslider: { visible: false },
      showgrid: true, gridcolor: C.grid, linecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    yaxis: {
      title: { text: 'PE(TTM)', font: { color: C.fg2, size: 10 } },
      showgrid: true, gridcolor: C.grid, linecolor: C.line,
      tickfont: { color: C.fg2, size: 10 },
    },
    shapes,
    annotations,
  };

  Plotly.react(peEl, traces, layout, { responsive: true, displaylogo: false });
}
