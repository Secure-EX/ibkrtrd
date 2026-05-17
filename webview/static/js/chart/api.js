// Network layer: fetch OHLCV + signals (and PE on demand) in parallel,
// mutate state, trigger re-render.
import { setStatus } from './dom.js';
import { render, renderPE } from './plot.js';
import { state } from './state.js';
import { renderTrend } from './trend.js';

export async function fetchData() {
  if (!state.ticker) return;
  setStatus('加载中…');
  try {
    const qs = `range=${encodeURIComponent(state.range)}&tf=${encodeURIComponent(state.tf)}`;
    const requests = [
      fetch(`/api/ohlcv/${encodeURIComponent(state.ticker)}?${qs}`),
      fetch(`/api/signals/${encodeURIComponent(state.ticker)}?tf=${encodeURIComponent(state.tf)}`),
    ];
    if (state.showPE) {
      requests.push(fetch(`/api/pe/${encodeURIComponent(state.ticker)}?range=${encodeURIComponent(state.range)}`));
    }
    const results = await Promise.all(requests);
    const ohlcvRes = results[0];
    const signalsRes = results[1];
    const peRes = state.showPE ? results[2] : null;

    if (!ohlcvRes.ok) throw new Error('OHLCV HTTP ' + ohlcvRes.status);
    const data = await ohlcvRes.json();
    state.rows = data.rows || [];
    state.trades = data.trades || [];
    if (Array.isArray(data.ma_periods) && data.ma_periods.length) {
      state.maPeriods = data.ma_periods;
    }
    render();

    if (signalsRes.ok) {
      const trend = await signalsRes.json();
      renderTrend(trend);
    }

    if (peRes) {
      if (peRes.ok) {
        const pe = await peRes.json();
        state.peAvailable = !!pe.available;
        state.peRows = pe.rows || [];
        state.peSummary = pe.summary || {};
        state.peCurrent = pe.current || {};
        state.peScenarios = pe.scenarios || [];
        state.peTargets = pe.targets || [];
      } else {
        state.peAvailable = false;
        state.peRows = [];
        state.peSummary = {};
        state.peCurrent = {};
        state.peScenarios = [];
        state.peTargets = [];
      }
      renderPE();
    }
  } catch (e) {
    setStatus('加载失败：' + e.message);
  }
}

export async function fetchPE() {
  if (!state.ticker) return;
  try {
    const res = await fetch(`/api/pe/${encodeURIComponent(state.ticker)}?range=${encodeURIComponent(state.range)}`);
    if (!res.ok) throw new Error('PE HTTP ' + res.status);
    const pe = await res.json();
    state.peAvailable = !!pe.available;
    state.peRows = pe.rows || [];
    state.peSummary = pe.summary || {};
  } catch {
    state.peAvailable = false;
    state.peRows = [];
    state.peSummary = {};
    state.peCurrent = {};
    state.peScenarios = [];
    state.peTargets = [];
  }
  renderPE();
}
