// Network layer: fetch OHLCV + signals in parallel, mutate state, trigger re-render.
import { setStatus } from './dom.js';
import { render } from './plot.js';
import { state } from './state.js';
import { renderTrend } from './trend.js';

export async function fetchData() {
  if (!state.ticker) return;
  setStatus('加载中…');
  try {
    const qs = `range=${encodeURIComponent(state.range)}&tf=${encodeURIComponent(state.tf)}`;
    const [ohlcvRes, signalsRes] = await Promise.all([
      fetch(`/api/ohlcv/${encodeURIComponent(state.ticker)}?${qs}`),
      fetch(`/api/signals/${encodeURIComponent(state.ticker)}?tf=${encodeURIComponent(state.tf)}`),
    ]);
    if (!ohlcvRes.ok) throw new Error('OHLCV HTTP ' + ohlcvRes.status);
    const data = await ohlcvRes.json();
    state.rows = data.rows || [];
    if (Array.isArray(data.ma_periods) && data.ma_periods.length) {
      state.maPeriods = data.ma_periods;
    }
    render();
    if (signalsRes.ok) {
      const trend = await signalsRes.json();
      renderTrend(trend);
    }
  } catch (e) {
    setStatus('加载失败：' + e.message);
  }
}
