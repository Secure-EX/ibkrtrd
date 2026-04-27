// Single source of truth for chart UI state. Mutated in-place by api.js / main.js;
// read by plot.js / trend.js. Importers receive the same object reference.
export const state = {
  ticker: null,
  tf: 'daily',
  range: '1Y',
  mode: 'line',     // 'line' | 'candle'
  showBB: true,
  showMA: true,
  rows: [],
  maPeriods: [20, 60, 250],
};
