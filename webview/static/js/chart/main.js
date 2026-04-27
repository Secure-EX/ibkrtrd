// Entry point: read window.__TICKER__, wire toolbar events, kick off first fetch.
import { fetchData } from './api.js';
import { render, resize } from './plot.js';
import { state } from './state.js';

function exclusiveActivate(group, btn) {
  group.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
  btn.classList.add('active');
}

function wireButtonGroup(selector, stateKey, dataAttr, opts = {}) {
  const group = document.querySelector(selector);
  if (!group) return;
  group.querySelectorAll('button').forEach((btn) => {
    btn.addEventListener('click', () => {
      exclusiveActivate(group, btn);
      state[stateKey] = btn.dataset[dataAttr];
      (opts.onChange || fetchData)();
    });
  });
}

function wireCheckbox(id, stateKey) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('change', (e) => {
    state[stateKey] = e.target.checked;
    render();
  });
}

function init() {
  state.ticker = window.__TICKER__;
  if (!state.ticker) return;

  // Server-side change (refetch)
  wireButtonGroup('#tf-buttons', 'tf', 'tf');
  wireButtonGroup('#range-buttons', 'range', 'range');
  // Client-only change (just re-render existing rows)
  wireButtonGroup('#mode-buttons', 'mode', 'mode', { onChange: render });

  wireCheckbox('bb-toggle', 'showBB');
  wireCheckbox('ma-toggle', 'showMA');

  window.addEventListener('resize', resize);

  fetchData();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
