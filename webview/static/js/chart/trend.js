// Renders the trend-analysis badge panel from the /api/signals payload.
import { TF_LABELS } from './constants.js';
import { escapeHtml } from './dom.js';
import { state } from './state.js';

export function renderTrend(trend) {
  const bodyEl = document.getElementById('trend-body');
  const labelEl = document.getElementById('trend-tf-label');
  if (!bodyEl) return;
  const tfLabel = TF_LABELS[state.tf] || state.tf;
  if (labelEl) labelEl.textContent = tfLabel;

  if (!trend || !trend.available) {
    bodyEl.innerHTML = `<p class="empty">${escapeHtml(tfLabel)}级别暂无分析数据。</p>`;
    return;
  }

  const parts = trend.groups.map((grp) => {
    const badges = grp.badges.map((b) => `
      <span class="badge ${escapeHtml(b.tone)}">
        <span class="badge-label">${escapeHtml(b.label)}</span>
        <span class="badge-value">${escapeHtml(b.value)}</span>
      </span>`).join('');
    return `<div class="trend-group"><h3>${escapeHtml(grp.title)}</h3><div class="badges">${badges}</div></div>`;
  });

  if (trend.generation_date) {
    parts.push(`<p class="muted small">分析数据生成日期：${escapeHtml(trend.generation_date)}</p>`);
  }
  bodyEl.innerHTML = parts.join('');
}
