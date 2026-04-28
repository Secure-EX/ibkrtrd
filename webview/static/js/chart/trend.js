// Renders the trend-analysis badge panel from the /api/signals payload.
import { TF_LABELS } from './constants.js';
import { escapeHtml } from './dom.js';
import { state } from './state.js';

// signals.py emits tone='good|bad|neutral'; map to sample's bull/bear CSS classes.
const TONE_CLASS = { good: 'bull', bad: 'bear', neutral: '' };

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
    const badges = grp.badges.map((b) => {
      const cls = TONE_CLASS[b.tone] || '';
      return `<span class="badge ${cls}"><span class="l">${escapeHtml(b.label)}</span><span class="v">${escapeHtml(b.value)}</span></span>`;
    }).join('');
    return `<div class="trend-group"><div class="gtitle">${escapeHtml(grp.title)}</div><div class="badges">${badges}</div></div>`;
  });

  if (trend.generation_date) {
    parts.push(`<p class="muted small" style="margin-top:8px;">分析数据生成日期：${escapeHtml(trend.generation_date)}</p>`);
  }
  bodyEl.innerHTML = parts.join('');
}
