// Tiny DOM utility helpers used across modules.

export function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Extract one column from an array of row objects, normalizing undefined → null.
export function col(rows, key) {
  return rows.map((r) => (r[key] === null || r[key] === undefined ? null : r[key]));
}

export function setStatus(msg) {
  const el = document.getElementById('chart-status');
  if (el) el.textContent = msg || '';
}
