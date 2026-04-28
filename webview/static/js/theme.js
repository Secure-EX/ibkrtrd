// Theme toggle: persists choice in localStorage, dispatches 'themechange' event
// so theme-aware widgets (e.g. Plotly charts) can re-render with new CSS variables.
(function () {
  const KEY = 'webview-theme';
  const SUN = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
  const MOON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

  function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    const btn = document.getElementById('theme-toggle');
    if (btn) btn.innerHTML = theme === 'dark' ? SUN : MOON;
    try { localStorage.setItem(KEY, theme); } catch (_) {}
    document.dispatchEvent(new CustomEvent('themechange', { detail: { theme } }));
  }

  function init() {
    const initial = (function () {
      try { return localStorage.getItem(KEY); } catch (_) { return null; }
    })() || 'dark';
    setTheme(initial);
    const btn = document.getElementById('theme-toggle');
    if (btn) {
      btn.addEventListener('click', () => {
        const cur = document.documentElement.getAttribute('data-theme');
        setTheme(cur === 'dark' ? 'light' : 'dark');
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
