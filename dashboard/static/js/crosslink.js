// dashboard/static/js/crosslink.js — "View in X" deep-links by ticker,
// built from the same APPS config shell.js uses. Always navigates, even to
// apps with no backend yet — the target screen renders its own honest
// empty-state rather than the link being hidden or disabled.

// The single most ticker-relevant screen per app, used as the cross-link target.
const CROSSLINK_SCREEN = {
  ml: "index.html",
  technical: "chart.html",
  fundamental: "dashboard.html",
  valuation: "dcf.html",
  forensic: "dashboard.html",
};

// Only the 5 ticker-relevant apps — AlphaLens.Ops has no per-ticker meaning
// and never appears in a "View in X" list.
const CROSSLINK_APP_IDS = ["ml", "technical", "fundamental", "valuation", "forensic"];

function buildCrossLinks(ticker, excludeAppId) {
  const wrap = el("div", { class: "cross-links" }, []);
  APPS.filter((a) => CROSSLINK_APP_IDS.includes(a.id) && a.id !== excludeAppId).forEach((a) => {
    const href = `${a.base}${CROSSLINK_SCREEN[a.id]}?ticker=${encodeURIComponent(ticker)}`;
    wrap.appendChild(el("a", { class: "cross-link", href }, [a.name]));
  });
  return wrap;
}
