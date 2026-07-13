// dashboard/static/js/api.js
// Shared fetch wrapper for all screens. Same origin serves both /ui/* static
// files and /api/v1/* routes (datastore/api/main.py), so window.location.origin
// is always the right base — mirrors dashboard/screens/daily_dashboard.py's
// _fetch() try/except-and-report pattern, translated to JS.

const API_BASE = window.location.origin;

async function apiGet(path, params) {
  let url = API_BASE + path;
  if (params) {
    const qs = new URLSearchParams(params).toString();
    if (qs) url += "?" + qs;
  }
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`${resp.status} ${resp.statusText} — ${url}`);
  }
  return resp.json();
}

async function apiPost(path, body) {
  const resp = await fetch(API_BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    throw new Error(`${resp.status} ${resp.statusText} — ${path}`);
  }
  return resp.json();
}

async function apiDelete(path) {
  const resp = await fetch(API_BASE + path, { method: "DELETE" });
  if (!resp.ok) {
    throw new Error(`${resp.status} ${resp.statusText} — ${path}`);
  }
  return resp.json();
}

function fmtPct(x, digits = 1) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return (x * 100).toFixed(digits) + "%";
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return Number(x).toFixed(digits);
}

// #22 — Indian-grouped integer display for raw counts (Volume, row counts,
// quantities, etc.) that shouldn't get fmtMoney()'s "₹" prefix.
function fmtInt(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return Number(x).toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function fmtMoney(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return "₹" + Number(x).toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function pnlClass(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "";
  return x >= 0 ? "pos" : "neg";
}

// mb_tier ("10x"/"5x"/"3x"/"2x"/"none") is a deterministic bucketing of
// mb_probability (systems/ml_signal_engine/models/multibagger/multibagger_model.py:96-101),
// not a separately-modeled multiplier prediction — "10x" does NOT mean
// "predicted to return 10x", only "mb_probability >= 0.80". Renders the
// actual probability band instead of the misleading multiplier label.
const MB_TIER_BANDS = {
  "10x": "≥ 80%",
  "5x": "60–79%",
  "3x": "45–59%",
  "2x": "30–44%",
  none: "< 30%",
};

function mbTierLabel(tier) {
  if (!tier) return "—";
  const band = MB_TIER_BANDS[tier];
  return band ? `${band} (${tier} band)` : tier;
}

function badgeClass(sev) {
  const map = { high: "b-red", medium: "b-amber", low: "b-blue", red: "b-red", amber: "b-amber", green: "b-green", teal: "b-teal" };
  return map[sev] || "b-gray";
}

function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "html") node.innerHTML = v;
      else node.setAttribute(k, v);
    }
  }
  (children || []).forEach((c) => {
    if (c === null || c === undefined) return;
    node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
  });
  return node;
}

function showError(containerId, err) {
  const c = document.getElementById(containerId);
  if (c) c.innerHTML = `<div class="error">Failed to load: ${err.message || err}</div>`;
}

function showLoading(containerId) {
  const c = document.getElementById(containerId);
  if (c) c.innerHTML = `<div class="loading">Loading…</div>`;
}

// Generic client-side column sort: click a sortable header to sort by it,
// click again to reverse. Rows are assumed small enough (<=few hundred) that
// no server-side sort param is needed — just re-render from the same data.
// (Originally ops/js/index.js-only; promoted here in #21 so Signal Deep
// Dive's full-universe table and #23's Exit Urgency page can reuse it.)
function sortRows(rows, key, dir) {
  const factor = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const va = a[key];
    const vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return 1; // nulls last regardless of direction
    if (vb == null) return -1;
    if (va < vb) return -1 * factor;
    if (va > vb) return 1 * factor;
    return 0;
  });
}

// A69 — uniform ticker-hyperlink convention: every ticker cell links to
// technical/chart.html?ticker=... in a new tab, plus a small "Signal Deep
// Dive" icon that opens ml/signal.html?ticker=... in a new tab. Absolute
// /ui/... paths so this works identically from any app's own directory
// (ml/, technical/, forensic/, big_investors/, valuation/, fundamental/).
function tickerCell(ticker, extraTdAttrs) {
  const attrs = Object.assign({ style: "font-weight:600;white-space:nowrap" }, extraTdAttrs || {});
  return el("td", attrs, [
    el("a", { href: `/ui/technical/chart.html?ticker=${ticker}`, target: "_blank", rel: "noopener" }, [ticker]),
    el("a", {
      href: `/ui/ml/signal.html?ticker=${ticker}`,
      target: "_blank",
      rel: "noopener",
      title: "Signal Deep Dive",
      style: "margin-left:6px;text-decoration:none;font-size:12px",
    }, ["🔎"]),
  ]);
}

// A67/ML28 — minimal dependency-free inline sparkline: an SVG polyline
// over a series of numbers (e.g. rebased-return points from
// features/sector_rotation.py's _sparkline_series, or raw OHLCV closes
// fetched directly from the API for a first pass per A67's note). Renders
// nothing (returns "—") for missing/too-short series — no fabricated
// placeholder shape.
function sparklineSvg(series, opts) {
  const o = Object.assign({ width: 80, height: 24, stroke: "#2563eb" }, opts || {});
  if (!Array.isArray(series) || series.length < 2) return "—";
  const min = Math.min(...series);
  const max = Math.max(...series);
  const range = max - min || 1;
  const stepX = o.width / (series.length - 1);
  const points = series
    .map((v, i) => `${(i * stepX).toFixed(2)},${(o.height - ((v - min) / range) * o.height).toFixed(2)}`)
    .join(" ");
  const lastUp = series[series.length - 1] >= series[0];
  const color = o.strokeAuto ? (lastUp ? "#16a34a" : "#dc2626") : o.stroke;
  return `<svg width="${o.width}" height="${o.height}" viewBox="0 0 ${o.width} ${o.height}" preserveAspectRatio="none">` +
    `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.5"/></svg>`;
}

function sortableHeader(label, key, sortState, onSort) {
  const isActive = sortState.key === key;
  const arrow = isActive ? (sortState.dir === "asc" ? " ▲" : " ▼") : "";
  const th = el("th", { style: "cursor:pointer;user-select:none" }, [label + arrow]);
  th.addEventListener("click", () => {
    const nextDir = isActive && sortState.dir === "asc" ? "desc" : "asc";
    onSort(key, nextDir);
  });
  return th;
}
