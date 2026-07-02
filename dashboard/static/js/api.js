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

function fmtPct(x, digits = 1) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return (x * 100).toFixed(digits) + "%";
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return Number(x).toFixed(digits);
}

function fmtMoney(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return "₹" + Number(x).toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function pnlClass(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "";
  return x >= 0 ? "pos" : "neg";
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
