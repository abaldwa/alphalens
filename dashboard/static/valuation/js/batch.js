// dashboard/static/valuation/js/batch.js — Batch Valuation
// GET /api/v1/valuation/batch/ranked?max_tier=&limit=&n_workers=
renderAppShell("valuation", "batch");

// Same generic client-side column sort as dashboard/static/ops/js/index.js.
function sortRows(rows, key, dir) {
  const factor = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const va = a[key];
    const vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (va < vb) return -1 * factor;
    if (va > vb) return 1 * factor;
    return 0;
  });
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

let lastRows = [];
const sortState = { key: "margin_of_safety", dir: "desc" };

function overallValuationBadge(mos) {
  if (mos == null) return el("span", { class: "badge b-gray" }, ["N/A"]);
  if (mos > 0.15) return el("span", { class: "badge b-green" }, ["Undervalued"]);
  if (mos < -0.15) return el("span", { class: "badge b-red" }, ["Overvalued"]);
  return el("span", { class: "badge b-amber" }, ["Fairly Valued"]);
}

function renderTable() {
  const c = document.getElementById("content");
  if (!lastRows.length) {
    c.innerHTML = `<div class="empty">No results</div>`;
    return;
  }
  const sorted = sortRows(lastRows, sortState.key, sortState.dir);
  const onSort = (key, dir) => {
    sortState.key = key;
    sortState.dir = dir;
    renderTable();
  };
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Stock", "ticker", sortState, onSort),
      sortableHeader("Overall Valuation", "margin_of_safety", sortState, onSort),
      sortableHeader("CMP", "current_price", sortState, onSort),
      sortableHeader("Price/Share (Valuation)", "intrinsic_value", sortState, onSort),
      sortableHeader("% Difference", "valuation_gap_pct", sortState, onSort),
      sortableHeader("Lifecycle Stage", "lifecycle_stage", sortState, onSort),
      sortableHeader("Model", "dcf_model_type", sortState, onSort),
      sortableHeader("Data Quality", "data_quality", sortState, onSort),
    ])]),
    el("tbody", {}, sorted.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [el("a", { href: `dcf.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("td", {}, [overallValuationBadge(r.margin_of_safety)]),
      el("td", { class: "mono" }, [fmtMoney(r.current_price)]),
      el("td", { class: "mono" }, [r.intrinsic_value != null ? fmtMoney(r.intrinsic_value) : "—"]),
      el("td", { class: "mono " + pnlClass(r.valuation_gap_pct != null ? -r.valuation_gap_pct : null) }, [
        r.valuation_gap_pct != null ? `${(r.valuation_gap_pct * 100).toFixed(1)}%` : "—",
      ]),
      el("td", { style: "font-size:12px;color:var(--tx2)" }, [r.lifecycle_stage || "—"]),
      el("td", { style: "font-size:12px;color:var(--tx2)" }, [r.dcf_model_type || "—"]),
      el("td", {}, [el("span", { class: "badge " + (r.data_quality === "full" ? "b-green" : r.data_quality === "partial" ? "b-amber" : "b-gray") }, [r.data_quality || "—"])]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function runBatch() {
  const scope = document.getElementById("scope-select").value;
  const params = { limit: 500, n_workers: 16 };
  if (scope !== "all") params.max_tier = scope;

  const statusEl = document.getElementById("run-status");
  const btn = document.getElementById("run-btn");
  btn.disabled = true;
  statusEl.textContent = "Running DCF valuation — this can take a while for larger scopes…";
  document.getElementById("content").innerHTML = `<div class="loading">Running batch valuation…</div>`;

  apiGet("/api/v1/valuation/batch/ranked", params)
    .then((r) => {
      lastRows = r.results || [];
      statusEl.textContent = `${fmtInt(r.count)} stocks valued as of ${r.as_of_date || "latest available data"}`;
      renderTable();
    })
    .catch((e) => {
      statusEl.textContent = "";
      showError("content", e);
    })
    .finally(() => {
      btn.disabled = false;
    });
}

document.getElementById("run-btn").addEventListener("click", runBatch);
