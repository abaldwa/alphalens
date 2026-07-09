// dashboard/static/big_investors/js/index.js — Bulk/Block Deals (Phase A/B/D)
renderAppShell("big_investors", "index");

const CAP_BAND_BADGE = { large: "b-gray", mid: "b-blue", small: "b-amber", micro: "b-red", unknown: "b-gray" };

function isoToday() {
  return new Date().toISOString().slice(0, 10);
}

function resolveReconciliation(id, row) {
  const reviewedBy = prompt("Reviewer name:");
  if (!reviewedBy) return;
  row.style.opacity = "0.5";
  apiPost(`/api/v1/big-investors/reconciliation/${id}/resolve`, { reviewed_by: reviewedBy })
    .then(() => loadReconciliation())
    .catch((e) => {
      row.style.opacity = "1";
      alert(`Resolve failed: ${e.message}`);
    });
}

function renderReconciliationTable(rows) {
  const c = document.getElementById("reconciliation-table");
  const flagged = rows.filter((r) => r.status === "flagged_for_review");
  if (!rows.length) {
    c.innerHTML = "";
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Family"]), el("th", {}, ["Ticker"]), el("th", {}, ["Quarter End"]),
      el("th", {}, ["Est. Position"]), el("th", {}, ["Reported Est."]), el("th", {}, ["Discrepancy"]),
      el("th", {}, ["Status"]), el("th", {}, ["Action"]),
    ])]),
    el("tbody", {}, rows.map((r) => {
      const row = el("tr", {}, [
        el("td", {}, [r.family_id]),
        el("td", { style: "font-weight:600" }, [r.ticker]),
        el("td", {}, [r.quarter_end_date]),
        el("td", { class: "mono" }, [r.estimated_position_pre_correction != null ? r.estimated_position_pre_correction.toLocaleString("en-IN") : "—"]),
        el("td", { class: "mono" }, [r.reported_shares_est != null ? r.reported_shares_est.toLocaleString("en-IN") : "—"]),
        el("td", { class: "mono" }, [r.discrepancy_pct != null ? `${(r.discrepancy_pct * 100).toFixed(1)}%` : "—"]),
        el("td", {}, [el("span", { class: "badge " + (r.status === "flagged_for_review" ? "b-red" : "b-green") }, [r.status])]),
        el("td", {}, []),
      ]);
      if (r.status === "flagged_for_review") {
        const btn = el("button", {}, ["Mark Reviewed"]);
        btn.addEventListener("click", () => resolveReconciliation(r.id, row));
        row.lastChild.appendChild(btn);
      }
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
  if (flagged.length) {
    c.insertBefore(
      el("div", { style: "font-size:11px; color:var(--red); margin-bottom:8px" }, [`${flagged.length} flagged for review`]),
      c.firstChild
    );
  }
}

function loadReconciliation() {
  const c = document.getElementById("reconciliation-table");
  c.innerHTML = `<div class="loading">Loading…</div>`;
  apiGet("/api/v1/big-investors/reconciliation")
    .then((r) => renderReconciliationTable(r.data))
    .catch((e) => showError("reconciliation-table", e));
}

// [Disabled] Date / Cap Band / Deal Type filter bar for the Entries/Exits
// table — the table now always lists every entry across all history (not
// one trade_date at a time), so a per-date filter no longer applies here.
// function renderFamilyFilters() {
//   const c = document.getElementById("family-filters");
//   const dateInput = el("input", { type: "date", id: "family-date-input", value: isoToday() });
//   const capSelect = el("select", { id: "family-cap-select" }, [
//     el("option", { value: "" }, ["All cap bands"]),
//     el("option", { value: "large" }, ["Large"]),
//     el("option", { value: "mid" }, ["Mid"]),
//     el("option", { value: "small" }, ["Small"]),
//     el("option", { value: "micro" }, ["Micro"]),
//   ]);
//   const dealTypeSelect = el("select", { id: "family-type-select" }, [
//     el("option", { value: "" }, ["Bulk + Block"]),
//     el("option", { value: "bulk" }, ["Bulk only"]),
//     el("option", { value: "block" }, ["Block only"]),
//   ]);
//   const goBtn = el("button", {}, ["Load"]);
//   goBtn.addEventListener("click", loadFamilyPositions);
//   c.innerHTML = "";
//   c.appendChild(
//     el("div", { class: "card", style: "margin-bottom:12px; display:flex; gap:8px; align-items:center" }, [
//       el("label", { for: "family-date-input" }, ["Date:"]),
//       dateInput,
//       el("label", { for: "family-cap-select" }, ["Cap band:"]),
//       capSelect,
//       el("label", { for: "family-type-select" }, ["Deal type:"]),
//       dealTypeSelect,
//       goBtn,
//     ])
//   );
// }

const FAMILY_COLUMNS = [
  { key: "ticker", label: "Ticker" },
  { key: "company_name", label: "Company" },
  { key: "cap_band", label: "Cap Band" },
  { key: "market_cap_cr", label: "Market Cap (cr)" },
  { key: "family_display_name", label: "Investor Family" },
  { key: "trade_date", label: "Txn Date" },
  { key: "net_transaction_type", label: "Net Txn" },
  { key: "net_quantity", label: "Net Qty" },
  { key: "avg_price", label: "Entry Price" },
  { key: "wac", label: "WAC" },
  { key: "cmp", label: "CMP" },
  { key: "price_diff_pct", label: "CMP vs Entry" },
  { key: "cumulative_position_est", label: "Position Est." },
  { key: "holding_pct_of_company", label: "% of Company" },
  { key: "entry_status", label: "Status" },
];

let familySortKey = "market_cap_cr";
let familySortDir = 1;
let familyLastRows = [];

function sortFamilyRows(rows) {
  const sorted = [...rows];
  sorted.sort((a, b) => {
    const av = a[familySortKey], bv = b[familySortKey];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === "string") return familySortDir * av.localeCompare(bv);
    return familySortDir * (av - bv);
  });
  return sorted;
}

function renderFamilyTable(rows) {
  const c = document.getElementById("family-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No family-attributed positions for this date/filter combination</div>`;
    return;
  }
  familyLastRows = rows;
  const sorted = sortFamilyRows(familyLastRows);
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, FAMILY_COLUMNS.map((col) => {
      const arrow = familySortKey === col.key ? (familySortDir === 1 ? " ▲" : " ▼") : "";
      const th = el("th", { style: "cursor:pointer; user-select:none" }, [col.label + arrow]);
      th.addEventListener("click", () => {
        if (familySortKey === col.key) {
          familySortDir *= -1;
        } else {
          familySortKey = col.key;
          familySortDir = 1;
        }
        renderFamilyTable(familyLastRows);
      });
      return th;
    }))]),
    el("tbody", {}, sorted.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [r.ticker]),
      el("td", {}, [r.company_name || "—"]),
      el("td", {}, [el("span", { class: "badge " + (CAP_BAND_BADGE[r.cap_band] || "b-gray") }, [r.cap_band])]),
      el("td", { class: "mono" }, [r.market_cap_cr != null ? r.market_cap_cr.toLocaleString("en-IN") : "—"]),
      el("td", {}, [
        r.family_id.startsWith("unmapped:")
          ? el("span", { style: "color:var(--tx3)" }, [r.family_display_name])
          : r.family_display_name,
      ]),
      el("td", {}, [r.trade_date || "—"]),
      el("td", {}, [el("span", { class: "badge " + (r.net_transaction_type === "BUY" ? "b-green" : "b-red") }, [r.net_transaction_type || "—"])]),
      el("td", { class: "mono" }, [r.net_quantity != null ? r.net_quantity.toLocaleString("en-IN") : "—"]),
      el("td", { class: "mono" }, [r.avg_price != null ? r.avg_price.toFixed(2) : "—"]),
      el("td", { class: "mono" }, [r.wac != null ? r.wac.toFixed(2) : "—"]),
      el("td", { class: "mono" }, [r.cmp != null ? r.cmp.toFixed(2) : "—"]),
      el("td", { class: "mono" }, [
        r.price_diff != null
          ? `${r.price_diff >= 0 ? "+" : ""}${r.price_diff.toFixed(2)} (${r.price_diff_pct.toFixed(1)}%)`
          : "—",
      ]),
      el("td", { class: "mono" }, [r.cumulative_position_est != null ? r.cumulative_position_est.toLocaleString("en-IN") : "—"]),
      el("td", { class: "mono" }, [r.holding_pct_of_company != null ? `${r.holding_pct_of_company.toFixed(2)}%` : "—"]),
      el("td", {}, [
        r.entry_status === "new_entry" ? el("span", { class: "badge b-green" }, ["NEW ENTRY"])
        : r.entry_status === "old_entry" ? el("span", { class: "badge b-blue" }, ["OLD ENTRY"])
        : "—",
      ]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadFamilyPositions() {
  const c = document.getElementById("family-table");
  c.innerHTML = `<div class="loading">Loading…</div>`;
  // No date/cap-band/deal-type filter — every entry across all history is
  // loaded, so purchase prices can be compared against CMP across a
  // family's full trading record in a ticker (see renderFamilyFilters).
  apiGet("/api/v1/big-investors/bulk-deals/families/entries-exits", {})
    .then((r) => renderFamilyTable(r.data))
    .catch((e) => showError("family-table", e));
}

function renderFilters() {
  const c = document.getElementById("deal-filters");
  const dateInput = el("input", { type: "date", id: "deal-date-input", value: isoToday() });
  const capSelect = el("select", { id: "deal-cap-select" }, [
    el("option", { value: "" }, ["All cap bands"]),
    el("option", { value: "large" }, ["Large"]),
    el("option", { value: "mid" }, ["Mid"]),
    el("option", { value: "small" }, ["Small"]),
    el("option", { value: "micro" }, ["Micro"]),
  ]);
  const dealTypeSelect = el("select", { id: "deal-type-select" }, [
    el("option", { value: "" }, ["Bulk + Block"]),
    el("option", { value: "bulk" }, ["Bulk only"]),
    el("option", { value: "block" }, ["Block only"]),
  ]);
  const goBtn = el("button", {}, ["Load"]);
  goBtn.addEventListener("click", loadDeals);
  c.innerHTML = "";
  c.appendChild(
    el("div", { class: "card", style: "margin-bottom:12px; display:flex; gap:8px; align-items:center" }, [
      el("label", { for: "deal-date-input" }, ["Date:"]),
      dateInput,
      el("label", { for: "deal-cap-select" }, ["Cap band:"]),
      capSelect,
      el("label", { for: "deal-type-select" }, ["Deal type:"]),
      dealTypeSelect,
      goBtn,
    ])
  );
}

function renderDealsTable(rows) {
  const c = document.getElementById("deals-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No bulk/block deals for this date/filter combination</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]), el("th", {}, ["Company"]), el("th", {}, ["Cap Band"]),
      el("th", {}, ["Market Cap (cr)"]), el("th", {}, ["Exchange"]), el("th", {}, ["Deal"]),
      el("th", {}, ["Client"]), el("th", {}, ["Txn"]), el("th", {}, ["Quantity"]), el("th", {}, ["Price"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [r.ticker]),
      el("td", {}, [r.company_name || "—"]),
      el("td", {}, [el("span", { class: "badge " + (CAP_BAND_BADGE[r.cap_band] || "b-gray") }, [r.cap_band])]),
      el("td", { class: "mono" }, [r.market_cap_cr != null ? r.market_cap_cr.toLocaleString("en-IN") : "—"]),
      el("td", {}, [r.exchange]),
      el("td", {}, [r.deal_type]),
      el("td", {}, [r.client_name || "—"]),
      el("td", {}, [el("span", { class: "badge " + (r.transaction_type === "BUY" ? "b-green" : "b-red") }, [r.transaction_type || "—"])]),
      el("td", { class: "mono" }, [r.quantity != null ? r.quantity.toLocaleString("en-IN") : "—"]),
      el("td", { class: "mono" }, [r.price != null ? r.price.toFixed(2) : "—"]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadDeals() {
  const dateVal = document.getElementById("deal-date-input").value;
  const capBand = document.getElementById("deal-cap-select").value;
  const dealType = document.getElementById("deal-type-select").value;
  const c = document.getElementById("deals-table");
  c.innerHTML = `<div class="loading">Loading…</div>`;
  const params = { date: dateVal };
  if (capBand) params.cap_band = capBand;
  if (dealType) params.deal_type = dealType;
  apiGet("/api/v1/big-investors/bulk-deals/entries-exits", params)
    .then((r) => renderDealsTable(r.data))
    .catch((e) => showError("deals-table", e));
}

loadReconciliation();
loadFamilyPositions();
renderFilters();
loadDeals();
