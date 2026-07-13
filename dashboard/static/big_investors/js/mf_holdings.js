// dashboard/static/big_investors/js/mf_holdings.js — MF Holdings movers (Phase C)
renderAppShell("big_investors", "mf_holdings");

const MF_CAP_BAND_BADGE = { large: "b-gray", mid: "b-blue", small: "b-amber", micro: "b-red", unknown: "b-gray" };
const MF_DIRECTION_BADGE = {
  new_entry: "b-green", full_exit: "b-red", increasing: "b-green", decreasing: "b-red", unchanged: "b-gray",
};
const MF_DIRECTION_LABEL = {
  new_entry: "NEW ENTRY", full_exit: "FULL EXIT", increasing: "Increasing", decreasing: "Decreasing", unchanged: "Unchanged",
};

function renderMfFilters() {
  const c = document.getElementById("mf-filters");
  const capSelect = el("select", { id: "mf-cap-select" }, [
    el("option", { value: "" }, ["All cap bands"]),
    el("option", { value: "large" }, ["Large"]),
    el("option", { value: "mid" }, ["Mid"]),
    el("option", { value: "small" }, ["Small"]),
    el("option", { value: "micro" }, ["Micro"]),
  ]);
  const dirSelect = el("select", { id: "mf-direction-select" }, [
    el("option", { value: "" }, ["All movers"]),
    el("option", { value: "new_entry" }, ["New entries"]),
    el("option", { value: "full_exit" }, ["Full exits"]),
    el("option", { value: "increasing" }, ["Increasing"]),
    el("option", { value: "decreasing" }, ["Decreasing"]),
  ]);
  const goBtn = el("button", {}, ["Load"]);
  goBtn.addEventListener("click", loadMfMovers);
  c.innerHTML = "";
  c.appendChild(
    el("div", { class: "card", style: "margin-bottom:12px; display:flex; gap:8px; align-items:center" }, [
      el("label", { for: "mf-cap-select" }, ["Cap band:"]),
      capSelect,
      el("label", { for: "mf-direction-select" }, ["Direction:"]),
      dirSelect,
      goBtn,
    ])
  );
}

const MF_COLUMNS = [
  { key: "ticker", label: "Ticker" },
  { key: "company_name", label: "Company" },
  { key: "cap_band", label: "Cap Band" },
  { key: "market_cap_cr", label: "Market Cap (cr)" },
  { key: "prev_qty", label: "Prev Qty" },
  { key: "curr_qty", label: "Curr Qty" },
  { key: "qty_change_pct", label: "Change %" },
  { key: "curr_scheme_count", label: "Schemes" },
  { key: "scheme_count_change", label: "Scheme Δ" },
  { key: "direction", label: "Status" },
];

let mfSortKey = "market_cap_cr";
let mfSortDir = 1;
let mfLastData = [];

function sortMfData(data) {
  const sorted = [...data];
  sorted.sort((a, b) => {
    const av = a[mfSortKey], bv = b[mfSortKey];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === "string") return mfSortDir * av.localeCompare(bv);
    return mfSortDir * (av - bv);
  });
  return sorted;
}

function closeMfModal() {
  const existing = document.getElementById("mf-scheme-modal");
  if (existing) existing.remove();
}

function showMfSchemeModal(row) {
  closeMfModal();
  const overlay = el("div", {
    id: "mf-scheme-modal",
    style: "position:fixed; inset:0; background:rgba(0,0,0,0.5); display:flex; align-items:center; justify-content:center; z-index:1000",
  }, []);
  overlay.addEventListener("click", (e) => { if (e.target === overlay) closeMfModal(); });

  const body = el("div", { id: "mf-scheme-modal-body" }, [el("div", { class: "loading" }, ["Loading…"])]);
  const panel = el("div", {
    class: "card",
    style: "max-width:520px; max-height:70vh; overflow-y:auto; background:var(--bg1, #1a1a1a)",
  }, [
    el("div", { style: "display:flex; justify-content:space-between; align-items:center; margin-bottom:8px" }, [
      el("span", { class: "sec-title" }, [`${row.ticker} — Schemes holding (${row.curr_month || "—"})`]),
      (() => {
        const btn = el("button", {}, ["✕"]);
        btn.addEventListener("click", closeMfModal);
        return btn;
      })(),
    ]),
    body,
  ]);
  overlay.appendChild(panel);
  document.body.appendChild(overlay);

  apiGet(`/api/v1/big-investors/mf-holdings/${row.ticker}`, { start_month: row.curr_month?.slice(0, 7), end_month: row.curr_month?.slice(0, 7) })
    .then((r) => {
      if (!r.data.length) {
        body.innerHTML = `<div class="empty">No scheme-level detail found for this month</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Scheme"]), el("th", {}, ["Quantity"])])]),
        el("tbody", {}, r.data.map((s) => el("tr", {}, [
          el("td", {}, [s.scheme_name]),
          el("td", { class: "mono" }, [s.quantity.toLocaleString("en-IN")]),
        ]))),
      ]);
      body.innerHTML = "";
      body.appendChild(table);
    })
    .catch((e) => { body.innerHTML = `<div class="empty">Failed to load: ${e}</div>`; });
}

function renderMfTable(r) {
  const c = document.getElementById("mf-table");
  if (!r.data.length) {
    c.innerHTML = `<div class="empty">No MF holdings data available yet (twice-monthly ingestion — see Ops for last run status)</div>`;
    return;
  }
  mfLastData = r.data;
  const periodNote = el("div", { style: "font-size:11px; color:var(--tx3); margin-bottom:8px" }, [
    `Comparing ${r.data[0].prev_month || "—"} → ${r.data[0].curr_month || "—"} (as of ${r.as_of})`,
  ]);
  const sorted = sortMfData(mfLastData);
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, MF_COLUMNS.map((col) => {
      const arrow = mfSortKey === col.key ? (mfSortDir === 1 ? " ▲" : " ▼") : "";
      const th = el("th", { style: "cursor:pointer; user-select:none" }, [col.label + arrow]);
      th.addEventListener("click", () => {
        if (mfSortKey === col.key) {
          mfSortDir *= -1;
        } else {
          mfSortKey = col.key;
          mfSortDir = 1;
        }
        renderMfTable(r);
      });
      return th;
    }))]),
    el("tbody", {}, sorted.map((row) => {
      const schemeChangeLabel = row.scheme_count_change > 0 ? `+${row.scheme_count_change}` : String(row.scheme_count_change);
      const schemeCell = el("td", { class: "mono", style: "cursor:pointer; text-decoration:underline dotted" }, [String(row.curr_scheme_count ?? "—")]);
      schemeCell.title = "Double-click for the list of mutual funds holding this stock";
      schemeCell.addEventListener("dblclick", () => showMfSchemeModal(row));
      return el("tr", {}, [
        tickerCell(row.ticker),
        el("td", {}, [row.company_name || "—"]),
        el("td", {}, [el("span", { class: "badge " + (MF_CAP_BAND_BADGE[row.cap_band] || "b-gray") }, [row.cap_band])]),
        el("td", { class: "mono" }, [row.market_cap_cr != null ? row.market_cap_cr.toLocaleString("en-IN") : "—"]),
        el("td", { class: "mono" }, [row.prev_qty.toLocaleString("en-IN")]),
        el("td", { class: "mono" }, [row.curr_qty.toLocaleString("en-IN")]),
        el("td", { class: "mono" }, [row.qty_change_pct != null ? `${row.qty_change_pct.toFixed(1)}%` : "—"]),
        schemeCell,
        el("td", { class: "mono" }, [schemeChangeLabel]),
        el("td", {}, [el("span", { class: "badge " + (MF_DIRECTION_BADGE[row.direction] || "b-gray") }, [MF_DIRECTION_LABEL[row.direction] || row.direction])]),
      ]);
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [periodNote, table]));
}

function loadMfMovers() {
  const capBand = document.getElementById("mf-cap-select").value;
  const direction = document.getElementById("mf-direction-select").value;
  const c = document.getElementById("mf-table");
  c.innerHTML = `<div class="loading">Loading…</div>`;
  const params = {};
  if (capBand) params.cap_band = capBand;
  if (direction) params.direction = direction;
  apiGet("/api/v1/big-investors/mf-holdings/movers", params)
    .then((r) => renderMfTable(r))
    .catch((e) => showError("mf-table", e));
}

renderMfFilters();
loadMfMovers();
