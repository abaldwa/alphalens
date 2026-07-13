// dashboard/static/ml/js/universe.js — ML25: Full Universe, split out of
// Signal Deep Dive (ml/signal.html) into its own page. Signal Deep Dive
// keeps only the per-ticker detail section; double-clicking a row here
// navigates to signal.html?ticker=... (new tab, A69 convention) instead of
// scrolling to a detail section on this same page.
renderAppShell("ml", "universe");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

const universeSortState = { key: "buy_prob", dir: "desc" };
let universeRows = [];

function forensicBadgeClass(flag) {
  if (flag === "green") return "b-green";
  if (flag === "red" || flag === "black") return "b-red";
  if (flag === "amber") return "b-amber";
  return "b-gray";
}

// ML23 — short descriptive "Basis" text derived from shap_top5_json, so the
// row-level rationale for a buy signal doesn't require opening the detail
// view (shap_top5_json already persisted on ml_signals per ML3/ML8).
function shapBasisText(shapJson) {
  if (!shapJson) return "—";
  try {
    const shap = JSON.parse(shapJson);
    const entries = Array.isArray(shap) ? shap : Object.entries(shap).map(([k, v]) => ({ feature: k, value: v }));
    const top = entries
      .map((e) => ({ feature: e.feature || e[0], value: e.value ?? e[1] ?? 0 }))
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
      .slice(0, 2);
    if (!top.length) return "—";
    return top.map((t) => `${t.feature} (${t.value >= 0 ? "+" : ""}${Number(t.value).toFixed(2)})`).join(", ");
  } catch (e) {
    return "—";
  }
}

function renderUniverseTable() {
  const c = document.getElementById("universe-table");
  if (!universeRows.length) {
    c.innerHTML = `<div class="empty">No scored universe found for the latest date</div>`;
    return;
  }
  const sorted = sortRows(universeRows, universeSortState.key, universeSortState.dir);
  const onSort = (key, dir) => {
    universeSortState.key = key;
    universeSortState.dir = dir;
    renderUniverseTable();
  };
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Ticker", "ticker", universeSortState, onSort),
      sortableHeader("Buy Prob", "buy_prob", universeSortState, onSort),
      sortableHeader("Q50 Return", "q50_return", universeSortState, onSort),
      sortableHeader("Meta Label Prob", "meta_label_prob", universeSortState, onSort),
      sortableHeader("P&D Score", "pnd_score", universeSortState, onSort),
      el("th", {}, ["Forensic"]),
      sortableHeader("MB Probability", "mb_probability", universeSortState, onSort),
      el("th", {}, ["Basis"]),
    ])]),
    el("tbody", {}, sorted.map((r) => {
      const row = el("tr", { style: "cursor:pointer" }, [
        tickerCell(r.ticker),
        el("td", { class: "mono" }, [fmtPct(r.buy_prob)]),
        el("td", { class: "mono " + pnlClass(r.q50_return) }, [fmtPct(r.q50_return)]),
        el("td", { class: "mono" }, [fmtPct(r.meta_label_prob)]),
        el("td", { class: "mono" }, [fmtNum(r.pnd_score, 0)]),
        el("td", {}, [el("span", { class: "badge " + forensicBadgeClass(r.forensic_flag) }, [r.forensic_flag || "—"])]),
        el("td", { class: "mono" }, [fmtPct(r.mb_probability)]),
        el("td", { style: "font-size:12px;color:var(--tx2);max-width:260px" }, [shapBasisText(r.shap_top5_json)]),
      ]);
      row.addEventListener("dblclick", () => {
        window.open(`signal.html?ticker=${r.ticker}`, "_blank", "noopener");
      });
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadUniverse() {
  showLoading("universe-table");
  apiGet(`/api/v1/signals/ml/universe/${todayStr()}`, { carry_forward: true })
    .then((rows) => {
      universeRows = rows;
      renderUniverseTable();
    })
    .catch((e) => showError("universe-table", e));
}

loadUniverse();
