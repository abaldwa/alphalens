// dashboard/static/momentum/js/universe.js — ML38 live momentum ranking
renderAppShell("momentum", "universe");

let currentStrategyId = null;

function renderUniverse(rows) {
  const c = document.getElementById("universe-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No ranking available yet for today — the daily pipeline's compute_momentum step may not have run yet</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Rank"]), el("th", {}, ["Ticker"]), el("th", {}, ["Name"]), el("th", {}, ["Price"]),
      el("th", {}, ["Trailing 6mo Return"]), el("th", {}, ["20d Return"]), el("th", {}, ["30d Trend"]), el("th", {}, ["In Top 15"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { class: "mono" }, [String(r.momentum_rank)]),
      el("td", { style: "font-weight:600" }, [el("a", { href: `../technical/chart.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("td", { style: "font-size:12px;color:var(--tx2)" }, [r.company_name || "—"]),
      el("td", { class: "mono" }, [r.price != null ? fmtMoney(r.price) : "—"]),
      el("td", { class: "mono " + pnlClass(r.momentum_return) }, [fmtPct(r.momentum_return)]),
      el("td", { class: "mono " + pnlClass(r.return_20d) }, [r.return_20d != null ? fmtPct(r.return_20d) : "—"]),
      el("td", { html: sparklineSvg(r.sparkline, { strokeAuto: true }) }, []),
      el("td", {}, [el("span", { class: "badge " + (r.in_top_n ? "b-green" : "b-gray") }, [r.in_top_n ? "Yes" : "No"])]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { style: "margin-bottom:8px; color:var(--muted, #888)" }, [
    `${rows.length} ticker(s) ranked`,
  ]));
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadUniverse() {
  if (!currentStrategyId) return;
  showLoading("universe-table");
  apiGet("/api/v1/momentum/universe", { strategy_id: currentStrategyId })
    .then(renderUniverse)
    .catch((e) => showError("universe-table", e));
}

initMomentumStrategyDropdown("strategy-picker", (strategyId) => {
  currentStrategyId = strategyId;
  loadUniverse();
});
