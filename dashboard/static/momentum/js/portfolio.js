// dashboard/static/momentum/js/portfolio.js — ML38 Holding Dashboard
renderAppShell("momentum", "portfolio");

let currentStrategyId = null;

function renderSummary(s) {
  const c = document.getElementById("summary-tiles");
  c.innerHTML = "";
  c.appendChild(
    el("div", { class: "grid grid-4" }, [
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Capital Invested (open positions)"]), el("div", { class: "stat-value" }, [fmtMoney(s.capital_invested)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Current Holdings Value"]), el("div", { class: "stat-value" }, [fmtMoney(s.current_holdings_value)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["CAGR"]), el("div", { class: "stat-value " + pnlClass(s.cagr) }, [s.cagr != null ? fmtPct(s.cagr) : "—"])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["XIRR (money-weighted)"]), el("div", { class: "stat-value " + pnlClass(s.xirr) }, [s.xirr != null ? fmtPct(s.xirr) : "—"])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Idle Cash"]), el("div", { class: "stat-value" }, [fmtMoney(s.idle_cash)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Total Net Worth"]), el("div", { class: "stat-value" }, [fmtMoney(s.total_net_worth)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Total Tax Due"]), el("div", { class: "stat-value" }, [fmtMoney(s.total_tax_due)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Post-Tax Value"]), el("div", { class: "stat-value" }, [fmtMoney(s.post_tax_value)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Total Contributed"]), el("div", { class: "stat-value" }, [fmtMoney(s.total_contributed)])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["As Of"]), el("div", { class: "stat-value" }, [s.as_of_date])]),
    ])
  );
}

function loadSummary() {
  if (!currentStrategyId) return;
  apiGet("/api/v1/momentum/summary", { strategy_id: currentStrategyId })
    .then(renderSummary)
    .catch((e) => showError("summary-tiles", e));
}

function sellPosition(trade) {
  const price = prompt(`Sell price for ${trade.ticker} (qty ${trade.qty})?`);
  if (!price) return;
  const date = prompt("Sale date (YYYY-MM-DD)?", new Date().toISOString().slice(0, 10));
  if (!date) return;
  apiPut(`/api/v1/momentum/trades/${trade.id}`, { sale_date: date, sell_price: Number(price) })
    .then(() => {
      loadPositions();
      loadSummary();
    })
    .catch((e) => alert(`Sell failed: ${e.message}`));
}

function renderPositions(rows) {
  const c = document.getElementById("positions-table");
  const open = rows.filter((r) => r.sale_date == null);
  if (!open.length) {
    c.innerHTML = `<div class="empty">No open positions for this strategy — record a buy above</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]), el("th", {}, ["Purchase Date"]), el("th", {}, ["Qty"]),
      el("th", {}, ["Purchase Price"]), el("th", {}, ["Grace Remaining"]), el("th", {}, ["Sell"]),
    ])]),
    el("tbody", {}, open.map((r) => {
      const row = el("tr", {}, [
        el("td", { style: "font-weight:600" }, [r.ticker]),
        el("td", { class: "mono" }, [r.purchase_date]),
        el("td", { class: "mono" }, [fmtInt(r.qty)]),
        el("td", { class: "mono" }, [r.purchase_price != null ? fmtMoney(r.purchase_price) : "—"]),
        el("td", { class: "mono" }, [r.grace_remaining != null ? String(r.grace_remaining) : "—"]),
        el("td", {}, []),
      ]);
      const sellBtn = el("button", { style: "background:var(--red)" }, ["Sell"]);
      sellBtn.addEventListener("click", () => sellPosition(r));
      row.lastChild.appendChild(sellBtn);
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadPositions() {
  if (!currentStrategyId) return;
  showLoading("positions-table");
  apiGet("/api/v1/momentum/trades/", { strategy_id: currentStrategyId, open_only: true })
    .then(renderPositions)
    .catch((e) => showError("positions-table", e));
}

document.getElementById("add-btn").addEventListener("click", () => {
  const ticker = document.getElementById("add-ticker").value.trim();
  const purchase_date = document.getElementById("add-date").value;
  const qty = Number(document.getElementById("add-qty").value);
  const purchase_price = Number(document.getElementById("add-price").value);
  if (!ticker || !purchase_date || !qty) {
    alert("Ticker, date and qty are required");
    return;
  }
  apiPost("/api/v1/momentum/trades/", {
    strategy_id: currentStrategyId, ticker, purchase_date, qty, purchase_price: purchase_price || null,
  })
    .then(() => {
      document.getElementById("add-ticker").value = "";
      document.getElementById("add-qty").value = "";
      document.getElementById("add-price").value = "";
      loadPositions();
      loadSummary();
    })
    .catch((e) => alert(`Record failed: ${e.message}`));
});

function renderContributions(rows) {
  const c = document.getElementById("contributions-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No contributions recorded yet for this strategy</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [el("th", {}, ["Date"]), el("th", {}, ["Amount"]), el("th", {}, ["Note"])])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { class: "mono" }, [r.contribution_date]),
      el("td", { class: "mono" }, [fmtMoney(r.amount)]),
      el("td", {}, [r.note || "—"]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadContributions() {
  if (!currentStrategyId) return;
  showLoading("contributions-table");
  apiGet("/api/v1/momentum/contributions/", { strategy_id: currentStrategyId })
    .then(renderContributions)
    .catch((e) => showError("contributions-table", e));
}

document.getElementById("contrib-btn").addEventListener("click", () => {
  const contribution_date = document.getElementById("contrib-date").value;
  const amount = Number(document.getElementById("contrib-amount").value);
  const note = document.getElementById("contrib-note").value.trim() || null;
  if (!contribution_date || !amount) {
    alert("Date and amount are required");
    return;
  }
  apiPost("/api/v1/momentum/contributions/", { strategy_id: currentStrategyId, contribution_date, amount, note })
    .then(() => {
      document.getElementById("contrib-amount").value = "";
      document.getElementById("contrib-note").value = "";
      loadContributions();
      loadSummary();
    })
    .catch((e) => alert(`Add failed: ${e.message}`));
});

initMomentumStrategyDropdown("strategy-picker", (strategyId) => {
  currentStrategyId = strategyId;
  loadSummary();
  loadPositions();
  loadContributions();
});
