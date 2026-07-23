// dashboard/static/momentum/js/rebalance.js — ML38 rebalance suggestions
renderAppShell("momentum", "rebalance");

let currentStrategyId = null;

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function actionBadgeClass(action) {
  if (action === "add") return "b-green";
  if (action === "exit") return "b-red";
  return "b-amber"; // grace_hold
}

function loadNextRebalance() {
  apiGet("/api/v1/momentum/rebalance/next", { strategy_id: currentStrategyId })
    .then((r) => {
      const c = document.getElementById("next-rebalance-banner");
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "card" }, [
          el("div", { class: "grid grid-2" }, [
            el("div", {}, [el("div", { class: "stat-label" }, ["Last Rebalance"]), el("div", { class: "stat-value" }, [r.last_rebalance_date || "—"])]),
            el("div", {}, [el("div", { class: "stat-label" }, ["Next Rebalance"]), el("div", { class: "stat-value" }, [r.next_rebalance_date || "—"])]),
          ]),
        ])
      );
    })
    .catch((e) => showError("next-rebalance-banner", e));
}

let pendingModalContext = null;

function openTradeModal(suggestion) {
  pendingModalContext = suggestion;
  document.getElementById("modal-ticker").value = suggestion.ticker;
  document.getElementById("modal-date").value = todayStr();
  document.getElementById("modal-qty").value = "";
  document.getElementById("modal-price").value = "";
  document.getElementById("trade-modal").style.display = "block";
}

function closeTradeModal() {
  pendingModalContext = null;
  document.getElementById("trade-modal").style.display = "none";
}

function saveTradeFromModal() {
  if (!pendingModalContext) return;
  const qty = Number(document.getElementById("modal-qty").value);
  const price = Number(document.getElementById("modal-price").value);
  const date = document.getElementById("modal-date").value;
  if (!qty || !price || !date) {
    alert("Qty, price and date are required");
    return;
  }
  const s = pendingModalContext;
  const isExit = s.action === "exit";
  const body = isExit
    ? null // handled via PUT against the existing open trade below
    : {
        strategy_id: currentStrategyId, ticker: s.ticker, purchase_date: date, qty, purchase_price: price,
        entry_rank: s.momentum_rank, suggestion_id: s.id,
      };

  const req = isExit
    ? apiGet("/api/v1/momentum/trades/", { strategy_id: currentStrategyId, open_only: true }).then((trades) => {
        const match = trades.find((t) => t.ticker === s.ticker);
        if (!match) throw new Error(`No open trade found for ${s.ticker}`);
        return apiPut(`/api/v1/momentum/trades/${match.id}`, {
          sale_date: date, sell_price: price, exit_rank: s.momentum_rank,
        });
      })
    : apiPost("/api/v1/momentum/trades/", body);

  req.then(() => {
    closeTradeModal();
    loadSuggestions();
  }).catch((e) => alert(`Save failed: ${e.message}`));
}

function dismissSuggestion(id) {
  apiPost(`/api/v1/momentum/rebalance/suggestions/${id}/dismiss`)
    .then(loadSuggestions)
    .catch((e) => alert(`Dismiss failed: ${e.message}`));
}

function renderSuggestions(rows) {
  const c = document.getElementById("suggestions-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No pending rebalance suggestions — either not a rebalance day yet, or everything's already been actioned</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Action"]), el("th", {}, ["Ticker"]), el("th", {}, ["Momentum Rank"]),
      el("th", {}, ["Grace Remaining"]), el("th", {}, ["Status"]), el("th", {}, ["Do"]),
    ])]),
    el("tbody", {}, rows.map((s) => {
      const row = el("tr", {}, [
        el("td", {}, [el("span", { class: "badge " + actionBadgeClass(s.action) }, [s.action.toUpperCase()])]),
        el("td", { style: "font-weight:600" }, [s.ticker]),
        el("td", { class: "mono" }, [s.momentum_rank != null ? String(s.momentum_rank) : "—"]),
        el("td", { class: "mono" }, [s.grace_remaining != null ? String(s.grace_remaining) : "—"]),
        el("td", {}, [el("span", { class: "badge b-gray" }, [s.status])]),
        el("td", {}, []),
      ]);
      const doCell = row.lastChild;
      if (s.status === "pending" && s.action !== "grace_hold") {
        const recordBtn = el("button", { style: "margin-right:6px" }, [s.action === "add" ? "Record Buy" : "Record Sell"]);
        recordBtn.addEventListener("click", () => openTradeModal(s));
        const dismissBtn = el("button", { style: "background:var(--tx3)" }, ["Dismiss"]);
        dismissBtn.addEventListener("click", () => dismissSuggestion(s.id));
        doCell.appendChild(recordBtn);
        doCell.appendChild(dismissBtn);
      } else if (s.action === "grace_hold") {
        doCell.appendChild(el("span", { style: "font-size:12px;color:var(--tx2)" }, ["still within grace — no action needed"]));
      }
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadSuggestions() {
  if (!currentStrategyId) return;
  showLoading("suggestions-table");
  apiGet("/api/v1/momentum/rebalance/suggestions", { strategy_id: currentStrategyId })
    .then(renderSuggestions)
    .catch((e) => showError("suggestions-table", e));
}

document.getElementById("modal-save").addEventListener("click", saveTradeFromModal);
document.getElementById("modal-cancel").addEventListener("click", closeTradeModal);

initMomentumStrategyDropdown("strategy-picker", (strategyId) => {
  currentStrategyId = strategyId;
  loadNextRebalance();
  loadSuggestions();
});
