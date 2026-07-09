// dashboard/static/ml/js/positions.js — ML-D Position Monitor
// (folds in the former standalone Paper Trading screen — /api/v1/paper_trading/*
// is real data closely tied to position monitoring, and has no dedicated
// screen ID of its own in the 27-screen prototype spec)
renderAppShell("ml", "positions");

function decisionBadge(actionType) {
  if (actionType === "buy") return "b-green";
  if (actionType === "sell") return "b-red";
  return "b-amber"; // reduce
}

function loadPendingActions() {
  apiGet("/api/v1/paper_trading/pending")
    .then((r) => {
      const c = document.getElementById("pending-actions");
      if (!r.actions.length) {
        c.innerHTML = `<div class="empty">No pending actions${r.date ? ` for ${r.date}` : ""}</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Action"]), el("th", {}, ["Stock"]), el("th", {}, ["Name"]), el("th", {}, ["Price"]),
          el("th", {}, ["Target"]), el("th", {}, ["Target %"]), el("th", {}, ["Duration"]),
          el("th", {}, ["Reason"]), el("th", {}, ["Decision"]),
        ])]),
        el("tbody", {}, r.actions.map((a) => {
          const targetPct = (a.target_price != null && a.price) ? (a.target_price / a.price - 1) * 100 : null;
          const row = el("tr", { "data-action-id": a.action_id }, [
            el("td", {}, [el("span", { class: "badge " + decisionBadge(a.action_type) }, [a.action_type.toUpperCase()])]),
            el("td", { style: "font-weight:600" }, [a.ticker]),
            el("td", { style: "font-size:12px;color:var(--tx2)" }, [a.company_name || "—"]),
            el("td", { class: "mono" }, [a.price !== null && a.price !== undefined ? fmtMoney(a.price) : "—"]),
            el("td", { class: "mono" }, [a.target_price !== null && a.target_price !== undefined ? fmtMoney(a.target_price) : "—"]),
            el("td", { class: "mono", style: "color:var(--green)" }, [targetPct !== null ? `+${targetPct.toFixed(1)}%` : "—"]),
            el("td", { class: "mono" }, [a.duration_days !== null && a.duration_days !== undefined ? `${a.duration_days}d` : "—"]),
            el("td", { style: "font-size:12px;color:var(--tx2)" }, [a.reason || "—"]),
            el("td", {}, []),
          ]);
          const decisionCell = row.lastChild;
          const acceptBtn = el("button", { style: "margin-right:6px" }, ["Accept"]);
          const rejectBtn = el("button", { style: "background:var(--red)" }, ["Reject"]);
          acceptBtn.addEventListener("click", () => decide(a.action_id, "accept", row));
          rejectBtn.addEventListener("click", () => decide(a.action_id, "reject", row));
          decisionCell.appendChild(acceptBtn);
          decisionCell.appendChild(rejectBtn);
          return row;
        })),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("pending-actions", e));
}

function decide(actionId, decision, row) {
  row.style.opacity = "0.5";
  apiPost(`/api/v1/paper_trading/pending/${actionId}/${decision}`)
    .then((r) => {
      row.lastChild.innerHTML = "";
      const label = r.executed ? "Executed" : (r.status === "rejected" ? "Rejected" : "Not executed");
      row.lastChild.appendChild(el("span", { class: "badge " + (r.executed ? "b-green" : "b-gray") }, [label + (r.detail ? ` — ${r.detail}` : "")]));
      row.style.opacity = "1";
      loadState();
    })
    .catch((e) => {
      row.style.opacity = "1";
      row.lastChild.innerHTML = "";
      row.lastChild.appendChild(el("span", { class: "badge b-red" }, [`Failed: ${e.message}`]));
    });
}

function loadGateStatus() {
  apiGet("/api/v1/paper_trading/gate_status")
    .then((r) => {
      const c = document.getElementById("gate-status");
      const pct = Math.min(100, (r.days_count / r.gate_threshold) * 100);
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "card" }, [
          el("div", { class: "grid grid-3", style: "margin-bottom:10px" }, [
            el("div", {}, [el("div", { class: "stat-label" }, ["Days Logged"]), el("div", { class: "stat-value" }, [String(r.days_count)])]),
            el("div", {}, [el("div", { class: "stat-label" }, ["Threshold"]), el("div", { class: "stat-value" }, [String(r.gate_threshold)])]),
            el("div", {}, [el("div", { class: "stat-label" }, ["Status"]), el("div", {}, [el("span", { class: "badge " + (r.gate_cleared ? "b-green" : "b-amber") }, [r.gate_cleared ? "CLEARED" : "in progress"])])]),
          ]),
          el("div", { style: "background:var(--bg3);border-radius:999px;height:8px;overflow:hidden" }, [
            el("div", { style: `background:var(--teal);height:100%;width:${pct}%` }, []),
          ]),
        ])
      );
    })
    .catch((e) => showError("gate-status", e));
}

function loadState() {
  apiGet("/api/v1/paper_trading/state")
    .then((r) => {
      const sc = document.getElementById("portfolio-state");
      const pc = document.getElementById("positions");
      if (!r.available) {
        sc.innerHTML = `<div class="empty">No paper trading runs yet — the daily bot hasn't run</div>`;
        pc.innerHTML = `<div class="empty">—</div>`;
        return;
      }
      document.getElementById("state-date").textContent = r.as_of_date || "";
      const pnl = r.total_equity - r.initial_capital;
      const pnlPct = r.initial_capital ? pnl / r.initial_capital : 0;
      sc.innerHTML = "";
      sc.appendChild(
        el("div", { class: "grid grid-4" }, [
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Total Equity"]), el("div", { class: "stat-value" }, [fmtMoney(r.total_equity)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Cash"]), el("div", { class: "stat-value" }, [fmtMoney(r.cash)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["P&L"]), el("div", { class: "stat-value " + pnlClass(pnl) }, [fmtMoney(pnl)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["P&L %"]), el("div", { class: "stat-value " + pnlClass(pnl) }, [fmtPct(pnlPct)])]),
        ])
      );

      const real = r.positions.filter((p) => p.ticker !== "_HEARTBEAT_");
      if (!real.length) {
        pc.innerHTML = `<div class="empty">No open positions</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Ticker"]), el("th", {}, ["Name"]), el("th", {}, ["Sector"]), el("th", {}, ["Entry Date"]),
          el("th", {}, ["Entry Price"]), el("th", {}, ["Qty"]), el("th", {}, ["Current"]), el("th", {}, ["Unrealised P&L"]),
          el("th", {}, ["Buy Prob (Entry)"]), el("th", {}, ["Buy Prob (Now)"]),
          el("th", {}, ["Target Price"]), el("th", {}, ["Target Date"]),
          el("th", {}, ["Stock Gain"]), el("th", {}, ["Nifty Gain"]),
          el("th", {}, ["Exit Criterion"]), el("th", {}, ["Action"]),
        ])]),
        el("tbody", {}, real.map((p) => {
          const row = el("tr", {}, [
            el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${p.ticker}` }, [p.ticker])]),
            el("td", { style: "font-size:12px;color:var(--tx2)" }, [p.company_name || "—"]),
            el("td", {}, [p.sector || "—"]),
            el("td", { class: "mono" }, [p.entry_date]),
            el("td", { class: "mono" }, [fmtMoney(p.entry_price)]),
            el("td", { class: "mono" }, [fmtInt(p.quantity)]),
            el("td", { class: "mono" }, [p.current_price ? fmtMoney(p.current_price) : "—"]),
            el("td", { class: "mono " + pnlClass(p.unrealised_pnl_pct) }, [fmtPct(p.unrealised_pnl_pct)]),
            el("td", { class: "mono" }, [p.buy_prob_entry != null ? fmtPct(p.buy_prob_entry) : "—"]),
            el("td", { class: "mono" }, [p.buy_prob_current != null ? fmtPct(p.buy_prob_current) : "—"]),
            el("td", { class: "mono" }, [p.target_price != null ? fmtMoney(p.target_price) : "—"]),
            el("td", { class: "mono" }, [p.target_date || "—"]),
            el("td", { class: "mono " + pnlClass(p.stock_gain_pct) }, [p.stock_gain_pct != null ? fmtPct(p.stock_gain_pct) : "—"]),
            el("td", { class: "mono " + pnlClass(p.nifty_gain_pct) }, [p.nifty_gain_pct != null ? fmtPct(p.nifty_gain_pct) : "—"]),
            el("td", { style: "font-size:11px;color:var(--tx2)" }, [p.exit_criterion || "—"]),
            el("td", {}, []),
          ]);
          const actionCell = row.lastChild;
          const sellBtn = el("button", { style: "background:var(--red)" }, ["Sell"]);
          sellBtn.addEventListener("click", () => sellPosition(p.ticker, row, sellBtn));
          actionCell.appendChild(sellBtn);
          return row;
        })),
      ]);
      pc.innerHTML = "";
      pc.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => {
      showError("portfolio-state", e);
      showError("positions", e);
    });
}

function sellPosition(ticker, row, btn) {
  if (!confirm(`Sell ${ticker} at the current market price?`)) return;
  btn.disabled = true;
  row.style.opacity = "0.5";
  apiPost(`/api/v1/paper_trading/positions/${ticker}/sell`)
    .then(() => {
      loadState();
    })
    .catch((e) => {
      row.style.opacity = "1";
      btn.disabled = false;
      alert(`Sell failed: ${e.message}`);
    });
}

function drawEquityChart(points) {
  const canvas = document.getElementById("equity-chart");
  const ctx = canvas.getContext("2d");
  canvas.width = canvas.clientWidth;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!points.length) {
    ctx.fillStyle = "#8E95A8";
    ctx.fillText("No equity curve data yet", 10, 20);
    return;
  }
  const vals = points.map((p) => p.equity);
  const min = Math.min(...vals);
  const max = Math.max(...vals, min + 1);
  const w = canvas.width / Math.max(points.length - 1, 1);
  ctx.strokeStyle = "#0A9B8E";
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((p, i) => {
    const x = i * w;
    const y = canvas.height - ((p.equity - min) / (max - min)) * (canvas.height - 20) - 10;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function loadEquityCurve() {
  apiGet("/api/v1/paper_trading/equity_curve")
    .then((r) => drawEquityChart(r.points))
    .catch(() => drawEquityChart([]));
}

function loadTrades() {
  apiGet("/api/v1/paper_trading/trades")
    .then((r) => {
      const c = document.getElementById("trades");
      if (!r.trades.length) {
        c.innerHTML = `<div class="empty">No closed trades yet</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Ticker"]), el("th", {}, ["Entry"]), el("th", {}, ["Exit"]),
          el("th", {}, ["Exit Type"]), el("th", {}, ["P&L"]), el("th", {}, ["P&L %"]),
        ])]),
        el("tbody", {}, r.trades.slice(0, 50).map((t) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [t.ticker]),
          el("td", { class: "mono" }, [`${fmtMoney(t.entry_price)} (${t.date})`]),
          el("td", { class: "mono" }, [`${t.exit_price ? fmtMoney(t.exit_price) : "—"} (${t.exit_date || "—"})`]),
          el("td", {}, [el("span", { class: "badge b-gray" }, [t.exit_type || "—"])]),
          el("td", { class: "mono " + pnlClass(t.pnl) }, [t.pnl !== null && t.pnl !== undefined ? fmtMoney(t.pnl) : "—"]),
          el("td", { class: "mono " + pnlClass(t.pnl_pct) }, [fmtPct(t.pnl_pct)]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("trades", e));
}

loadGateStatus();
loadPendingActions();
loadState();
loadEquityCurve();
loadTrades();
