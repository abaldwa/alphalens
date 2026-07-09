// dashboard/static/ml/js/exit_urgency.js — #23 Dedicated Exit Urgency page
renderAppShell("ml", "exit_urgency");

const sortState = { key: "exit_urgency", dir: "desc" };
let lastRows = [];

function urgencyBadge(u) {
  if (u === null || u === undefined) return "b-gray";
  if (u >= 70) return "b-red";
  if (u >= 40) return "b-amber";
  return "b-green";
}

function render() {
  const c = document.getElementById("exit-urgency-table");
  if (!lastRows.length) {
    c.innerHTML = `<div class="empty">No open positions</div>`;
    return;
  }
  const sorted = sortRows(lastRows, sortState.key, sortState.dir);
  const onSort = (key, dir) => {
    sortState.key = key;
    sortState.dir = dir;
    render();
  };
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Ticker", "ticker", sortState, onSort),
      el("th", {}, ["Name"]),
      sortableHeader("Entry Date", "entry_date", sortState, onSort),
      sortableHeader("Entry Price", "entry_price", sortState, onSort),
      sortableHeader("Current", "current_price", sortState, onSort),
      sortableHeader("Unrealised P&L", "unrealised_pnl_pct", sortState, onSort),
      sortableHeader("Exit Urgency", "exit_urgency", sortState, onSort),
      el("th", {}, ["Exit Type (reason)"]),
    ])]),
    el("tbody", {}, sorted.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("td", { style: "font-size:12px;color:var(--tx2)" }, [r.company_name || "—"]),
      el("td", { class: "mono" }, [r.entry_date]),
      el("td", { class: "mono" }, [fmtMoney(r.entry_price)]),
      el("td", { class: "mono" }, [r.current_price != null ? fmtMoney(r.current_price) : "—"]),
      el("td", { class: "mono " + pnlClass(r.unrealised_pnl_pct) }, [r.unrealised_pnl_pct != null ? fmtPct(r.unrealised_pnl_pct) : "—"]),
      el("td", {}, [el("span", { class: "badge " + urgencyBadge(r.exit_urgency) }, [r.exit_urgency != null ? fmtNum(r.exit_urgency, 0) : "—"])]),
      el("td", {}, [el("span", { class: "badge b-gray" }, [r.exit_type || "—"])]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function load() {
  showLoading("exit-urgency-table");
  apiGet("/api/v1/paper_trading/exit_urgency")
    .then((r) => {
      lastRows = r.rows || [];
      document.getElementById("as-of-date").textContent = r.as_of_date
        ? `All open positions, ranked by exit_urgency — as of ${r.as_of_date}`
        : "All open positions, ranked by exit_urgency";
      render();
    })
    .catch((e) => showError("exit-urgency-table", e));
}

load();
