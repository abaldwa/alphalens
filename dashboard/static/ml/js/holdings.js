// dashboard/static/ml/js/holdings.js — #24 Upload-current-portfolio page
// Storage note: uploaded (ticker, quantity) rows are kept ONLY in this
// browser's localStorage under HOLDINGS_KEY below — no server table, no
// backend write. This is a deliberate read-only "monitor my real holdings"
// layer over the existing signals endpoints; explicitly excluded from any
// model training/backtest data since nothing here is ever persisted server-side.
renderAppShell("ml", "holdings");

const HOLDINGS_KEY = "alphalens_my_holdings_v1";

function loadStoredHoldings() {
  try {
    return JSON.parse(localStorage.getItem(HOLDINGS_KEY) || "[]");
  } catch (e) {
    return [];
  }
}

function saveStoredHoldings(rows) {
  localStorage.setItem(HOLDINGS_KEY, JSON.stringify(rows));
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (!lines.length) return [];
  const header = lines[0].toLowerCase().split(",").map((h) => h.trim());
  const tickerIdx = header.indexOf("ticker");
  const qtyIdx = header.indexOf("quantity");
  const startIdx = tickerIdx === -1 ? 0 : 1;
  const rows = [];
  for (let i = startIdx; i < lines.length; i++) {
    const cols = lines[i].split(",");
    const ticker = (tickerIdx === -1 ? cols[0] : cols[tickerIdx]).trim().toUpperCase();
    const qty = Number(tickerIdx === -1 ? cols[1] : cols[qtyIdx]);
    if (ticker) rows.push({ ticker, quantity: Number.isFinite(qty) ? qty : null });
  }
  return rows;
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function renderHoldings(rows, signalsByTicker) {
  const c = document.getElementById("holdings-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">Upload a CSV to see your holdings' signals</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]), el("th", {}, ["Quantity"]), el("th", {}, ["Direction (signal_5d)"]),
      el("th", {}, ["Buy Prob"]), el("th", {}, ["Exit Urgency"]), el("th", {}, ["Exit Type"]),
      el("th", {}, ["P&D Score"]), el("th", {}, ["Remove"]),
    ])]),
    el("tbody", {}, rows.map((h) => {
      const sig = signalsByTicker[h.ticker];
      const row = el("tr", {}, [
        el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${h.ticker}` }, [h.ticker])]),
        el("td", { class: "mono" }, [h.quantity != null ? fmtInt(h.quantity) : "—"]),
        el("td", {}, [el("span", { class: "badge " + (sig && sig.signal_direction === "sell" ? "b-red" : sig && sig.signal_direction === "buy" ? "b-green" : "b-blue") }, [sig ? (sig.signal_direction || "—") : "no signal"])]),
        el("td", { class: "mono" }, [sig ? fmtPct(sig.buy_prob) : "—"]),
        el("td", { class: "mono" }, [sig && sig.exit_urgency != null ? fmtNum(sig.exit_urgency, 0) : "—"]),
        el("td", {}, [el("span", { class: "badge b-gray" }, [sig ? (sig.exit_type || "—") : "—"])]),
        el("td", { class: "mono" }, [sig ? fmtNum(sig.pnd_score, 0) : "—"]),
        el("td", {}, []),
      ]);
      const btn = el("button", { style: "background:var(--red)" }, ["x"]);
      btn.addEventListener("click", () => {
        const remaining = loadStoredHoldings().filter((r) => r.ticker !== h.ticker);
        saveStoredHoldings(remaining);
        refresh();
      });
      row.lastChild.appendChild(btn);
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function refresh() {
  const rows = loadStoredHoldings();
  if (!rows.length) {
    renderHoldings([], {});
    return;
  }
  showLoading("holdings-table");
  const today = todayStr();
  Promise.all(
    rows.map((h) =>
      apiGet(`/api/v1/signals/ml/${h.ticker}/${today}`, { carry_forward: true })
        .then((sigs) => sigs.find((s) => s.model_name === "signal_5d") || null)
        .catch(() => null)
    )
  ).then((sigs) => {
    const signalsByTicker = {};
    rows.forEach((h, i) => {
      signalsByTicker[h.ticker] = sigs[i];
    });
    renderHoldings(rows, signalsByTicker);
  });
}

document.getElementById("csv-input").addEventListener("change", (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    const parsed = parseCsv(reader.result);
    const existing = loadStoredHoldings();
    const byTicker = {};
    existing.forEach((r) => (byTicker[r.ticker] = r));
    parsed.forEach((r) => (byTicker[r.ticker] = r));
    saveStoredHoldings(Object.values(byTicker));
    refresh();
  };
  reader.readAsText(file);
});

document.getElementById("clear-btn").addEventListener("click", () => {
  if (!confirm("Clear all uploaded holdings from this browser?")) return;
  localStorage.removeItem(HOLDINGS_KEY);
  refresh();
});

refresh();
