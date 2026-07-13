// dashboard/static/ml/js/holdings.js — #24 My Holdings page.
// ML30 (2026-07-13): storage moved off browser localStorage into a real
// DB-backed table (my_holdings) via GET/POST/PUT/DELETE
// /api/v1/holdings and POST /api/v1/holdings/upload-csv
// (datastore/api/routers/holdings.py) — this is still a read-only
// "monitor my real holdings against daily signals" layer; nothing here
// ever feeds model training or backtest data (#24).
renderAppShell("ml", "holdings");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function fmtHoldingDate(d) {
  return d || "—";
}

function renderHoldings(rows, signalsByTicker) {
  const c = document.getElementById("holdings-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">Add a holding above or upload a CSV to see your positions' signals</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]), el("th", {}, ["Qty"]), el("th", {}, ["Buy Date"]), el("th", {}, ["Buy Price"]),
      el("th", {}, ["Sale Date"]), el("th", {}, ["Sell Price"]),
      el("th", {}, ["Direction (signal_5d)"]), el("th", {}, ["Buy Prob"]), el("th", {}, ["Exit Urgency"]),
      el("th", {}, ["Exit Type"]), el("th", {}, ["P&D Score"]), el("th", {}, ["Remove"]),
    ])]),
    el("tbody", {}, rows.map((h) => {
      const sig = signalsByTicker[h.ticker];
      const row = el("tr", {}, [
        el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${h.ticker}` }, [h.ticker])]),
        el("td", { class: "mono" }, [h.qty != null ? fmtNum(h.qty, 0) : "—"]),
        el("td", { class: "mono" }, [fmtHoldingDate(h.purchase_date)]),
        el("td", { class: "mono" }, [h.purchase_price != null ? fmtMoney(h.purchase_price) : "—"]),
        el("td", { class: "mono" }, [fmtHoldingDate(h.sale_date)]),
        el("td", { class: "mono" }, [h.sell_price != null ? fmtMoney(h.sell_price) : "—"]),
        el("td", {}, [el("span", { class: "badge " + (sig && sig.signal_direction === "sell" ? "b-red" : sig && sig.signal_direction === "buy" ? "b-green" : "b-blue") }, [sig ? (sig.signal_direction || "—") : "no signal"])]),
        el("td", { class: "mono" }, [sig ? fmtPct(sig.buy_prob) : "—"]),
        el("td", { class: "mono" }, [sig && sig.exit_urgency != null ? fmtNum(sig.exit_urgency, 0) : "—"]),
        el("td", {}, [el("span", { class: "badge b-gray" }, [sig ? (sig.exit_type || "—") : "—"])]),
        el("td", { class: "mono" }, [sig ? fmtNum(sig.pnd_score, 0) : "—"]),
        el("td", {}, []),
      ]);
      const btn = el("button", { style: "background:var(--red)" }, ["x"]);
      btn.addEventListener("click", async () => {
        await apiDelete(`/api/v1/holdings/${h.id}`);
        refresh();
      });
      row.lastChild.appendChild(btn);
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

async function refresh() {
  showLoading("holdings-table");
  let rows;
  try {
    rows = await apiGet("/api/v1/holdings/");
  } catch (e) {
    showError("holdings-table", e);
    return;
  }
  if (!rows.length) {
    renderHoldings([], {});
    return;
  }
  const today = todayStr();
  const sigs = await Promise.all(
    rows.map((h) =>
      apiGet(`/api/v1/signals/ml/${h.ticker}/${today}`, { carry_forward: true })
        .then((s) => s.find((x) => x.model_name === "signal_5d") || null)
        .catch(() => null)
    )
  );
  const signalsByTicker = {};
  rows.forEach((h, i) => {
    signalsByTicker[h.ticker] = sigs[i];
  });
  renderHoldings(rows, signalsByTicker);
}

document.getElementById("csv-input").addEventListener("change", (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async () => {
    try {
      await fetch("/api/v1/holdings/upload-csv", {
        method: "POST",
        headers: { "Content-Type": "text/csv" },
        body: reader.result,
      }).then((r) => {
        if (!r.ok) return r.json().then((body) => { throw new Error(body.detail || "Upload failed"); });
      });
    } catch (err) {
      alert(`CSV upload failed: ${err.message}`);
    }
    e.target.value = "";
    refresh();
  };
  reader.readAsText(file);
});

document.getElementById("add-btn").addEventListener("click", async () => {
  const ticker = document.getElementById("add-ticker").value.trim().toUpperCase();
  const purchase_date = document.getElementById("add-purchase-date").value || todayStr();
  const qty = Number(document.getElementById("add-qty").value);
  const priceVal = document.getElementById("add-price").value;
  const rationale = document.getElementById("add-rationale").value.trim();
  if (!ticker || !qty) {
    alert("Ticker and quantity are required");
    return;
  }
  await apiPost("/api/v1/holdings/", {
    ticker,
    purchase_date,
    qty,
    purchase_price: priceVal ? Number(priceVal) : null,
    purchase_rationale: rationale || null,
  });
  document.getElementById("add-ticker").value = "";
  document.getElementById("add-qty").value = "";
  document.getElementById("add-price").value = "";
  document.getElementById("add-rationale").value = "";
  refresh();
});

refresh();
