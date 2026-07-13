// dashboard/static/valuation/js/accuracy.js — F6 Valuation Accuracy
// GET /api/v1/valuation/accuracy/backtest?horizon_days=&min_age_days=
renderAppShell("valuation", "accuracy");

function renderSummary(data) {
  const c = document.getElementById("summary-cards");
  if (!data.scored) {
    c.innerHTML = "";
    return;
  }
  const cards = [
    { label: "Scored predictions", value: `${data.scored} / ${data.count}` },
    { label: "Hit rate (direction)", value: data.hit_rate != null ? fmtPct(data.hit_rate) : "—" },
    { label: "Avg return — undervalued calls", value: data.avg_return_undervalued_pct != null ? fmtNum(data.avg_return_undervalued_pct, 2) + "%" : "—" },
    { label: "Avg return — overvalued calls", value: data.avg_return_overvalued_pct != null ? fmtNum(data.avg_return_overvalued_pct, 2) + "%" : "—" },
  ];
  c.innerHTML = "";
  c.appendChild(el("div", { style: "display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px" },
    cards.map((cd) => el("div", { class: "card" }, [
      el("div", { style: "font-size:12px;color:var(--tx2)" }, [cd.label]),
      el("div", { style: "font-size:20px;font-weight:700;margin-top:4px" }, [cd.value]),
    ]))
  ));
}

function renderTable(rows) {
  const c = document.getElementById("accuracy-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No valuation_signals rows old enough to score yet at this horizon — try a shorter horizon or wait for more history to accumulate.</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]), el("th", {}, ["Signal Date"]), el("th", {}, ["Lifecycle"]),
      el("th", {}, ["MoS"]), el("th", {}, ["Predicted"]), el("th", {}, ["Entry Price"]),
      el("th", {}, ["Realized Date"]), el("th", {}, ["Realized Price"]), el("th", {}, ["Realized Return"]),
      el("th", {}, ["Hit?"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [el("a", { href: `dcf.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("td", { class: "mono" }, [r.signal_date]),
      el("td", {}, [r.lifecycle_stage || "—"]),
      el("td", { class: "mono" }, [r.margin_of_safety != null ? fmtNum(r.margin_of_safety, 2) : "—"]),
      el("td", {}, [r.predicted_undervalued == null ? "—" : el("span", { class: "badge " + (r.predicted_undervalued ? "b-green" : "b-red") }, [r.predicted_undervalued ? "Undervalued" : "Overvalued"])]),
      el("td", { class: "mono" }, [fmtMoney(r.entry_price)]),
      el("td", { class: "mono" }, [r.realized_date]),
      el("td", { class: "mono" }, [fmtMoney(r.realized_price)]),
      el("td", { class: "mono " + pnlClass(r.realized_return_pct) }, [fmtNum(r.realized_return_pct, 2) + "%"]),
      el("td", {}, [r.hit == null ? "—" : el("span", { class: "badge " + (r.hit ? "b-green" : "b-red") }, [r.hit ? "Hit" : "Miss"])]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function run() {
  const horizon = Number(document.getElementById("horizon-input").value) || 5;
  showLoading("accuracy-table");
  document.getElementById("summary-cards").innerHTML = "";
  apiGet("/api/v1/valuation/accuracy/backtest", { horizon_days: horizon })
    .then((data) => {
      renderSummary(data);
      renderTable(data.rows || []);
    })
    .catch((e) => showError("accuracy-table", e));
}

document.getElementById("run-btn").addEventListener("click", run);
run();
