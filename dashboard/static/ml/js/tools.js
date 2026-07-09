// dashboard/static/ml/js/tools.js — Tools / Historical Review
// (#29 — Backdated Entry relocated here from the main Paper Trading screen)
renderAppShell("ml", "tools");
CalendarPicker.attach("backdate-input");

function loadBackdateRecommendations() {
  const date = document.getElementById("backdate-input").value;
  if (!date) return;
  showLoading("backdate-table");
  apiGet(`/api/v1/signals/ml/top_buys/${date}`, { n: 10 })
    .then((rows) => {
      const c = document.getElementById("backdate-table");
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No signals were written for ${date} — the pipeline may not have run that day</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Stock"]), el("th", {}, ["Direction"]), el("th", {}, ["Buy Prob"]), el("th", {}, ["Action"]),
        ])]),
        el("tbody", {}, rows.map((r) => {
          const row = el("tr", {}, [
            el("td", { style: "font-weight:600" }, [r.ticker]),
            el("td", {}, [el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : "b-green") }, [(r.signal_direction || "—").toUpperCase()])]),
            el("td", { class: "mono" }, [fmtPct(r.buy_prob)]),
            el("td", {}, []),
          ]);
          const actionCell = row.lastChild;
          const btn = el("button", {}, ["Buy"]);
          btn.addEventListener("click", () => backdatedBuy(r.ticker, date, row));
          actionCell.appendChild(btn);
          return row;
        })),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("backdate-table", e));
}

function backdatedBuy(ticker, date, row) {
  row.lastChild.style.opacity = "0.5";
  apiPost("/api/v1/paper_trading/backdated_buy", { ticker, date })
    .then((r) => {
      row.lastChild.innerHTML = "";
      const label = r.executed ? `Bought ${r.quantity} @ ${fmtMoney(r.entry_price)}` : (r.detail || "Not executed");
      row.lastChild.appendChild(el("span", { class: "badge " + (r.executed ? "b-green" : "b-gray") }, [label]));
    })
    .catch((e) => {
      row.lastChild.innerHTML = "";
      row.lastChild.appendChild(el("span", { class: "badge b-red" }, [`Failed: ${e.message}`]));
    });
}

document.getElementById("load-backdate-btn").addEventListener("click", loadBackdateRecommendations);
