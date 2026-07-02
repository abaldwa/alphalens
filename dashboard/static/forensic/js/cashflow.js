// dashboard/static/forensic/js/cashflow.js — FOREN-D Cash Flow Deep Dive
//
// FundamentalsRow has no raw "CFO" line item — only `fcf` (free cash flow)
// and `pat` (net income). We render those real series, honestly labeled,
// rather than the prototype's literal "CFO vs NI" framing. Accrual-quality
// summary (sloan_accrual, dechow_f) comes from the real forensic row.
renderAppShell("forensic", "cashflow");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("cashflow-content");
  Promise.all([
    apiGet(`/api/v1/signals/ml/forensic/${ticker}`),
    apiGet(`/api/v1/fundamentals/${ticker}/history`).catch(() => null),
  ])
    .then(([forensicRow, fundHistory]) => {
      const c = document.getElementById("cashflow-content");
      c.innerHTML = "";
      c.appendChild(el("div", { class: "sec-title", style: "margin-bottom:16px" }, [`Cash Flow Quality — ${ticker}`]));

      const accrualCard = el("div", { class: "card", style: "margin-bottom:16px" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Accrual Quality Summary"]),
      ]);
      if (forensicRow && (forensicRow.sloan_accrual !== null || forensicRow.dechow_f !== null)) {
        accrualCard.appendChild(
          el("div", { class: "grid grid-2" }, [
            el("div", {}, [el("div", { class: "stat-label" }, ["Sloan Accrual Ratio"]), el("div", { class: "stat-value" }, [fmtNum(forensicRow.sloan_accrual, 3)])]),
            el("div", {}, [el("div", { class: "stat-label" }, ["Dechow F-Score"]), el("div", { class: "stat-value" }, [fmtNum(forensicRow.dechow_f, 2)])]),
          ])
        );
      } else {
        accrualCard.appendChild(el("div", { class: "empty" }, ["No accrual-quality scores for this ticker"]));
      }
      c.appendChild(accrualCard);

      const fcfCard = el("div", { class: "card" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Free Cash Flow vs PAT — Quarterly"]),
      ]);
      const rows = (fundHistory && fundHistory.data) || [];
      if (rows.length) {
        const sorted = [...rows].sort((a, b) => new Date(a.quarter_end_date) - new Date(b.quarter_end_date));
        const table = el("table", {}, [
          el("thead", {}, [el("tr", {}, [el("th", {}, ["Quarter"]), el("th", {}, ["PAT"]), el("th", {}, ["FCF"]), el("th", {}, ["Capex"])])]),
          el("tbody", {}, sorted.map((r) => el("tr", {}, [
            el("td", {}, [`${r.fiscal_year} Q${r.quarter}`]),
            el("td", {}, [r.pat !== null && r.pat !== undefined ? fmtMoney(r.pat) : "—"]),
            el("td", {}, [r.fcf !== null && r.fcf !== undefined ? fmtMoney(r.fcf) : "—"]),
            el("td", {}, [r.capex !== null && r.capex !== undefined ? fmtMoney(r.capex) : "—"]),
          ]))),
        ]);
        fcfCard.appendChild(table);
      } else {
        fcfCard.appendChild(el("div", { class: "empty" }, ["No quarterly fundamentals history for this ticker"]));
      }
      c.appendChild(fcfCard);
    })
    .catch((e) => showError("cashflow-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
