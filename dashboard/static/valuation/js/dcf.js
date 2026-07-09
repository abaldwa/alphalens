// dashboard/static/valuation/js/dcf.js — Valuation Dashboard (single-ticker DCF)
// GET /api/v1/valuation/{ticker} and /api/v1/valuation/{ticker}/sensitivity
renderAppShell("valuation", "dcf");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function overallValuationBadge(mos) {
  if (mos == null) return el("span", { class: "badge b-gray" }, ["N/A"]);
  if (mos > 0.15) return el("span", { class: "badge b-green" }, ["Undervalued"]);
  if (mos < -0.15) return el("span", { class: "badge b-red" }, ["Overvalued"]);
  return el("span", { class: "badge b-amber" }, ["Fairly Valued"]);
}

function loadSummary(ticker) {
  showLoading("dcf-summary");
  apiGet(`/api/v1/valuation/${ticker}`)
    .then((r) => {
      const c = document.getElementById("dcf-summary");
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "grid grid-4" }, [
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Overall Valuation"]), el("div", { class: "stat-value" }, [overallValuationBadge(r.margin_of_safety)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["CMP"]), el("div", { class: "stat-value" }, [fmtMoney(r.current_price)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Intrinsic Value"]), el("div", { class: "stat-value" }, [r.intrinsic_value != null ? fmtMoney(r.intrinsic_value) : "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["% Difference"]), el("div", { class: "stat-value " + pnlClass(r.valuation_gap_pct != null ? -r.valuation_gap_pct : null) }, [r.valuation_gap_pct != null ? `${(r.valuation_gap_pct * 100).toFixed(1)}%` : "—"])]),
        ])
      );
      c.appendChild(
        el("div", { class: "grid grid-4", style: "margin-top:12px" }, [
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Lifecycle Stage"]), el("div", {}, [r.lifecycle_stage || "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["DCF Model"]), el("div", {}, [r.dcf_model_type || "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["WACC"]), el("div", {}, [r.wacc != null ? fmtPct(r.wacc) : "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Cost of Equity"]), el("div", {}, [r.cost_of_equity != null ? fmtPct(r.cost_of_equity) : "—"])]),
        ])
      );
      renderScenarios(r);
    })
    .catch((e) => {
      showError("dcf-summary", e);
      document.getElementById("dcf-scenarios").innerHTML = "";
    });
}

function renderScenarios(r) {
  const c = document.getElementById("dcf-scenarios");
  if (r.scenario_bull == null && r.scenario_base == null && r.scenario_bear == null) {
    c.innerHTML = `<div class="empty">No Monte Carlo scenario data (distressed or excess-return models skip MC)</div>`;
    return;
  }
  c.innerHTML = "";
  c.appendChild(
    el("div", { class: "grid grid-4" }, [
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Bear (P10)"]), el("div", { class: "stat-value" }, [r.scenario_bear != null ? fmtMoney(r.scenario_bear) : "—"])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Base (Median)"]), el("div", { class: "stat-value" }, [r.scenario_base != null ? fmtMoney(r.scenario_base) : "—"])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Bull (P90)"]), el("div", { class: "stat-value" }, [r.scenario_bull != null ? fmtMoney(r.scenario_bull) : "—"])]),
      el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["P(Undervalued)"]), el("div", { class: "stat-value" }, [r.mc_probability_undervalued != null ? fmtPct(r.mc_probability_undervalued) : "—"])]),
    ])
  );
}

function loadSensitivity(ticker) {
  showLoading("dcf-sensitivity");
  apiGet(`/api/v1/valuation/${ticker}/sensitivity`)
    .then((r) => {
      const c = document.getElementById("dcf-sensitivity");
      const waccVals = [...new Set(r.table.map((cell) => cell.wacc))].sort((a, b) => a - b);
      const growthVals = [...new Set(r.table.map((cell) => cell.terminal_growth))].sort((a, b) => a - b);
      const byKey = {};
      r.table.forEach((cell) => { byKey[`${cell.wacc}|${cell.terminal_growth}`] = cell.intrinsic_value; });

      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["WACC \\ Growth"]),
          ...growthVals.map((g) => el("th", { class: "mono" }, [`${(g * 100).toFixed(0)}%`])),
        ])]),
        el("tbody", {}, waccVals.map((w) => el("tr", {}, [
          el("th", { class: "mono", style: "font-weight:600" }, [`${(w * 100).toFixed(1)}%`]),
          ...growthVals.map((g) => {
            const v = byKey[`${w}|${g}`];
            const isBase = Math.abs(w - r.base_wacc) < 1e-6 && Math.abs(g - r.base_terminal_growth) < 1e-6;
            return el("td", { class: "mono", style: isBase ? "background:var(--bg3);font-weight:700" : "" }, [v != null ? fmtMoney(v) : "—"]);
          }),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("dcf-sensitivity", e));
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  loadSummary(ticker);
  loadSensitivity(ticker);
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
