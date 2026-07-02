// dashboard/static/fundamental/js/dashboard.js — FA-A Financial Dashboard
//
// Raw quarterly fundamentals (FundamentalsResponse) are real. The
// traffic-light section now uses real sector-relative z-scored ratios
// (GET /api/v1/fundamentals/{ticker}/ratios) — z > 0.5 = top quartile-ish
// vs sector peers (green), z < -0.5 = bottom (red), else amber — and the
// real quality/growth composite scores (GET /api/v1/fundamentals/{ticker}/scores).
renderAppShell("fundamental", "dashboard");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

const TRAFFIC_LIGHT_RATIOS = [
  ["roe", "ROE"], ["roce", "ROCE"], ["net_margin", "Net Margin"],
  ["debt_to_equity", "Debt/Equity (lower better)"], ["revenue_growth_yoy", "Revenue Growth YoY"],
];

function trafficLightClass(key, z) {
  if (z === null || z === undefined) return "h-neutral";
  // debt_to_equity: lower z (less leverage than sector) is better, so the sign flips.
  const signed = key === "debt_to_equity" ? -z : z;
  if (signed > 0.5) return "h-green";
  if (signed < -0.5) return "h-red";
  return "h-amber";
}

function loadTrafficLight(ticker) {
  Promise.all([
    apiGet(`/api/v1/fundamentals/${ticker}/ratios`),
    apiGet(`/api/v1/fundamentals/${ticker}/scores`),
  ])
    .then(([ratiosResp, scoresResp]) => {
      const c = document.getElementById("fa-traffic-light");
      if (!ratiosResp.available) {
        c.innerHTML = `<div class="empty">No ratio data for ${ticker} yet</div>`;
        return;
      }
      c.innerHTML = "";
      const scoreCards = el("div", { class: "card-grid grid grid-2", style: "margin-bottom:12px" }, [
        el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Quality Score"]), el("div", { class: "stat-value" }, [scoresResp.quality_score !== null ? fmtNum(scoresResp.quality_score, 0) : "—"])])]),
        el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Growth Score"]), el("div", { class: "stat-value" }, [scoresResp.growth_score !== null ? fmtNum(scoresResp.growth_score, 0) : "—"])])]),
      ]);
      const cellsRow = TRAFFIC_LIGHT_RATIOS.map(([key, label]) => {
        const z = ratiosResp.ratios[key];
        return el("td", { class: "heatmap-cell " + trafficLightClass(key, z) }, [z === null || z === undefined ? "—" : fmtNum(z, 2)]);
      });
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, TRAFFIC_LIGHT_RATIOS.map(([k, l]) => el("th", {}, [l])))]),
        el("tbody", {}, [el("tr", {}, cellsRow)]),
      ]);
      c.appendChild(scoreCards);
      c.appendChild(el("div", { class: "card" }, [
        el("div", { class: "stat-sub", style: "margin-bottom:8px" }, ["Sector-relative z-score (vs sector peers, not an absolute threshold)"]),
        table,
      ]));
    })
    .catch((e) => showError("fa-traffic-light", e));
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("fa-table");
  document.getElementById("fa-header").innerHTML = "";
  document.getElementById("fa-header").appendChild(
    el("div", { style: "display:flex;align-items:center;gap:12px" }, [
      el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
    ])
  );
  document.getElementById("fa-header").appendChild(buildCrossLinks(ticker, "fundamental"));

  loadTrafficLight(ticker);

  apiGet(`/api/v1/fundamentals/${ticker}/history`)
    .then((r) => {
      const c = document.getElementById("fa-table");
      const rows = r.data || [];
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No quarterly fundamentals for ${ticker}</div>`;
        return;
      }
      const sorted = [...rows].sort((a, b) => new Date(a.quarter_end_date) - new Date(b.quarter_end_date));
      const cols = ["revenue", "ebitda", "pat", "eps", "operating_margin", "net_margin", "roe", "roce", "debt_to_equity"];
      const labels = ["Revenue", "EBITDA", "PAT", "EPS", "Op Margin", "Net Margin", "ROE", "ROCE", "D/E"];
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Quarter"]), ...labels.map((l) => el("th", {}, [l]))])]),
        el("tbody", {}, sorted.map((row) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [`${row.fiscal_year} Q${row.quarter}`]),
          ...cols.map((cc) => {
            const v = row[cc];
            const isPct = ["operating_margin", "net_margin", "roe", "roce"].includes(cc);
            return el("td", { class: "mono" }, [v === null || v === undefined ? "—" : (isPct ? fmtPct(v) : fmtNum(v, 2))]);
          }),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("fa-table", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
