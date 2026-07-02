// dashboard/static/fundamental/js/sector.js — FA-C Sector Deep-Dive (Partial:
// sector aggregates of standard ratios are real; sector-unique metrics like
// GNPA/ANDA are never computed anywhere, kept as a separate empty-stated panel.
renderAppShell("fundamental", "sector");

const KEY_RATIOS = [
  ["roe", "ROE"], ["roce", "ROCE"], ["net_margin", "Net Margin"],
  ["debt_to_equity", "Debt/Equity"], ["revenue_growth_yoy", "Revenue Growth YoY"],
];

function load() {
  const sector = document.getElementById("sector-input").value.trim();
  if (!sector) return;
  showLoading("sector-avg-table");

  renderEmptyState("sector-unique", {
    icon: "🏷️",
    detail: "Sector-unique metrics (GNPA for banks, ANDA approvals for pharma, etc.) are not computed anywhere in this codebase yet — only the standard ratio set's sector aggregate, above, is real.",
  });

  apiGet(`/api/v1/fundamentals/sector/${encodeURIComponent(sector)}`)
    .then((r) => {
      document.getElementById("sector-count").textContent = `${r.ticker_count} tickers`;
      const c = document.getElementById("sector-avg-table");
      if (!r.ticker_count) {
        c.innerHTML = `<div class="empty">No tickers found for sector "${sector}"</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, KEY_RATIOS.map(([k, l]) => el("th", {}, [l])))]),
        el("tbody", {}, [el("tr", {}, KEY_RATIOS.map(([k]) => {
          const v = r.avg_ratios[k];
          return el("td", { class: "mono" }, [v === null || v === undefined ? "—" : fmtNum(v, 3)]);
        }))]),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [
        el("div", { class: "stat-sub", style: "margin-bottom:8px" }, [`Sector average z-score (~0 by construction — sector z-scores are mean-centered)`]),
        table,
      ]));
    })
    .catch((e) => showError("sector-avg-table", e));
}

document.getElementById("load-btn").addEventListener("click", load);
