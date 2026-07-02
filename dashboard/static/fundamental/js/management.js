// dashboard/static/fundamental/js/management.js — FA-F Management Quality
//
// Governance/shareholding (promoter pledge, FII/DII/MF/retail %, superstar
// investor flag) are real via GovernanceRow, plus the real composite
// management_quality_score (features/fundamental_composites.py).
// Related-party-transaction data has no backend (systems/
// fundamental_analysis/management/ is an empty stub beyond this composite)
// — kept as a separate, clearly empty-stated sub-section.
renderAppShell("fundamental", "management");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("gov-content");

  renderEmptyState("rpt-content", {
    icon: "🔗",
    detail: "Related-party-transaction analysis needs a backend that doesn't exist yet — systems/fundamental_analysis/management/ is an empty stub.",
  });

  Promise.all([
    apiGet(`/api/v1/governance/${ticker}`),
    apiGet(`/api/v1/fundamentals/${ticker}/scores`).catch(() => null),
  ])
    .then(([r, scoresResp]) => {
      const c = document.getElementById("gov-content");
      const rows = r.data || [];
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No governance data for ${ticker}</div>`;
        return;
      }
      const latest = [...rows].sort((a, b) => new Date(b.filing_date) - new Date(a.filing_date))[0];
      c.innerHTML = "";
      const mgmtScore = scoresResp && scoresResp.management_quality_score;
      const header = el("div", { style: "display:flex;align-items:center;gap:12px;margin-bottom:12px" }, [
        el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
        el("span", { class: "badge b-blue" }, [`Management Quality ${mgmtScore !== null && mgmtScore !== undefined ? fmtNum(mgmtScore, 0) : "—"}`]),
        latest.superstar_flag ? el("span", { class: "badge b-purple" }, ["Superstar Investor Tracked"]) : null,
      ]);
      header.appendChild(buildCrossLinks(ticker, "fundamental"));
      c.appendChild(header);

      c.appendChild(
        el("div", { class: "card-grid grid grid-4", style: "margin-bottom:16px" }, [
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Promoter %"]), el("div", { class: "stat-value" }, [fmtPct(latest.promoter_pct / 100)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Promoter Pledge %"]), el("div", { class: "stat-value " + (latest.promoter_pledge > 10 ? "dn" : "") }, [fmtPct(latest.promoter_pledge / 100)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["FII %"]), el("div", { class: "stat-value" }, [fmtPct(latest.fii_pct / 100)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["DII %"]), el("div", { class: "stat-value" }, [fmtPct(latest.dii_pct / 100)])])]),
        ])
      );

      const sorted = [...rows].sort((a, b) => new Date(a.filing_date) - new Date(b.filing_date));
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Filing Date"]), el("th", {}, ["Promoter %"]), el("th", {}, ["Pledge %"]),
          el("th", {}, ["FII %"]), el("th", {}, ["DII %"]), el("th", {}, ["MF %"]), el("th", {}, ["Retail %"]),
        ])]),
        el("tbody", {}, sorted.map((row) => el("tr", {}, [
          el("td", { class: "mono" }, [row.filing_date.slice(0, 10)]),
          el("td", { class: "mono" }, [fmtPct(row.promoter_pct / 100)]),
          el("td", { class: "mono" }, [fmtPct(row.promoter_pledge / 100)]),
          el("td", { class: "mono" }, [fmtPct(row.fii_pct / 100)]),
          el("td", { class: "mono" }, [fmtPct(row.dii_pct / 100)]),
          el("td", { class: "mono" }, [fmtPct(row.mf_pct / 100)]),
          el("td", { class: "mono" }, [fmtPct(row.retail_pct / 100)]),
        ]))),
      ]);
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("gov-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
