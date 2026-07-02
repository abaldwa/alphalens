// dashboard/static/fundamental/js/thesis.js — FA-E Thesis Builder
//
// Real, templated synthesis (same pattern as Forensic's FOREN-F investigation
// report) — every "strength"/"risk" sentence is generated from a real
// sector-relative z-score crossing a documented +/-0.5 threshold, never
// generative/LLM text.
renderAppShell("fundamental", "thesis");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

const RATIO_LABELS = {
  roe: "ROE", roce: "ROCE", net_margin: "Net margin", revenue_growth_yoy: "Revenue growth (YoY)",
  eps_growth_yoy: "EPS growth (YoY)", debt_to_equity: "Debt/Equity",
};
// Lower is better for these — flip the sign before applying the +/-0.5 threshold.
const LOWER_IS_BETTER = new Set(["debt_to_equity", "pe_ratio"]);

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("thesis-content");

  Promise.all([
    apiGet(`/api/v1/fundamentals/${ticker}/ratios`),
    apiGet(`/api/v1/fundamentals/${ticker}/scores`),
  ])
    .then(([ratiosResp, scoresResp]) => {
      const c = document.getElementById("thesis-content");
      if (!ratiosResp.available) {
        c.innerHTML = `<div class="empty">No ratio data for ${ticker} yet</div>`;
        return;
      }
      const strengths = [];
      const risks = [];
      Object.entries(RATIO_LABELS).forEach(([key, label]) => {
        const raw = ratiosResp.ratios[key];
        if (raw === null || raw === undefined) return;
        const z = LOWER_IS_BETTER.has(key) ? -raw : raw;
        if (z > 0.5) strengths.push(`${label} is ${fmtNum(z, 1)} sector-std above peers`);
        else if (z < -0.5) risks.push(`${label} is ${fmtNum(Math.abs(z), 1)} sector-std below peers`);
      });

      c.innerHTML = "";
      c.appendChild(
        el("div", { style: "display:flex;align-items:center;gap:12px;margin-bottom:16px" }, [
          el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
          el("span", { class: "badge b-blue" }, [`Quality ${scoresResp.quality_score !== null ? fmtNum(scoresResp.quality_score, 0) : "—"}`]),
          el("span", { class: "badge b-purple" }, [`Growth ${scoresResp.growth_score !== null ? fmtNum(scoresResp.growth_score, 0) : "—"}`]),
        ])
      );

      const strengthsCard = el("div", { class: "card", style: "margin-bottom:12px;border-left:3px solid var(--green)" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:8px;color:var(--green)" }, ["Strengths"]),
      ]);
      if (strengths.length) strengths.forEach((s) => strengthsCard.appendChild(el("div", { style: "font-size:13px;margin-bottom:4px" }, [`• ${s}`])));
      else strengthsCard.appendChild(el("div", { class: "empty" }, ["No ratio is meaningfully above sector peers"]));
      c.appendChild(strengthsCard);

      const risksCard = el("div", { class: "card", style: "border-left:3px solid var(--red)" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:8px;color:var(--red)" }, ["Risks"]),
      ]);
      if (risks.length) risks.forEach((s) => risksCard.appendChild(el("div", { style: "font-size:13px;margin-bottom:4px" }, [`• ${s}`])));
      else risksCard.appendChild(el("div", { class: "empty" }, ["No ratio is meaningfully below sector peers"]));
      c.appendChild(risksCard);
    })
    .catch((e) => showError("thesis-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
