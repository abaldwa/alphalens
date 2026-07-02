// dashboard/static/forensic/js/redflag.js — FOREN-B Red Flag Drill-Down
//
// Severity thresholds below are the documented constants from
// systems/ml_signal_engine/models/forensic/classical_scores.py
// (BENEISH_MANIPULATOR_THRESHOLD=-1.78, ALTMAN_DISTRESS_THRESHOLD=1.81,
// ALTMAN_SAFE_THRESHOLD=2.99, PIOTROSKI_WEAK_THRESHOLD=2) — real backend
// thresholds applied to real backend values, not invented cutoffs.
renderAppShell("forensic", "redflag");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function flagPanel(severity, title, detail) {
  const colors = { critical: "var(--red)", high: "var(--amber)", info: "var(--blue)" };
  const badges = { critical: ["b-red", "🔴 CRITICAL"], high: ["b-amber", "⚠ HIGH"], info: ["b-blue", "ℹ INFO"] };
  const [badgeCls, badgeText] = badges[severity];
  return el("div", { class: "card", style: `margin-bottom:12px;border-left:3px solid ${colors[severity]}` }, [
    el("div", { style: "display:flex;align-items:center;gap:12px" }, [
      el("span", { class: "badge " + badgeCls }, [badgeText]),
      el("span", { style: "font-weight:600" }, [title]),
    ]),
    el("div", { style: "margin-top:12px;font-size:12px;color:var(--tx2)" }, [detail]),
  ]);
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("flags-content");
  apiGet(`/api/v1/signals/ml/forensic/${ticker}`)
    .then((row) => {
      const c = document.getElementById("flags-content");
      if (!row) {
        c.innerHTML = `<div class="empty">No forensic score for ${ticker}</div>`;
        return;
      }
      c.innerHTML = "";
      c.appendChild(el("div", { class: "sec-title", style: "margin-bottom:16px" }, [`Red Flag Drill-Down — ${ticker}`]));

      const panels = [];
      if (row.beneish_m !== null && row.beneish_m !== undefined) {
        const flagged = row.beneish_m > -1.78;
        panels.push(flagPanel(flagged ? "critical" : "info", `Beneish M-Score (${fmtNum(row.beneish_m, 2)})`,
          `Manipulator threshold: -1.78 · ${flagged ? "Above threshold — earnings manipulation risk" : "Below threshold — within normal range"}`));
      }
      if (row.altman_z !== null && row.altman_z !== undefined) {
        const distress = row.altman_z < 1.81;
        const safe = row.altman_z > 2.99;
        panels.push(flagPanel(distress ? "critical" : (safe ? "info" : "high"), `Altman Z-Score (${fmtNum(row.altman_z, 2)})`,
          `Distress zone: < 1.81 · Safe zone: > 2.99 · ${distress ? "In distress zone" : safe ? "In safe zone" : "In grey zone"}`));
      }
      if (row.piotroski_f !== null && row.piotroski_f !== undefined) {
        const weak = row.piotroski_f <= 2;
        panels.push(flagPanel(weak ? "high" : "info", `Piotroski F-Score (${fmtNum(row.piotroski_f, 0)})`,
          `Weak threshold: <= 2 · ${weak ? "Weak fundamental quality" : "Within normal range"}`));
      }
      if (row.ohlson_o !== null && row.ohlson_o !== undefined) {
        panels.push(flagPanel("info", `Ohlson O-Score (${fmtNum(row.ohlson_o, 2)})`, "Bankruptcy-risk score — higher values indicate higher risk"));
      }
      if (row.dechow_f !== null && row.dechow_f !== undefined) {
        panels.push(flagPanel("info", `Dechow F-Score (${fmtNum(row.dechow_f, 2)})`, "Earnings-misstatement probability score"));
      }
      if (row.sloan_accrual !== null && row.sloan_accrual !== undefined) {
        panels.push(flagPanel("info", `Sloan Accrual Ratio (${fmtNum(row.sloan_accrual, 3)})`, "Higher values indicate lower earnings quality (more accrual-driven, less cash-driven)"));
      }
      if (row.forensic_ml_prob !== null && row.forensic_ml_prob !== undefined) {
        const high = row.forensic_ml_prob > 0.5;
        panels.push(flagPanel(high ? "high" : "info", `ML Fraud Probability (${fmtPct(row.forensic_ml_prob)})`, "Ensemble model's estimated fraud probability"));
      }
      if (row.benford_mad !== null && row.benford_mad !== undefined) {
        panels.push(flagPanel("info", `Benford MAD (${fmtNum(row.benford_mad, 4)})`, "Mean absolute deviation from Benford's Law digit distribution — see the Benford screen for detail"));
      }
      if (row.pattern_match) {
        panels.push(flagPanel("high", "Historical Pattern Match", row.pattern_match));
      }

      if (!panels.length) {
        c.appendChild(el("div", { class: "empty" }, ["No classical forensic scores available for this ticker"]));
        return;
      }
      panels.forEach((p) => c.appendChild(p));
    })
    .catch((e) => showError("flags-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
