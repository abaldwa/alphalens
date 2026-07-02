// dashboard/static/forensic/js/dashboard.js — FOREN-A Forensic Dashboard
renderAppShell("forensic", "dashboard");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function ringClass(flagLabel, composite) {
  if (flagLabel === "green") return "ring-green";
  if (flagLabel === "red" || flagLabel === "black") return "ring-red";
  if (flagLabel === "amber") return "ring-amber";
  // fall back to composite score if label missing
  if (composite === null || composite === undefined) return "ring-amber";
  if (composite <= 20) return "ring-green";
  if (composite <= 60) return "ring-amber";
  return "ring-red";
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("dash-content");
  apiGet(`/api/v1/signals/ml/forensic/${ticker}`)
    .then((row) => {
      const c = document.getElementById("dash-content");
      if (!row) {
        c.innerHTML = `<div class="empty">No forensic score for ${ticker}</div>`;
        return;
      }
      c.innerHTML = "";

      const header = el("div", { style: "display:flex;align-items:center;gap:16px;margin-bottom:16px" }, [
        el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
        el("div", { class: "score-ring " + ringClass(row.forensic_flag_label, row.forensic_composite) }, [
          row.forensic_composite !== null && row.forensic_composite !== undefined ? fmtNum(row.forensic_composite, 0) : "—",
        ]),
        el("span", { class: "badge " + badgeClass(row.forensic_flag_label === "green" ? "green" : (["red", "black"].includes(row.forensic_flag_label) ? "red" : "amber")) }, [
          row.forensic_flag_label ? row.forensic_flag_label.toUpperCase() : "UNSCORED",
        ]),
      ]);
      header.appendChild(buildCrossLinks(ticker, "forensic"));
      c.appendChild(header);

      if (row.forensic_flag) {
        c.appendChild(el("div", { class: "alert-banner alert-critical" }, ["This stock is BLOCKED from buy recommendations — forensic composite above the block threshold"]));
      }

      // Real classical + ML scores — not the prototype's fabricated 4-category
      // weighted breakdown, since the backend doesn't compute those weights.
      const scoreCards = [
        ["Beneish M-Score", row.beneish_m],
        ["Altman Z-Score", row.altman_z],
        ["Piotroski F-Score", row.piotroski_f],
        ["Ohlson O-Score", row.ohlson_o],
        ["Dechow F-Score", row.dechow_f],
        ["Sloan Accrual", row.sloan_accrual],
        ["Benford MAD", row.benford_mad],
        ["ML Fraud Prob", row.forensic_ml_prob !== null && row.forensic_ml_prob !== undefined ? fmtPct(row.forensic_ml_prob) : null],
      ];
      c.appendChild(
        el("div", { class: "card-grid grid grid-4", style: "margin-bottom:16px" },
          scoreCards.map(([label, v]) => el("div", { class: "card" }, [
            el("div", { class: "stat" }, [
              el("div", { class: "stat-label" }, [label]),
              el("div", { class: "stat-value" }, [v === null || v === undefined ? "—" : (typeof v === "string" ? v : fmtNum(v, 2))]),
            ]),
          ]))
        )
      );

      // SHAP risk factors — only if the API actually returned them
      const shapCard = el("div", { class: "card", style: "margin-bottom:16px" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Top Risk Factors (SHAP)"]),
      ]);
      if (row.shap_top5_json) {
        try {
          const shap = JSON.parse(row.shap_top5_json);
          const entries = Array.isArray(shap) ? shap : Object.entries(shap).map(([k, v]) => ({ feature: k, value: v }));
          entries.forEach((e2) => {
            const val = e2.value ?? e2[1] ?? 0;
            const label = e2.feature || e2[0] || "—";
            shapCard.appendChild(
              el("div", { class: "shap-bar" }, [
                el("span", { class: "shap-label" }, [label]),
                el("div", { class: "shap-track" }, [
                  el("div", { class: "shap-fill " + (val >= 0 ? "shap-pos" : "shap-neg"), style: `width:${Math.min(Math.abs(val) * 100, 50)}%` }, []),
                ]),
                el("span", { class: "mono " + (val >= 0 ? "up" : "dn") }, [fmtNum(val, 3)]),
              ])
            );
          });
        } catch (e) {
          shapCard.appendChild(el("div", { class: "empty" }, ["SHAP data not parseable"]));
        }
      } else {
        shapCard.appendChild(el("div", { class: "empty" }, ["No SHAP attribution available for this ticker"]));
      }
      c.appendChild(shapCard);

      // Historical pattern match — render the real string field as-is, not a
      // fabricated similarity-score table (the API only returns free text here)
      const patternCard = el("div", { class: "card" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Historical Pattern Match"]),
      ]);
      if (row.pattern_match) {
        patternCard.appendChild(el("div", { style: "font-size:13px" }, [row.pattern_match]));
      } else {
        patternCard.appendChild(el("div", { class: "empty" }, ["No pattern match recorded for this ticker"]));
      }
      c.appendChild(patternCard);
    })
    .catch((e) => showError("dash-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
