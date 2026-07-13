// dashboard/static/forensic/js/report.js — FOREN-F Investigation Report
//
// This is a real-data report builder, not a generative/LLM report: every
// sentence is a template populated from real ForensicRow fields. FO6 —
// export is a real server-side PDF (datastore/api/routers/forensic.py's
// GET /{ticker}/report/pdf, reportlab-rendered), not just window.print().
renderAppShell("forensic", "report");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("report-content");
  apiGet(`/api/v1/signals/ml/forensic/${ticker}`)
    .then((row) => {
      const c = document.getElementById("report-content");
      if (!row) {
        c.innerHTML = `<div class="empty">No forensic score for ${ticker}</div>`;
        return;
      }
      c.innerHTML = "";

      const lines = [];
      lines.push(`<strong>Ticker:</strong> ${ticker}`);
      lines.push(`<strong>Forensic Composite:</strong> ${row.forensic_composite !== null && row.forensic_composite !== undefined ? fmtNum(row.forensic_composite, 0) : "—"}/100 (${row.forensic_flag_label ? row.forensic_flag_label.toUpperCase() : "unscored"})`);
      if (row.beneish_m !== null && row.beneish_m !== undefined) lines.push(`<strong>Beneish M-Score:</strong> ${fmtNum(row.beneish_m, 2)} (manipulator threshold: -1.78)`);
      if (row.altman_z !== null && row.altman_z !== undefined) lines.push(`<strong>Altman Z-Score:</strong> ${fmtNum(row.altman_z, 2)} (distress: <1.81, safe: >2.99)`);
      if (row.piotroski_f !== null && row.piotroski_f !== undefined) lines.push(`<strong>Piotroski F-Score:</strong> ${fmtNum(row.piotroski_f, 0)} (weak: <=2)`);
      if (row.sloan_accrual !== null && row.sloan_accrual !== undefined) lines.push(`<strong>Sloan Accrual Ratio:</strong> ${fmtNum(row.sloan_accrual, 3)} (high-accrual: >0.10)`);
      if (row.benford_mad !== null && row.benford_mad !== undefined) lines.push(`<strong>Benford MAD:</strong> ${fmtNum(row.benford_mad, 4)} (non-conforming: >0.015)`);
      if (row.forensic_ml_prob !== null && row.forensic_ml_prob !== undefined) lines.push(`<strong>ML Fraud Probability:</strong> ${fmtPct(row.forensic_ml_prob)}`);
      if (row.pattern_match) lines.push(`<strong>Historical Pattern Match:</strong> ${row.pattern_match}`);
      lines.push(`<strong>Recommendation:</strong> <span class="badge ${row.forensic_flag ? "b-red" : "b-green"}">${row.forensic_flag ? "BLOCKED FROM BUY RECOMMENDATIONS" : "Not currently blocked"}</span>`);

      c.appendChild(
        el("div", { class: "card", html: `<div style="font-size:13px;line-height:1.8">${lines.join("<br>")}</div>` }, [])
      );
      c.appendChild(
        el("div", { style: "margin-top:12px;text-align:center;display:flex;gap:8px;justify-content:center" }, [
          el("button", { onclick: `window.location.href='${API_BASE}/api/v1/signals/ml/forensic/${ticker}/report/pdf'` }, ["Download PDF"]),
          el("button", { onclick: "window.print()" }, ["Print"]),
        ])
      );
    })
    .catch((e) => showError("report-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
