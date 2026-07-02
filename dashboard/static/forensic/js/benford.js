// dashboard/static/forensic/js/benford.js — FOREN-C Benford Visualization
//
// Only the summary MAD scalar is exposed by the API (ForensicRow.benford_mad).
// The per-digit distribution / Chi² table the prototype shows needs raw
// per-digit frequency counts that classical_scores.py computes internally
// but never persists or exposes — that sub-panel is honestly empty-stated
// rather than fabricated. Thresholds (0.015 / 0.030) are the real
// BENFORD_MAD_NONCONFORMING / BENFORD_MAD_SIGNIFICANT constants from
// systems/ml_signal_engine/models/forensic/classical_scores.py.
renderAppShell("forensic", "benford");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("benford-content");
  apiGet(`/api/v1/signals/ml/forensic/${ticker}`)
    .then((row) => {
      const c = document.getElementById("benford-content");
      if (!row) {
        c.innerHTML = `<div class="empty">No forensic score for ${ticker}</div>`;
        return;
      }
      c.innerHTML = "";
      c.appendChild(el("div", { class: "sec-title", style: "margin-bottom:16px" }, [`Benford's Law Analysis — ${ticker}`]));

      const card = el("div", { class: "card", style: "margin-bottom:16px" }, [
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Mean Absolute Deviation (MAD)"]),
      ]);
      if (row.benford_mad !== null && row.benford_mad !== undefined) {
        const significant = row.benford_mad > 0.030;
        const nonconforming = row.benford_mad > 0.015;
        card.appendChild(el("div", { style: "font-size:13px" }, [
          el("strong", {}, [`MAD = ${fmtNum(row.benford_mad, 4)}`]),
          ` (non-conforming threshold: 0.015 · significant-deviation threshold: 0.030) `,
          el("span", { class: "badge " + (significant ? "b-red" : nonconforming ? "b-amber" : "b-green") }, [
            significant ? "SIGNIFICANT DEVIATION" : nonconforming ? "NON-CONFORMING" : "CONFORMS",
          ]),
        ]));
      } else {
        card.appendChild(el("div", { class: "empty" }, ["No Benford MAD score for this ticker"]));
      }
      c.appendChild(card);

      const digitWrap = el("div", { id: "digit-distribution" }, []);
      c.appendChild(digitWrap);
      renderEmptyState("digit-distribution", {
        icon: "📊",
        detail: "The API only exposes the summary MAD score — per-digit frequency counts and the Chi² test are computed internally by classical_scores.py but not persisted or exposed via an endpoint.",
      });
    })
    .catch((e) => showError("benford-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
