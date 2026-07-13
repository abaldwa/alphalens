// dashboard/static/forensic/js/benford.js — FOREN-C Benford Visualization
//
// FO5 (2026-07-11): benford_detail_json (ForensicRow) now carries the full
// chi-square statistic/p-value + per-digit (1-9) observed-frequency
// distribution per financial series that classical_scores.py's
// benford_analysis() already computed internally — previously only the
// single aggregate benford_mad float reached this screen. Thresholds
// (0.015 / 0.030) are the real BENFORD_MAD_NONCONFORMING /
// BENFORD_MAD_SIGNIFICANT constants from
// systems/ml_signal_engine/models/forensic/classical_scores.py.
renderAppShell("forensic", "benford");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function digitBar(observed, expected, digit) {
  const oPct = observed != null ? observed * 100 : null;
  const ePct = expected * 100;
  return el("div", { style: "display:flex;align-items:center;gap:8px;margin-bottom:4px" }, [
    el("div", { style: "width:16px;font-size:12px;color:var(--tx2)" }, [String(digit)]),
    el("div", { style: "flex:1;position:relative;height:14px;background:var(--bg2,rgba(128,128,128,.15));border-radius:3px" }, [
      el("div", {
        style: `position:absolute;top:0;left:0;height:100%;border-radius:3px;background:var(--blue,#3b82f6);width:${oPct != null ? Math.min(oPct * 3, 100) : 0}%`,
      }, []),
      el("div", {
        style: `position:absolute;top:0;left:${Math.min(ePct * 3, 100)}%;height:100%;width:2px;background:var(--red)`,
        title: `Expected: ${ePct.toFixed(1)}%`,
      }, []),
    ]),
    el("div", { class: "mono", style: "width:56px;font-size:12px;text-align:right" }, [oPct != null ? oPct.toFixed(1) + "%" : "—"]),
  ]);
}

function renderSeriesPanel(name, detail, expected) {
  const chi2 = detail[`benford_${name}_chi2`];
  const pVal = detail[`benford_${name}_p_value`];
  const mad = detail[`benford_${name}_mad`];
  const nObs = detail[`benford_${name}_n_obs`];
  const dist = detail[`benford_${name}_digit_distribution`];

  const header = el("div", { style: "font-weight:600;margin-bottom:6px" }, [
    name.replace(/_/g, " "),
    el("span", { style: "font-weight:400;color:var(--tx2);font-size:12px;margin-left:8px" }, [
      `n=${nObs ?? "—"} · χ²=${chi2 != null ? fmtNum(chi2, 2) : "—"} · p=${pVal != null ? fmtNum(pVal, 4) : "—"} · MAD=${mad != null ? fmtNum(mad, 4) : "—"}`,
    ]),
  ]);

  const bars = el("div", {}, []);
  if (dist && dist.length === 9) {
    for (let d = 1; d <= 9; d++) {
      bars.appendChild(digitBar(dist[d - 1], expected[d - 1], d));
    }
  } else {
    bars.appendChild(el("div", { class: "empty" }, ["Fewer than 5 real observations for this series — no distribution computed"]));
  }

  return el("div", { class: "card", style: "margin-bottom:12px" }, [header, bars]);
}

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
        el("div", { class: "sec-title", style: "font-size:14px;margin-bottom:12px" }, ["Overall Mean Absolute Deviation (MAD)"]),
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

      let detail = null;
      if (row.benford_detail_json) {
        try {
          detail = JSON.parse(row.benford_detail_json);
        } catch (e) {
          detail = null;
        }
      }

      if (!detail) {
        renderEmptyState("digit-distribution", {
          icon: "📊",
          detail: "No per-digit Benford distribution recorded for this ticker/date yet — re-run the forensic scorer (score_forensic.py) to populate benford_detail_json.",
        });
        return;
      }

      const expected = detail.benford_expected_distribution || [0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046];
      const seriesNames = Object.keys(detail)
        .filter((k) => k.startsWith("benford_") && k.endsWith("_chi2"))
        .map((k) => k.slice("benford_".length, -"_chi2".length));

      if (!seriesNames.length) {
        digitWrap.innerHTML = `<div class="empty">No per-series Benford data in benford_detail_json</div>`;
        return;
      }

      digitWrap.appendChild(el("div", { style: "font-size:12px;color:var(--tx2);margin:12px 0 8px" }, [
        "Per-digit (1-9) first-digit observed frequency vs Benford's expected frequency (red marker), per financial series:",
      ]));
      seriesNames.forEach((name) => {
        digitWrap.appendChild(renderSeriesPanel(name, detail, expected));
      });
    })
    .catch((e) => showError("benford-content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
