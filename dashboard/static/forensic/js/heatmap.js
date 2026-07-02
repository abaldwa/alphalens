// dashboard/static/forensic/js/heatmap.js — FOREN-E Peer Forensic Heatmap
//
// /flagged only returns composite + flag label, not the per-metric classical
// breakdown — so for each flagged ticker (capped at 15 to bound request
// fan-out) we fetch the full forensic row to populate real heatmap columns.
// Cell thresholds are the same documented constants from classical_scores.py
// used on the other forensic screens (Beneish -1.78, Altman 1.81/2.99,
// Piotroski <=2, Sloan accrual >0.10 "high accrual").
renderAppShell("forensic", "heatmap");

function heatCell(value, digits, isBad) {
  if (value === null || value === undefined) return el("td", { class: "heatmap-cell h-neutral" }, ["—"]);
  return el("td", { class: "heatmap-cell " + (isBad ? "h-red" : "h-green") }, [fmtNum(value, digits)]);
}

apiGet("/api/v1/signals/ml/forensic/flagged", { flag: "red,amber" })
  .then((r) => {
    const c = document.getElementById("heatmap-content");
    if (!r.rows.length) {
      c.innerHTML = `<div class="empty">No red/amber flagged tickers</div>`;
      return;
    }
    const tickers = r.rows.slice(0, 15);
    return Promise.all(tickers.map((row) => apiGet(`/api/v1/signals/ml/forensic/${row.ticker}`).catch(() => null)))
      .then((details) => {
        c.innerHTML = "";
        const table = el("table", {}, [
          el("thead", {}, [el("tr", {}, [
            el("th", {}, ["Company"]), el("th", {}, ["Score"]), el("th", {}, ["Beneish"]),
            el("th", {}, ["Altman Z"]), el("th", {}, ["Piotroski F"]), el("th", {}, ["Sloan Accrual"]), el("th", {}, ["Benford MAD"]),
          ])]),
          el("tbody", {}, tickers.map((t, i) => {
            const d = details[i];
            return el("tr", {}, [
              el("td", { style: "font-weight:600" }, [t.ticker]),
              el("td", {}, [el("span", { class: "badge " + badgeClass(t.forensic_flag_label === "green" ? "green" : (["red", "black"].includes(t.forensic_flag_label) ? "red" : "amber")) }, [fmtNum(t.forensic_composite, 0)])]),
              d ? heatCell(d.beneish_m, 2, d.beneish_m !== null && d.beneish_m !== undefined && d.beneish_m > -1.78) : el("td", { class: "heatmap-cell h-neutral" }, ["—"]),
              d ? heatCell(d.altman_z, 2, d.altman_z !== null && d.altman_z !== undefined && d.altman_z < 1.81) : el("td", { class: "heatmap-cell h-neutral" }, ["—"]),
              d ? heatCell(d.piotroski_f, 0, d.piotroski_f !== null && d.piotroski_f !== undefined && d.piotroski_f <= 2) : el("td", { class: "heatmap-cell h-neutral" }, ["—"]),
              d ? heatCell(d.sloan_accrual, 3, d.sloan_accrual !== null && d.sloan_accrual !== undefined && d.sloan_accrual > 0.10) : el("td", { class: "heatmap-cell h-neutral" }, ["—"]),
              d ? heatCell(d.benford_mad, 4, d.benford_mad !== null && d.benford_mad !== undefined && d.benford_mad > 0.030) : el("td", { class: "heatmap-cell h-neutral" }, ["—"]),
            ]);
          })),
        ]);
        c.appendChild(el("div", { class: "card" }, [table]));
      });
  })
  .catch((e) => showError("heatmap-content", e));
