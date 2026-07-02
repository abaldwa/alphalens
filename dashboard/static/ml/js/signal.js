// dashboard/static/ml/js/signal.js — ML-B Signal Deep Dive
renderAppShell("ml", "signal");
TickerPicker.attach("ticker-input");
CalendarPicker.attach("date-input");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

const params = new URLSearchParams(window.location.search);
document.getElementById("date-input").value = todayStr();
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function loadHeader(ticker) {
  const c = document.getElementById("signal-header");
  Promise.all([
    apiGet(`/api/v1/signals/ml/forensic/${ticker}`).catch(() => null),
    apiGet(`/api/v1/signals/ml/multibagger/${ticker}`).catch(() => null),
  ]).then(([forensicRow, mbRow]) => {
    c.innerHTML = "";
    const links = el("div", { style: "display:flex;align-items:center;gap:12px;flex-wrap:wrap" }, [
      el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
    ]);
    if (forensicRow) {
      links.appendChild(el("span", { class: "badge " + badgeClass(forensicRow.forensic_flag_label === "green" ? "green" : (["red", "black"].includes(forensicRow.forensic_flag_label) ? "red" : "amber")) }, [
        `Forensic: ${forensicRow.forensic_flag_label || "—"} (${fmtNum(forensicRow.forensic_composite, 0)})`,
      ]));
    }
    if (mbRow && mbRow.mb_probability !== null && mbRow.mb_probability !== undefined) {
      links.appendChild(el("span", { class: "badge b-purple" }, [`Multibagger: ${fmtPct(mbRow.mb_probability)}`]));
    }
    c.appendChild(links);
    c.appendChild(buildCrossLinks(ticker, "ml"));
  });
}

function loadPrice(ticker) {
  apiGet(`/api/v1/ohlcv/${ticker}/latest`)
    .then((row) => {
      const c = document.getElementById("price-card");
      if (!row) {
        c.innerHTML = `<div class="empty">No OHLCV data for ${ticker}</div>`;
        return;
      }
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "card-grid grid grid-4" }, [
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Close"]), el("div", { class: "stat-value" }, [fmtMoney(row.close)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["High"]), el("div", { class: "stat-value" }, [fmtMoney(row.high)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Low"]), el("div", { class: "stat-value" }, [fmtMoney(row.low)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Volume"]), el("div", { class: "stat-value" }, [String(row.volume)])])]),
        ])
      );
    })
    .catch((e) => showError("price-card", e));
}

function loadSignals() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  const date = document.getElementById("date-input").value.trim();
  if (!ticker || !date) return;

  loadHeader(ticker);
  loadPrice(ticker);
  showLoading("model-scores");
  showLoading("shap");

  apiGet(`/api/v1/signals/ml/${ticker}/${date}`, { carry_forward: true })
    .then((rows) => {
      const c = document.getElementById("model-scores");
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No signal has ever been generated for ${ticker} on or before ${date}</div>`;
        document.getElementById("shap").innerHTML = `<div class="empty">—</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Model"]), el("th", {}, ["Direction"]), el("th", {}, ["Buy Prob"]),
          el("th", {}, ["Meta"]), el("th", {}, ["P&D"]), el("th", {}, ["Exit Urgency"]),
          el("th", {}, ["Q50 Return"]), el("th", {}, ["Interval"]),
        ])]),
        el("tbody", {}, rows.map((r) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [r.model_name]),
          el("td", {}, [el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : r.signal_direction === "buy" ? "b-green" : "b-blue") }, [r.signal_direction || "—"])]),
          el("td", { class: "mono" }, [fmtPct(r.buy_prob)]),
          el("td", { class: "mono" }, [r.meta_label || "—"]),
          el("td", { class: "mono" }, [fmtNum(r.pnd_score, 0)]),
          el("td", { class: "mono" }, [r.exit_urgency !== null && r.exit_urgency !== undefined ? fmtNum(r.exit_urgency, 0) : "—"]),
          el("td", { class: "mono " + pnlClass(r.q50_return) }, [fmtPct(r.q50_return)]),
          el("td", { class: "mono" }, [`${fmtPct(r.conformal_lower)} to ${fmtPct(r.conformal_upper)}`]),
        ]))),
      ]);
      c.innerHTML = "";
      const signalDate = rows[0].date ? rows[0].date.slice(0, 10) : null;
      if (signalDate && signalDate !== date) {
        c.appendChild(el("div", { class: "empty", style: "margin-bottom:8px" }, [
          `Showing the last generated signal, from ${signalDate} — none for ${date} yet`,
        ]));
      }
      c.appendChild(el("div", { class: "card" }, [table]));

      const sigRow = rows.find((r) => r.shap_top5_json);
      const shapC = document.getElementById("shap");
      if (sigRow) {
        try {
          const shap = JSON.parse(sigRow.shap_top5_json);
          const entries = Array.isArray(shap) ? shap : Object.entries(shap).map(([k, v]) => ({ feature: k, value: v }));
          shapC.innerHTML = "";
          const shapCard = el("div", { class: "card" }, []);
          entries.forEach((e2) => {
            const val = e2.value ?? e2[1] ?? 0;
            const label = e2.feature || e2[0] || "—";
            shapCard.appendChild(
              el("div", { class: "shap-bar" }, [
                el("span", { class: "shap-label" }, [label]),
                el("div", { class: "shap-track" }, [
                  el("div", { class: "shap-fill " + (val >= 0 ? "shap-pos" : "shap-neg"), style: `width:${Math.min(Math.abs(val) * 100, 50)}%` }, []),
                ]),
                el("span", { class: "mono " + (val >= 0 ? "up" : "dn") }, [fmtNum(val, 4)]),
              ])
            );
          });
          shapC.appendChild(shapCard);
        } catch (e) {
          shapC.innerHTML = `<div class="empty">SHAP data not parseable</div>`;
        }
      } else {
        shapC.innerHTML = `<div class="empty">No SHAP data for ${ticker} on ${date}</div>`;
      }
    })
    .catch((e) => {
      showError("model-scores", e);
      showError("shap", e);
    });
}

function drawRegimeChart(days) {
  const canvas = document.getElementById("regime-chart");
  const ctx = canvas.getContext("2d");
  canvas.width = canvas.clientWidth;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!days.length) {
    ctx.fillStyle = "#8E95A8";
    ctx.fillText("No regime history available", 10, 20);
    return;
  }
  const vals = days.map((d) => d.hmm_regime_prob ?? 0);
  const max = Math.max(...vals, 0.01);
  const w = canvas.width / days.length;
  ctx.strokeStyle = "#0A9B8E";
  ctx.lineWidth = 2;
  ctx.beginPath();
  days.forEach((d, i) => {
    const x = i * w + w / 2;
    const y = canvas.height - (d.hmm_regime_prob ?? 0) / max * (canvas.height - 20) - 10;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function loadRegimeHistory() {
  apiGet("/api/v1/macro/regime/history", { days: 30 })
    .then((r) => drawRegimeChart(r.days))
    .catch(() => drawRegimeChart([]));
}

document.getElementById("load-btn").addEventListener("click", loadSignals);
loadRegimeHistory();
if (params.get("ticker")) loadSignals();
