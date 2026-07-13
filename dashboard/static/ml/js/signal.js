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
      // FutureDevelopment.md #14: forensic now scores weekly (Sunday,
      // schedule_forensic_scoring) — same "as of {date}" staleness pattern
      // as hub.js's regime strip/top_buys carry-forward note, since this
      // badge can legitimately be several days stale between weekly runs.
      if (forensicRow.date) {
        links.appendChild(el("span", { style: "font-size:12px;color:var(--tx3)" }, [`as of ${String(forensicRow.date).slice(0, 10)}`]));
      }
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
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Volume"]), el("div", { class: "stat-value" }, [Number(row.volume).toLocaleString("en-IN")])])]),
        ])
      );
    })
    .catch((e) => showError("price-card", e));
}

const MODEL_LEGEND = {
  signal_5d: "signal_5d — 5-day-forward buy/hold/sell classifier + 3 quantile regressors (q10/q50/q90 forward-return band). The only model the paper-trading bot actually trades.",
  meta_labeler: "meta_labeler — secondary filter on top of signal_5d: estimates the probability signal_5d's call is worth acting on (meta_label_prob), to suppress low-conviction buy calls.",
  pnd_detector: "pnd_detector — pump-and-dump risk score (0-100) from volume/price anomaly features; pnd_block=true removes a ticker from all buy lists regardless of signal_5d's score.",
};

function renderModelLegend(rows) {
  const names = [...new Set(rows.map((r) => r.model_name))];
  const known = names.filter((n) => MODEL_LEGEND[n]);
  if (!known.length) return null;
  const box = el("div", { class: "card", style: "margin-bottom:10px;font-size:12px;color:var(--tx2)" }, []);
  known.forEach((n) => box.appendChild(el("div", { style: "margin-bottom:4px" }, [MODEL_LEGEND[n]])));
  return box;
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
          el("th", {}, ["Model"]),
          el("th", {}, ["Direction"]),
          // ML24 (2026-07-13): Buy Prob (classifier head) and Q50 Return
          // (quantile-regressor head) are scored independently and can
          // legitimately disagree even for the same model_name/horizon —
          // tooltips make this explicit rather than implying one unified
          // confidence number.
          el("th", { title: "Buy/hold/sell classifier's own probability — independent of Q50 Return" }, ["Buy Prob*"]),
          el("th", {}, ["Meta"]), el("th", {}, ["P&D"]), el("th", {}, ["Exit Urgency"]),
          el("th", { title: "Median (q50) of the quantile-regressor's forward-return distribution — independent of Buy Prob" }, ["Q50 Return*"]),
          el("th", {}, ["Interval"]),
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
      c.appendChild(el("div", { style: "font-size:12px;color:var(--tx3);margin-bottom:6px" }, [
        "*Buy Prob and Q50 Return are separate model heads (classifier vs. quantile regressor), scored independently — they can disagree (e.g. a high buy probability alongside a negative Q50 Return) and are not one unified confidence score.",
      ]));
      const legend = renderModelLegend(rows);
      if (legend) c.appendChild(legend);
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

// ===== #17 — 5-day recommendation history + Sell rationale =====
const EXIT_TYPE_TEXT = {
  thesis_broken: "Thesis broken — stop-loss hit; the original entry thesis no longer holds.",
  momentum_exhaustion: "Momentum exhaustion — the move has stalled well before target or stop.",
  risk_management: "Risk management — position sized/trimmed to control portfolio risk.",
  target_achieved: "Target achieved — the position hit its profit target.",
  opportunity_cost: "Opportunity cost — max hold period reached without target or stop; capital reallocated.",
  pnd_exit: "Pump-and-dump exit — a P&D risk pattern was detected after entry; exiting defensively.",
};

function renderSellRationale(container, latestRow) {
  if (!latestRow || latestRow.exit_urgency === null || latestRow.exit_urgency === undefined) {
    container.appendChild(el("div", { class: "empty" }, ["No exit signal on the latest call — nothing to act on"]));
    return;
  }
  const urgent = latestRow.exit_urgency >= 50;
  const exitType = latestRow.exit_type;
  const box = el("div", { class: "card", style: `border-left:4px solid ${urgent ? "var(--red)" : "var(--amber)"}` }, [
    el("div", { style: "font-weight:700;margin-bottom:6px" }, [
      urgent ? "Sell Recommendation" : "Watch — Not Yet a Sell",
      el("span", { class: "badge " + (urgent ? "b-red" : "b-amber"), style: "margin-left:8px" }, [`urgency ${fmtNum(latestRow.exit_urgency, 0)}`]),
    ]),
    el("div", { style: "font-size:13px;color:var(--tx2)" }, [
      exitType ? (EXIT_TYPE_TEXT[exitType] || exitType) : "No exit_type recorded on the latest call.",
    ]),
  ]);
  container.appendChild(box);
}

// ML26 — collapse a Buy signal that persists across N consecutive days
// into one paired Buy-date/Sell-date/Buy-price/Sell-price/CMP/rationale
// row, instead of showing every individual daily call. `rows` must be
// sorted ASCENDING by date (oldest first) for the persistence-walk below
// to work; the caller (loadHistory) fetches them descending from the API
// so callers of this function should reverse first.
//
// Edge cases (deliberately conservative, no guessed pairing):
//   - An unmatched Buy (no Sell call yet in the fetched window) reports
//     sellDate=null/sellPrice=null and the caller shows CMP instead.
//   - A Buy -> Sell -> Buy sequence (re-entry) produces two separate
//     paired rows, never merged into one.
//   - Consecutive Sell calls after a Buy only close the position once
//     (the first Sell ends it); further Sells with no open Buy are
//     ignored (nothing to pair them with) rather than fabricating one.
//   - "hold" calls neither open nor close a position — they're ignored
//     for pairing purposes (the position, if any, just continues).
function pairBuySellHistory(rowsAscending) {
  const pairs = [];
  let open = null; // { buyDate, buyRow }
  for (const r of rowsAscending) {
    const dir = r.signal_direction;
    if (dir === "buy") {
      if (!open) {
        open = { buyDate: r.date.slice(0, 10), buyRow: r };
      }
      // else: Buy persists across another day — same open position, no
      // new row (this IS the "collapsing an N-day-persisted Buy" case).
    } else if (dir === "sell") {
      if (open) {
        pairs.push({
          buyDate: open.buyDate,
          buyRow: open.buyRow,
          sellDate: r.date.slice(0, 10),
          sellRow: r,
        });
        open = null;
      }
      // else: a Sell with nothing open — no matching Buy in this window,
      // nothing to pair.
    }
    // "hold" (or any other value): no state change.
  }
  if (open) {
    // Unmatched Buy — still open as of the latest fetched call.
    pairs.push({ buyDate: open.buyDate, buyRow: open.buyRow, sellDate: null, sellRow: null });
  }
  return pairs;
}

function renderPairedHistory(rows, closeByDate, cmp, ticker) {
  const c = document.getElementById("paired-history-table");
  const ascending = rows.slice().reverse(); // API returns newest-first
  const pairs = pairBuySellHistory(ascending);
  if (!pairs.length) {
    c.innerHTML = `<div class="empty">No buy/sell-paired calls for ${ticker} in the last 10 signal_5d calls</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Buy Date"]), el("th", {}, ["Buy Price"]),
      el("th", {}, ["Sell Date"]), el("th", {}, ["Sell Price"]),
      el("th", {}, ["CMP"]), el("th", {}, ["Return"]), el("th", {}, ["Rationale"]),
    ])]),
    el("tbody", {}, pairs.slice().reverse().map((p) => {
      const buyPrice = closeByDate[p.buyDate] ?? null;
      const sellPrice = p.sellDate ? (closeByDate[p.sellDate] ?? null) : null;
      const isOpen = p.sellDate === null;
      const refPrice = isOpen ? cmp : sellPrice;
      const ret = (buyPrice && refPrice) ? (refPrice / buyPrice - 1) : null;
      const rationale = p.sellRow && p.sellRow.exit_type
        ? (EXIT_TYPE_TEXT[p.sellRow.exit_type] || p.sellRow.exit_type)
        : (isOpen ? "Position still open — no Sell call yet" : "—");
      return el("tr", {}, [
        el("td", { class: "mono" }, [p.buyDate]),
        el("td", { class: "mono" }, [buyPrice != null ? fmtMoney(buyPrice) : "—"]),
        el("td", { class: "mono" }, [p.sellDate || "—"]),
        el("td", { class: "mono" }, [isOpen ? "—" : (sellPrice != null ? fmtMoney(sellPrice) : "—")]),
        el("td", { class: "mono" }, [isOpen && cmp != null ? fmtMoney(cmp) : "—"]),
        el("td", { class: "mono " + pnlClass(ret) }, [ret != null ? fmtPct(ret) : "—"]),
        el("td", { style: "font-size:12px;color:var(--tx2)" }, [rationale]),
      ]);
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadHistory() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("history-table");
  Promise.all([
    apiGet(`/api/v1/signals/ml/history/${ticker}`, { model_name: "signal_5d", n: 10 }),
    apiGet(`/api/v1/ohlcv/${ticker}/latest`).catch(() => null),
  ]).then(([rows, latestOhlcv]) => {
    const c = document.getElementById("history-table");
    if (!rows.length) {
      c.innerHTML = `<div class="empty">No signal_5d history for ${ticker}</div>`;
      return;
    }
    const cmp = latestOhlcv ? latestOhlcv.close : null;
    c.innerHTML = "";

    apiGet(`/api/v1/ohlcv/${ticker}`, {
      from_date: rows[rows.length - 1].date.slice(0, 10),
      to_date: rows[0].date.slice(0, 10),
    })
      .catch(() => ({ data: [] }))
      .then((ohlcvResp) => {
        const closeByDate = {};
        const sortedOhlcv = (ohlcvResp.data || [])
          .slice()
          .sort((a, b) => String(a.date).localeCompare(String(b.date)));
        sortedOhlcv.forEach((r) => {
          closeByDate[String(r.date).slice(0, 10)] = r.close;
        });

        // A67 — since-recommendation price trend sparkline, reusing the
        // same closes already fetched for the recommended-price lookup;
        // no extra API call needed.
        function trendSparkline(recDate) {
          const series = sortedOhlcv
            .filter((r) => String(r.date).slice(0, 10) >= recDate)
            .map((r) => r.close);
          return sparklineSvg(series, { strokeAuto: true });
        }

        const table = el("table", {}, [
          el("thead", {}, [el("tr", {}, [
            el("th", {}, ["Recommended Date"]), el("th", {}, ["Recommended Price"]),
            el("th", {}, ["Expected Return (q50)"]), el("th", {}, ["CMP"]), el("th", {}, ["Current Return"]),
            el("th", {}, ["Direction"]), el("th", {}, ["Trend"]),
          ])]),
          el("tbody", {}, rows.map((r) => {
            const recDate = r.date.slice(0, 10);
            const recPrice = closeByDate[recDate] ?? null;
            const currentReturn = (recPrice && cmp) ? (cmp / recPrice - 1) : null;
            const trendTd = el("td", {}, []);
            trendTd.innerHTML = trendSparkline(recDate);
            return el("tr", {}, [
              el("td", { class: "mono" }, [recDate]),
              el("td", { class: "mono" }, [recPrice != null ? fmtMoney(recPrice) : "—"]),
              el("td", { class: "mono " + pnlClass(r.q50_return) }, [fmtPct(r.q50_return)]),
              el("td", { class: "mono" }, [cmp != null ? fmtMoney(cmp) : "—"]),
              el("td", { class: "mono " + pnlClass(currentReturn) }, [currentReturn != null ? fmtPct(currentReturn) : "—"]),
              el("td", {}, [el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : r.signal_direction === "buy" ? "b-green" : "b-blue") }, [r.signal_direction || "—"])]),
              trendTd,
            ]);
          })),
        ]);
        c.appendChild(el("div", { class: "card" }, [table]));
        renderSellRationale(c, rows[0]);
        renderPairedHistory(rows, closeByDate, cmp, ticker);
      });
  }).catch((e) => {
    showError("history-table", e);
    showError("paired-history-table", e);
  });
}

document.getElementById("load-btn").addEventListener("click", () => {
  loadSignals();
  loadHistory();
});
loadRegimeHistory();
if (params.get("ticker")) {
  loadSignals();
  loadHistory();
}
