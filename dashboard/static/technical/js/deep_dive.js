// dashboard/static/technical/js/deep_dive.js — T6: Technical Deep Dive,
// mirroring ml/signal.js's Signal Deep Dive pattern but for pure TA state:
// 5/21/63-ish DMA ratios (real feature set exposes sma_20/50/100/200_ratio,
// not literal 5/21/63d SMAs — features/technical.py Category 2), 52wk
// hi/lo distance, support/resistance (reused from the Daily WatchList's
// existing /api/v1/ta/watchlist/daily rolling-swing-high/floor-pivot
// computation), and delivery volume/% (Category 9). Opened via a per-row
// icon from watchlist.html in a new tab.
renderAppShell("technical", "deep_dive");
TickerPicker.attach("ticker-input");
CalendarPicker.attach("date-input");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

const params = new URLSearchParams(window.location.search);
document.getElementById("date-input").value = todayStr();
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function statCard(label, value, cls) {
  return el("div", { class: "card" }, [
    el("div", { class: "stat" }, [
      el("div", { class: "stat-label" }, [label]),
      el("div", { class: "stat-value " + (cls || "") }, [value]),
    ]),
  ]);
}

function loadHeader(ticker) {
  const c = document.getElementById("deep-dive-header");
  c.innerHTML = "";
  const links = el("div", { style: "display:flex;align-items:center;gap:12px;flex-wrap:wrap" }, [
    el("span", { style: "font-size:20px;font-weight:700" }, [ticker]),
  ]);
  c.appendChild(links);
  c.appendChild(buildCrossLinks(ticker, "technical"));
}

function loadIndicators(ticker, date) {
  showLoading("dma-card");
  showLoading("range-card");
  showLoading("delivery-card");
  apiGet(`/api/v1/ta/${ticker}/indicators`, { date })
    .then((r) => {
      const dmaC = document.getElementById("dma-card");
      const rangeC = document.getElementById("range-card");
      const deliveryC = document.getElementById("delivery-card");
      if (!r.available) {
        dmaC.innerHTML = `<div class="empty">No indicator data for ${ticker} on ${date}</div>`;
        rangeC.innerHTML = `<div class="empty">—</div>`;
        deliveryC.innerHTML = `<div class="empty">—</div>`;
        return;
      }
      const ind = r.indicators || {};
      dmaC.innerHTML = "";
      dmaC.appendChild(
        el("div", { class: "card-grid grid grid-4" }, [
          statCard("SMA 20", fmtNum(ind.sma_20_ratio, 3)),
          statCard("SMA 50", fmtNum(ind.sma_50_ratio, 3)),
          statCard("SMA 100", fmtNum(ind.sma_100_ratio, 3)),
          statCard("SMA 200", fmtNum(ind.sma_200_ratio, 3)),
        ])
      );

      rangeC.innerHTML = "";
      rangeC.appendChild(
        el("div", { class: "card-grid grid grid-2" }, [
          statCard("Dist. from 52wk High", fmtPct(ind.dist_from_52w_high), pnlClass(ind.dist_from_52w_high)),
          statCard("Dist. from 52wk Low", fmtPct(ind.dist_from_52w_low), pnlClass(ind.dist_from_52w_low)),
        ])
      );

      deliveryC.innerHTML = "";
      deliveryC.appendChild(
        el("div", { class: "card-grid grid grid-3" }, [
          statCard("Delivery %", fmtPct(ind.delivery_pct)),
          statCard("Delivery Z-score (21d)", fmtNum(ind.delivery_pct_zscore_21d, 2)),
          statCard("Delivery/Price Corr (21d)", fmtNum(ind.delivery_price_corr_21d, 2)),
        ])
      );
    })
    .catch((e) => {
      showError("dma-card", e);
      showError("range-card", e);
      showError("delivery-card", e);
    });
}

function loadPatterns(ticker, date) {
  showLoading("patterns-card");
  apiGet(`/api/v1/ta/${ticker}/patterns`, { date })
    .then((r) => {
      const c = document.getElementById("patterns-card");
      if (!r.available || !Object.keys(r.patterns || {}).length) {
        c.innerHTML = `<div class="empty">No chart-pattern scores for ${ticker} on ${date}</div>`;
        return;
      }
      c.innerHTML = "";
      const box = el("div", { class: "card" }, []);
      Object.entries(r.patterns).forEach(([name, val]) => {
        box.appendChild(
          el("div", { class: "shap-bar" }, [
            el("span", { class: "shap-label" }, [name]),
            el("div", { class: "shap-track" }, [
              el("div", { class: "shap-fill up", style: `width:${Math.min(Math.abs(val || 0) * 100, 100)}%` }, []),
            ]),
            el("span", { class: "mono" }, [fmtNum(val, 3)]),
          ])
        );
      });
      c.appendChild(box);
    })
    .catch((e) => showError("patterns-card", e));
}

// Support/resistance reuses the Daily WatchList's already-computed swing-
// high/floor-pivot levels (SPEC-TA-004) rather than recomputing them —
// over-fetches the watchlist and finds this ticker client-side since that
// endpoint has no per-ticker filter today.
function loadLevels(ticker) {
  showLoading("levels-card");
  apiGet("/api/v1/ta/watchlist/daily", { limit: 100 })
    .then((r) => {
      const c = document.getElementById("levels-card");
      const row = (r.rows || []).find((x) => x.ticker === ticker);
      if (!row) {
        c.innerHTML = `<div class="empty">${ticker} not in today's WatchList template matches — no computed levels available</div>`;
        return;
      }
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "card-grid grid grid-2" }, [
          el("div", { class: "card" }, [
            el("div", { class: "stat" }, [
              el("div", { class: "stat-label" }, ["Next Resistance"]),
              el("div", { class: "stat-value dn" }, [row.resistance_levels.length ? row.resistance_levels.map(fmtMoney).join(" / ") : "—"]),
            ]),
          ]),
          el("div", { class: "card" }, [
            el("div", { class: "stat" }, [
              el("div", { class: "stat-label" }, ["Support"]),
              el("div", { class: "stat-value up" }, [row.support_levels.length ? row.support_levels.map(fmtMoney).join(" / ") : "—"]),
            ]),
          ]),
        ])
      );
    })
    .catch((e) => showError("levels-card", e));
}

function loadAll() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  const date = document.getElementById("date-input").value.trim();
  if (!ticker || !date) return;
  loadHeader(ticker);
  loadIndicators(ticker, date);
  loadPatterns(ticker, date);
  loadLevels(ticker);
}

document.getElementById("load-btn").addEventListener("click", loadAll);
if (params.get("ticker")) loadAll();
