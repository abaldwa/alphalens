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

// The Bhav Copy (and everything derived from it — indicators, patterns,
// levels) is only written once the 18:00 IST daily pipeline run completes
// (config/settings.py DAILY_PIPELINE_SCHEDULE_TIME), so "today" is not a
// safe default: on a holiday/weekend, or before that run finishes, no data
// exists yet for the naive calendar date. ops/freshness reports
// MAX(date) actually present in ohlcv_adjusted, which is exactly "the last
// trading date we have Bhav Copy data for" — including the 6pm cutoff and
// holiday calendar, with no client-side date math needed.
function loadDefaultDate() {
  return apiGet("/api/v1/ops/freshness")
    .then((r) => {
      const row = (r.sources || []).find((x) => x.source === "ohlcv_adjusted");
      return (row && row.latest_data_date) || new Date().toISOString().slice(0, 10);
    })
    .catch(() => new Date().toISOString().slice(0, 10));
}

const params = new URLSearchParams(window.location.search);
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
    el("a", {
      href: `/ui/technical/chart.html?ticker=${ticker}`, target: "_blank", rel: "noopener",
      style: "font-size:20px;font-weight:700;text-decoration:none;color:var(--tx)",
      title: "Open chart",
    }, [ticker]),
  ]);
  c.appendChild(links);
  const reason = params.get("reason");
  if (reason) {
    c.appendChild(
      el("div", { class: "card", style: "margin-top:10px;font-size:13px;color:var(--tx2)" }, [
        el("span", { style: "font-weight:600;color:var(--tx)" }, ["Why on WatchList: "]),
        reason,
      ])
    );
  }
  c.appendChild(buildCrossLinks(ticker, "technical"));
}

let lastSummary = null;

// Row 1/2/3 come from GET /api/v1/ta/{ticker}/summary, which computes raw
// display-scale values (CMP, 52wk hi/lo, raw SMA/EMA/MACD/VWAP) directly
// from OHLCV — features/technical.py only stores ratios for most of these,
// not the raw price-scale figures the spec calls for.
function loadSummary(ticker, date) {
  showLoading("row1-card");
  showLoading("row2-card");
  showLoading("row3-card");
  return apiGet(`/api/v1/ta/${ticker}/summary`, { date })
    .then((r) => {
      lastSummary = r;
      const row1C = document.getElementById("row1-card");
      const row2C = document.getElementById("row2-card");
      const row3C = document.getElementById("row3-card");
      if (!r.available) {
        row1C.innerHTML = `<div class="empty">No price data for ${ticker} on ${date}</div>`;
        row2C.innerHTML = `<div class="empty">—</div>`;
        row3C.innerHTML = `<div class="empty">—</div>`;
        return r;
      }

      row1C.innerHTML = "";
      row1C.appendChild(
        el("div", { class: "card-grid grid grid-6" }, [
          statCard("CMP", fmtMoney(r.cmp)),
          statCard("52 Wk High", fmtMoney(r.week52_high)),
          statCard("52 Wk Low", fmtMoney(r.week52_low)),
          statCard("Delivery %", r.delivery_pct == null ? "—" : fmtNum(r.delivery_pct, 1) + "%"),
          statCard("Average Delivery", r.avg_delivery_pct_21d == null ? "—" : fmtNum(r.avg_delivery_pct_21d, 1) + "%"),
          statCard("Delivery Z-Score", fmtNum(r.delivery_pct_zscore_21d, 2)),
        ])
      );

      row2C.innerHTML = "";
      row2C.appendChild(
        el("div", { class: "card-grid grid grid-6" }, [
          statCard("SMA 20", fmtMoney(r.sma_20)),
          statCard("SMA 50", fmtMoney(r.sma_50)),
          statCard("SMA 100", fmtMoney(r.sma_100)),
          statCard("SMA 200", fmtMoney(r.sma_200)),
          statCard("9 EMA", fmtMoney(r.ema_9)),
          statCard("21 EMA", fmtMoney(r.ema_21)),
          statCard("RSI", fmtNum(r.rsi_14, 1)),
          statCard(
            "SuperTrend",
            r.supertrend_value == null ? "—" : `${fmtMoney(r.supertrend_value)} (${r.supertrend_dir > 0 ? "Up" : "Down"})`,
            r.supertrend_value == null ? "" : r.supertrend_dir > 0 ? "pos" : "neg"
          ),
          statCard("MACD", r.macd == null ? "—" : `${fmtNum(r.macd, 2)} / ${fmtNum(r.macd_signal, 2)}`),
        ])
      );

      row3C.innerHTML = "";
      row3C.appendChild(
        el("div", { class: "card-grid grid grid-4" }, [
          statCard("SMA Ratio (50/200)", fmtNum(r.sma_50_200_ratio, 3)),
          statCard("Dist. from 52 Wk High", fmtPct(r.dist_from_52w_high), pnlClass(r.dist_from_52w_high)),
          statCard("Dist. from 52 Wk Low", fmtPct(r.dist_from_52w_low), pnlClass(r.dist_from_52w_low)),
          statCard("VWAP (20d)", fmtMoney(r.vwap_20d)),
        ])
      );
      return r;
    })
    .catch((e) => {
      showError("row1-card", e);
      showError("row2-card", e);
      showError("row3-card", e);
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
        const pct = Math.min(Math.abs(val || 0) * 100, 100);
        box.appendChild(
          el("div", { class: "shap-bar" }, [
            el("span", { class: "shap-label" }, [name]),
            el("div", { class: "shap-track" }, [
              el("div", { class: "shap-fill " + ((val || 0) >= 0 ? "up" : "dn"), style: `width:${pct}%` }, []),
            ]),
            el("span", { class: "mono" }, [fmtNum(val, 3)]),
          ])
        );
      });
      c.appendChild(box);
    })
    .catch((e) => showError("patterns-card", e));
}

// Slider: a horizontal track spanning [min(support), max(resistance)] (with
// a small margin), with tick markers for each support/resistance level and
// a marker for CMP so it's visually obvious where price sits relative to
// its nearby floor/ceiling levels — rather than just two bare number lists.
function levelsSlider(cmp, support, resistance) {
  const supports = (support || []).slice().sort((a, b) => a - b);
  const resistances = (resistance || []).slice().sort((a, b) => a - b);
  const all = [...supports, ...resistances, cmp].filter((v) => v !== null && v !== undefined);
  if (!all.length) return el("div", { class: "empty" }, ["No support/resistance levels available"]);
  const lo = Math.min(...all);
  const hi = Math.max(...all);
  const span = hi - lo || 1;
  const pad = span * 0.08;
  const rangeLo = lo - pad;
  const rangeHi = hi + pad;
  const rangeSpan = rangeHi - rangeLo;
  const pctOf = (v) => ((v - rangeLo) / rangeSpan) * 100;

  const track = el("div", {
    style: "position:relative;height:56px;margin:28px 12px 8px 12px;background:var(--bg3);border-radius:4px",
  }, []);

  supports.forEach((v) => {
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(v)}%;top:0;bottom:0;width:2px;background:var(--green)`,
      title: `Support ${fmtMoney(v)}`,
    }, []));
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(v)}%;bottom:100%;transform:translateX(-50%);font-size:10px;color:var(--green);white-space:nowrap`,
    }, [fmtMoney(v)]));
  });
  resistances.forEach((v) => {
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(v)}%;top:0;bottom:0;width:2px;background:var(--red)`,
      title: `Resistance ${fmtMoney(v)}`,
    }, []));
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(v)}%;bottom:100%;transform:translateX(-50%);font-size:10px;color:var(--red);white-space:nowrap`,
    }, [fmtMoney(v)]));
  });
  if (cmp !== null && cmp !== undefined) {
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(cmp)}%;top:-6px;bottom:-6px;width:3px;background:var(--blue);border-radius:2px`,
      title: `CMP ${fmtMoney(cmp)}`,
    }, []));
    track.appendChild(el("div", {
      style: `position:absolute;left:${pctOf(cmp)}%;top:100%;transform:translateX(-50%);font-size:11px;font-weight:700;color:var(--blue);white-space:nowrap;margin-top:4px`,
    }, [`CMP ${fmtMoney(cmp)}`]));
  }
  return el("div", { style: "padding-bottom:16px" }, [track]);
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
      const cmp = (lastSummary && lastSummary.cmp) != null ? lastSummary.cmp : row.current_price;
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [levelsSlider(cmp, row.support_levels, row.resistance_levels)]));
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
  loadPatterns(ticker, date);
  loadSummary(ticker, date).then(() => loadLevels(ticker));
}

document.getElementById("load-btn").addEventListener("click", loadAll);
loadDefaultDate().then((d) => {
  document.getElementById("date-input").value = d;
  if (params.get("ticker")) loadAll();
});
