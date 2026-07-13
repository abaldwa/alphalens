// dashboard/static/ml/js/hub.js — ML-A Daily Insight Hub
renderAppShell("ml", "hub");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function horizonLabel(modelName) {
  const m = /(\d+)d$/.exec(modelName || "");
  return m ? `${m[1]} days` : "—";
}

function loadRegime() {
  apiGet("/api/v1/macro/regime")
    .then((r) => {
      const c = document.getElementById("regime-strip");
      if (!r.available) {
        c.innerHTML = `<div class="empty">No regime data yet</div>`;
        return;
      }
      const cls = r.hmm_regime === "bull" ? "regime-bull" : r.hmm_regime === "bear" ? "regime-bear" : "regime-side";
      c.innerHTML = "";
      c.appendChild(
        el("div", { style: "display:flex;align-items:center;gap:16px" }, [
          el("span", { class: "regime-badge " + cls }, [`● ${r.hmm_regime || "—"}`]),
          el("span", { style: "font-size:12px;color:var(--tx3)" }, [
            `confidence ${fmtPct(r.hmm_regime_prob)} · stability ${fmtNum(r.hmm_stability)} · as of ${r.date ? r.date.slice(0, 10) : "—"}`,
          ]),
        ])
      );
    })
    .catch((e) => showError("regime-strip", e));
}

// Higher-signal alert types surface first — drift/exit are rare and need
// immediate attention; pnd_block/pnd_flag are common (often dozens/day)
// and shown as an expandable tail so they don't bury everything else.
const ALERT_TYPE_PRIORITY = { drift_halt: 0, exit_urgent: 1, drift_warning: 2, pnd_block: 3, pnd_flag: 4 };
const ALERTS_VISIBLE_CAP = 5;

function alertBanner(a) {
  const cls = a.severity === "high" ? "alert-critical" : a.severity === "medium" ? "alert-warning" : "alert-info";
  return el("div", { class: "alert-banner " + cls }, [`[${a.alert_type}] ${a.message}`]);
}

function loadAlerts() {
  apiGet("/api/v1/alerts/today")
    .then((r) => {
      const c = document.getElementById("alerts");
      c.innerHTML = "";
      if (!r.alerts.length) return;

      const sorted = [...r.alerts].sort(
        (a, b) => (ALERT_TYPE_PRIORITY[a.alert_type] ?? 9) - (ALERT_TYPE_PRIORITY[b.alert_type] ?? 9)
      );
      const visible = sorted.slice(0, ALERTS_VISIBLE_CAP);
      const rest = sorted.slice(ALERTS_VISIBLE_CAP);

      visible.forEach((a) => c.appendChild(alertBanner(a)));

      if (rest.length) {
        const details = el("details", { class: "alert-more" }, [
          el("summary", { style: "cursor:pointer;font-size:13px;color:var(--tx3);margin-bottom:8px" }, [
            `+${rest.length} more alert${rest.length === 1 ? "" : "s"}`,
          ]),
        ]);
        rest.forEach((a) => details.appendChild(alertBanner(a)));
        c.appendChild(details);
      }
    })
    .catch((e) => showError("alerts", e));
}

function insightCard(r, price) {
  return el("div", { class: "card", style: "margin-bottom:12px" }, [
    el("div", { style: "display:flex;align-items:center;gap:12px;margin-bottom:12px" }, [
      el("span", { style: "font-weight:700;font-size:16px" }, [el("a", { href: `signal.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : "b-green") }, [`${(r.signal_direction || "—").toUpperCase()} ${horizonLabel(r.model_name)}`]),
      r.meta_label ? el("span", { class: "badge b-teal" }, [`Meta: ${r.meta_label}`]) : null,
      buildCrossLinks(r.ticker, "ml"),
    ]),
    el("div", { class: "insight-card" }, [
      el("div", { class: "insight-metric i-entry" }, [
        el("div", { class: "insight-label" }, ["Entry Point"]),
        el("div", { class: "insight-val" }, [price !== null && price !== undefined ? fmtMoney(price) : "—"]),
        el("div", { class: "insight-sub" }, ["Latest close"]),
      ]),
      el("div", { class: "insight-metric i-returns" }, [
        el("div", { class: "insight-label" }, ["Expected Return"]),
        el("div", { class: "insight-val" }, [fmtPct(r.q50_return)]),
        el("div", { class: "insight-sub" }, [`Range: ${fmtPct(r.conformal_lower)} to ${fmtPct(r.conformal_upper)}`]),
      ]),
      el("div", { class: "insight-metric i-duration" }, [
        el("div", { class: "insight-label" }, ["Duration"]),
        el("div", { class: "insight-val" }, [horizonLabel(r.model_name)]),
        el("div", { class: "insight-sub" }, ["Signal horizon"]),
      ]),
      el("div", { class: "insight-metric i-confidence" }, [
        el("div", { class: "insight-label" }, ["Confidence"]),
        el("div", { class: "insight-val" }, [fmtPct(r.buy_prob)]),
        el("div", { class: "insight-sub" }, ["Buy probability"]),
      ]),
    ]),
  ]);
}

// GET /api/v1/signals/ml/top_buys/{date} caps n at 100 server-side
// (datastore/api/routers/signals.py Query(..., le=100)) — 100 is the
// highest this list can ever show without a backend change, not an
// arbitrary front-end choice.
const TOP_BUYS_MAX = 100;

const TOP_BUYS_COLUMNS = [
  { key: "ticker", label: "Stock" },
  { key: "signal_direction", label: "Signal" },
  { key: "buy_prob", label: "Prob" },
  { key: "meta_label", label: "Meta" },
  { key: "conformal_lower", label: "Interval" },
  { key: "pnd_score", label: "P&D" },
  { key: "hmm_regime", label: "Regime" },
];

let _topBuysRows = [];
let _topBuysSort = { key: "buy_prob", dir: -1 };

function renderTopBuysTable() {
  const c = document.getElementById("top-buys-table");
  if (!c) return;
  const { key, dir } = _topBuysSort;
  const sorted = [..._topBuysRows].sort((a, b) => {
    const av = a[key], bv = b[key];
    if (av === null || av === undefined) return 1;
    if (bv === null || bv === undefined) return -1;
    if (typeof av === "string") return dir * av.localeCompare(bv);
    return dir * (av - bv);
  });
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, TOP_BUYS_COLUMNS.map((col) => {
      const arrow = _topBuysSort.key === col.key ? (_topBuysSort.dir === 1 ? " ▲" : " ▼") : "";
      const th = el("th", { style: "cursor:pointer;user-select:none" }, [col.label + arrow]);
      th.addEventListener("click", () => {
        if (_topBuysSort.key === col.key) _topBuysSort.dir *= -1;
        else _topBuysSort = { key: col.key, dir: 1 };
        renderTopBuysTable();
      });
      return th;
    }))]),
    el("tbody", {}, sorted.map((r) => el("tr", {}, [
      tickerCell(r.ticker),
      el("td", {}, [el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : "b-green") }, [`${(r.signal_direction || "—").toUpperCase()} ${horizonLabel(r.model_name)}`])]),
      el("td", { class: "mono " + pnlClass(r.buy_prob - 0.5) }, [fmtPct(r.buy_prob)]),
      el("td", { class: "mono" }, [r.meta_label || "—"]),
      el("td", { class: "mono" }, [`${fmtPct(r.conformal_lower)} to ${fmtPct(r.conformal_upper)}`]),
      el("td", { class: "mono" }, [fmtNum(r.pnd_score, 0)]),
      el("td", {}, [r.hmm_regime ? el("span", { class: "badge b-gray" }, [r.hmm_regime]) : "—"]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadTopBuys() {
  const today = todayStr();
  apiGet(`/api/v1/signals/ml/top_buys/${today}`, { n: TOP_BUYS_MAX, carry_forward: true })
    .then((rows) => {
      const c = document.getElementById("top-buys");
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No buy signals generated yet</div>`;
        return;
      }
      c.innerHTML = "";
      const signalDate = rows[0].date ? rows[0].date.slice(0, 10) : null;
      if (signalDate && signalDate !== today) {
        c.appendChild(el("div", { class: "empty", style: "margin-bottom:8px" }, [
          `Showing the last generated signals, from ${signalDate} — today's run hasn't produced signals yet`,
        ]));
      }
      const top2 = rows.slice(0, 2);
      Promise.all(top2.map((r) => apiGet(`/api/v1/ohlcv/${r.ticker}/latest`).then((row) => row && row.close).catch(() => null)))
        .then((prices) => {
          top2.forEach((r, i) => c.appendChild(insightCard(r, prices[i])));
          // FutureDevelopment.md #15: the stopgap explanation banner that used to
          // live here is removed for Meta/P&D/Regime — /api/v1/signals/ml/
          // top_buys/{date} now fuses meta_labeler/pnd_detector/hmm_market rows
          // onto each signal_5d row at read time (datastore/api/routers/
          // signals.py), verified live against real DuckDB rows (2026-07-02:
          // non-null meta_label/pnd_score/pnd_phase/hmm_regime for every ticker
          // top_buys returned). Interval (conformal_lower/upper) is a genuinely
          // separate case, not a stopgap-banner removal: daily_inference.py's
          // conformal_signal5d scoring (FutureDevelopment.md #14) is new code
          // that has not yet run against a live trading day as of 2026-07-04 —
          // conformal_lower/upper are still null for every existing row, so
          // "—" here for now is an honest null, not a bug. It will populate
          // starting with the next scheduled daily_inference run.
          _topBuysRows = rows;
          _topBuysSort = { key: "buy_prob", dir: -1 };
          c.appendChild(el("div", { id: "top-buys-table" }, []));
          renderTopBuysTable();
        });
    })
    .catch((e) => showError("top-buys", e));
}

// ML22 (2026-07-13): full Daily WatchList tables, merged in from the
// former standalone watchlist.html/watchlist.js — these supersede the
// hub's old truncated "watchlist-mini" (MB top-3) and "horizon-mini"
// (21d/63d top-3) sections, which showed a strict subset of the same
// underlying /api/v1/watchlist/daily rows with fewer columns.
function basisLabel(basis) {
  if (basis === "quantile") return "model";
  if (basis === "atr") return "ATR-est.";
  return "—";
}

function renderHorizonTable(containerId, rows) {
  const c = document.getElementById(containerId);
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No buy signals for this horizon on the latest available date</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Stock"]), el("th", {}, ["Name"]), el("th", {}, ["Sector"]),
      // ML24 (2026-07-13): Buy Prob (the classifier's buy/hold/sell call)
      // and Target/Expected Return (the separate quantile-regressor's
      // median forward-return forecast) are two independent model heads
      // scored independently — they can legitimately disagree (e.g. a
      // high buy probability alongside a negative expected return) and
      // are not one unified confidence number. Column headers/tooltips
      // make this explicit rather than implying a single consistent score.
      el("th", { title: "Buy/hold/sell classifier's own probability — a separate model head from Target/Expected Return below" }, ["Buy Prob*"]),
      el("th", {}, ["Price"]),
      el("th", { title: "Median (q50) of the quantile-regressor's forward-return distribution — independent of Buy Prob*" }, ["Target*"]),
      el("th", { title: "Median (q50) of the quantile-regressor's forward-return distribution — independent of Buy Prob*" }, ["Expected Return*"]),
      el("th", {}, ["Range (low–high)"]), el("th", {}, ["Basis"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("td", { style: "font-size:12px;color:var(--tx2)" }, [r.company_name || "—"]),
      el("td", {}, [r.sector || "—"]),
      el("td", { class: "mono" }, [fmtPct(r.buy_prob)]),
      el("td", { class: "mono" }, [fmtMoney(r.current_price)]),
      el("td", { class: "mono" }, [fmtMoney(r.target_price)]),
      el("td", { class: "mono " + pnlClass(r.expected_return_pct) }, [r.expected_return_pct != null ? `${r.expected_return_pct > 0 ? "+" : ""}${r.expected_return_pct.toFixed(1)}%` : "—"]),
      el("td", { class: "mono", style: "font-size:12px;color:var(--tx2)" }, [
        (r.target_low != null && r.target_high != null) ? `${fmtMoney(r.target_low)} – ${fmtMoney(r.target_high)}` : "—",
      ]),
      el("td", {}, [el("span", { class: "badge b-gray" }, [basisLabel(r.target_basis)])]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function renderMultibaggerTable(containerId, rows) {
  const mbContainer = document.getElementById(containerId);
  if (!rows.length) {
    mbContainer.innerHTML = `<div class="empty">No multibagger scoring data yet</div>`;
    return;
  }
  const cols = ["ticker", "mb_probability", "mb_tier", "mb_archetype", "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m"];
  const labels = ["Ticker", "MB Prob", "Deterministic Probability Band", "Archetype", "6m", "12m", "18m", "24m", "36m"];
  const mbTable = el("table", {}, [
    el("thead", {}, [el("tr", {}, labels.map((l) => el("th", {}, [l])))]),
    el("tbody", {}, rows.map((t) => el("tr", {}, cols.map((cc) => {
      if (cc === "ticker") return el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${t.ticker}` }, [t.ticker])]);
      if (cc === "mb_probability") return el("td", { class: "mono" }, [fmtPct(t[cc])]);
      if (cc === "mb_tier") return el("td", {}, [el("span", { class: "badge b-purple" }, [mbTierLabel(t[cc])])]);
      if (cc.startsWith("survival")) return el("td", { class: "mono" }, [fmtPct(t[cc])]);
      return el("td", {}, [t[cc] || "—"]);
    })))),
  ]);
  mbContainer.innerHTML = "";
  mbContainer.appendChild(el("div", { class: "card" }, [mbTable]));
}

function loadWatchlistDaily() {
  apiGet("/api/v1/watchlist/daily", { n_per_horizon: 10 })
    .then((r) => {
      document.getElementById("watchlist-notes").textContent =
        r.date ? `Signals for ${r.date} — targets from the model's own quantile-regression forward-return distribution (or a volatility/ATR-scaled band when unavailable), never a fixed %. *Buy Prob and Target/Expected Return are independent model outputs (classifier vs. quantile regressor) and can disagree — see column tooltips.` : "No signal data available yet";

      const byHorizon = { "5d": [], "21d": [], "63d": [] };
      r.rows.forEach((row) => { if (byHorizon[row.horizon]) byHorizon[row.horizon].push(row); });
      renderHorizonTable("watchlist-5d", byHorizon["5d"]);
      renderHorizonTable("watchlist-21d", byHorizon["21d"]);
      renderHorizonTable("watchlist-63d", byHorizon["63d"]);

      document.getElementById("mb-notes").textContent = r.multibagger.length ? `Top ${r.multibagger.length} by multibagger probability` : "";
      renderMultibaggerTable("watchlist-mb", r.multibagger);

      document.getElementById("mb-lowliq-notes").textContent = r.low_liquidity_multibagger.length
        ? `${r.low_liquidity_multibagger.length} picks below the Rs20cr/day ADTV recommendation floor — shown separately, not filtered into the main list above`
        : "";
      renderMultibaggerTable("watchlist-mb-lowliq", r.low_liquidity_multibagger);
    })
    .catch((e) => {
      showError("watchlist-5d", e);
      showError("watchlist-21d", e);
      showError("watchlist-63d", e);
      showError("watchlist-mb", e);
      showError("watchlist-mb-lowliq", e);
    });
}

function loadPositionsMini() {
  apiGet("/api/v1/paper_trading/state")
    .then((r) => {
      const c = document.getElementById("positions-mini");
      const real = (r.positions || []).filter((p) => p.ticker !== "_HEARTBEAT_");
      if (!r.available || !real.length) {
        c.innerHTML = `<div class="empty">No open positions</div>`;
        return;
      }
      const top3 = real.slice(0, 3);
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Stock"]), el("th", {}, ["P&L"]), el("th", {}, ["Entry Date"])])]),
        el("tbody", {}, top3.map((p) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [el("a", { href: `positions.html` }, [p.ticker])]),
          el("td", { class: "mono " + pnlClass(p.unrealised_pnl_pct) }, [fmtPct(p.unrealised_pnl_pct)]),
          el("td", { class: "mono" }, [p.entry_date]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("positions-mini", e));
}

loadRegime();
loadAlerts();
loadTopBuys();
loadPositionsMini();
loadWatchlistDaily();
