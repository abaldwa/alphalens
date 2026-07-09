// dashboard/static/technical/js/chart.js — TA-A Interactive Chart
//
// No charting library exists in this zero-build-tooling app — rather than
// claim a candlestick chart that isn't there, this screen shows the real
// latest price + a curated indicator/pattern panel from the real feature
// store (GET /api/v1/ta/{ticker}/indicators, /patterns).
renderAppShell("technical", "chart");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

// Curated subset of the 76+18 real indicators, for a readable panel —
// the full set is available via the same API, just not all rendered here.
const CURATED_INDICATORS = [
  ["sma_50_ratio", "Price / SMA50", "ratio"],
  ["sma_200_ratio", "Price / SMA200", "ratio"],
  ["ema_21_ratio", "Price / EMA21", "ratio"],
  ["rsi_14", "RSI (14)", "num"],
  ["macd_hist", "MACD Histogram", "num"],
  ["adx_14", "ADX (14)", "num"],
  ["bb_position", "Bollinger Band Position", "num"],
  ["atr_14_pct", "ATR % (14)", "pct"],
  ["supertrend_dir", "Supertrend Direction", "num"],
  ["ichimoku_cloud_position", "Ichimoku Cloud Position", "num"],
  ["composite_momentum_21d", "Composite Momentum (21d)", "num"],
  ["rs_vs_nifty500_21d", "RS vs Nifty 500 (21d)", "pct"],
];

function fmtIndicator(v, kind) {
  if (v === null || v === undefined) return "—";
  if (kind === "pct") return fmtPct(v);
  return fmtNum(v, 3);
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;

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
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Volume"]), el("div", { class: "stat-value" }, [fmtInt(row.volume)])])]),
        ])
      );
    })
    .catch((e) => showError("price-card", e));

  apiGet(`/api/v1/ta/${ticker}/indicators`)
    .then((r) => {
      document.getElementById("ind-date").textContent = r.date || "";
      const c = document.getElementById("indicators-card");
      if (!r.available) {
        c.innerHTML = `<div class="empty">No indicator data for ${ticker}</div>`;
        return;
      }
      const table = el("table", {}, [
        el("tbody", {}, CURATED_INDICATORS.map(([key, label, kind]) => el("tr", {}, [
          el("td", {}, [label]),
          el("td", { class: "mono" }, [fmtIndicator(r.indicators[key], kind)]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("indicators-card", e));

  apiGet(`/api/v1/ta/${ticker}/patterns`)
    .then((r) => {
      const c = document.getElementById("patterns-card");
      if (!r.available) {
        c.innerHTML = `<div class="empty">No pattern data for ${ticker}</div>`;
        return;
      }
      const labels = {
        head_shoulders_score: "Head & Shoulders", double_bottom_score: "Double Bottom",
        cup_handle_score: "Cup & Handle", flag_pattern_score: "Flag Pattern",
        wedge_score: "Wedge", base_breakout_score: "Base Breakout",
      };
      c.innerHTML = "";
      const card = el("div", { class: "card" }, []);
      Object.entries(r.patterns).forEach(([key, v]) => {
        const pct = v === null || v === undefined ? 0 : Math.round(v * 100);
        card.appendChild(
          el("div", { class: "shap-bar" }, [
            el("span", { class: "shap-label" }, [labels[key] || key]),
            el("div", { class: "shap-track" }, [
              el("div", { class: "shap-fill shap-pos", style: `width:${pct / 2}%;right:50%` }, []),
            ]),
            el("span", { class: "mono" }, [v === null || v === undefined ? "—" : pct + "%"]),
          ])
        );
      });
      c.appendChild(card);
    })
    .catch((e) => showError("patterns-card", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
