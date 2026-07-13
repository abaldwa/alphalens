// dashboard/static/technical/js/chart.js — TA-A Interactive Chart
//
// T3 — real candlestick OHLCV rendering via Chart.js + chartjs-chart-
// financial (vendored under dashboard/static/vendor/, no CDN/build step —
// same "plain <script> tag" pattern every other screen uses). Candles and
// the SMA50/SMA200/EMA21 overlay lines are computed from the same real
// ohlcv_adjusted rows (GET /api/v1/ohlcv/{ticker}) the rest of the app
// reads — SMA/EMA are standard, deterministic formulas over real closes,
// not synthetic/fabricated data. The curated indicator/pattern panel below
// (real feature-store snapshot) is unchanged.
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

let candleChart = null;
let volumeChart = null;

function sma(closes, period) {
  const out = new Array(closes.length).fill(null);
  let sum = 0;
  for (let i = 0; i < closes.length; i++) {
    sum += closes[i];
    if (i >= period) sum -= closes[i - period];
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

function ema(closes, period) {
  const out = new Array(closes.length).fill(null);
  const k = 2 / (period + 1);
  let prev = null;
  for (let i = 0; i < closes.length; i++) {
    if (i === period - 1) {
      prev = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
      out[i] = prev;
    } else if (i >= period) {
      prev = closes[i] * k + prev * (1 - k);
      out[i] = prev;
    }
  }
  return out;
}

function overlayDataset(label, values, color) {
  return {
    type: "line",
    label,
    data: values.map((v, i) => (v === null ? null : { x: i, y: v })).filter((p) => p !== null),
    borderColor: color,
    borderWidth: 1.5,
    pointRadius: 0,
    yAxisID: "y",
  };
}

function dateAxisTickLabel(dates) {
  return (value) => {
    const d = dates[value];
    return d ? d.slice(0, 10) : "";
  };
}

function loadCandles(ticker) {
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - 400); // enough real trading days to cover SMA200 warmup
  const fmt = (d) => d.toISOString().slice(0, 10);

  apiGet(`/api/v1/ohlcv/${ticker}`, { from: fmt(from), to: fmt(to) })
    .then((resp) => {
      const rows = (resp.data || []).slice().sort((a, b) => (a.date < b.date ? -1 : 1));
      const canvasEl = document.getElementById("candle-canvas");
      const volEl = document.getElementById("volume-canvas");
      if (candleChart) { candleChart.destroy(); candleChart = null; }
      if (volumeChart) { volumeChart.destroy(); volumeChart = null; }
      if (!rows.length) {
        canvasEl.getContext("2d").clearRect(0, 0, canvasEl.width, canvasEl.height);
        volEl.getContext("2d").clearRect(0, 0, volEl.width, volEl.height);
        return;
      }

      const dates = rows.map((r) => r.date);
      const closes = rows.map((r) => r.close);
      // Category (index-based) x scale, not "time" — chartjs-chart-financial's
      // bar-derived controller miscomputes pixel positions against a
      // continuous time scale (element.x resolves to NaN once trading-day
      // gaps like weekends are involved), so every candle silently fails to
      // draw. Indexing by row also removes weekend gaps from the chart.
      const candleData = rows.map((r, i) => ({ x: i, o: r.open, h: r.high, l: r.low, c: r.close }));

      const datasets = [{
        label: ticker,
        data: candleData,
        yAxisID: "y",
      }];
      if (document.getElementById("ov-sma50").checked) datasets.push(overlayDataset("SMA50", sma(closes, 50), "#3b82f6"));
      if (document.getElementById("ov-sma200").checked) datasets.push(overlayDataset("SMA200", sma(closes, 200), "#a855f7"));
      if (document.getElementById("ov-ema21").checked) datasets.push(overlayDataset("EMA21", ema(closes, 21), "#f59e0b"));

      candleChart = new Chart(canvasEl.getContext("2d"), {
        type: "candlestick",
        data: { labels: dates, datasets },
        options: {
          animation: false,
          scales: {
            x: { type: "category", ticks: { maxRotation: 0, autoSkip: true, callback: dateAxisTickLabel(dates) } },
            y: { position: "right" },
          },
          plugins: { legend: { display: true, position: "top" } },
        },
      });

      volumeChart = new Chart(volEl.getContext("2d"), {
        type: "bar",
        data: {
          labels: dates,
          datasets: [{
            label: "Volume",
            data: rows.map((r, i) => ({ x: i, y: r.volume })),
            backgroundColor: "#64748b88",
          }],
        },
        options: {
          animation: false,
          scales: {
            x: { type: "category", ticks: { maxRotation: 0, autoSkip: true, callback: dateAxisTickLabel(dates) } },
            y: { position: "right", ticks: { callback: (v) => fmtInt(v) } },
          },
          plugins: { legend: { display: false } },
        },
      });
    })
    .catch((e) => showError("candle-card", e));
}

["ov-sma50", "ov-sma200", "ov-ema21"].forEach((id) => {
  document.getElementById(id).addEventListener("change", () => {
    const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
    if (ticker) loadCandles(ticker);
  });
});

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;

  loadCandles(ticker);

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
