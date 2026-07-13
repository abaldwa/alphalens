// dashboard/static/ml/js/sector_rotation.js — ML12 steps 4-6, extended by
// ML28 (2026-07-13) with 1d/5d/21d/63d relative-strength horizons + trend
// sparklines. Ranked sector table with a per-sector top-stocks drill-down,
// backed by real GET /api/v1/sector_rotation/report
// (features/sector_rotation.py). Tickers use the framework's uniform
// ticker-hyperlink convention (A69's tickerCell) and the table header is
// sortable (A66/A73's sortRows/sortableHeader).
renderAppShell("ml", "sector_rotation");

let currentSectors = [];
let sortState = { key: "rank", dir: "asc" };

function fmtSectorPct(x) {
  return x == null ? "—" : fmtPct(x, 2);
}

function topStocksCellHtml(topStocks) {
  if (!topStocks || !topStocks.length) return "—";
  return topStocks
    .map((t) => {
      const prob = t.buy_prob != null ? ` (${fmtPct(t.buy_prob, 0)})` : "";
      return `<a href="/ui/technical/chart.html?ticker=${t.ticker}" target="_blank" rel="noopener">${t.ticker}</a>` +
        `<a href="/ui/ml/signal.html?ticker=${t.ticker}" target="_blank" rel="noopener" title="Signal Deep Dive" style="margin-left:2px;text-decoration:none;font-size:11px">🔎</a>${prob}`;
    })
    .join(", ");
}

function renderSectorTable(sectors) {
  const c = document.getElementById("sector-rotation-content");
  if (!sectors.length) {
    renderEmptyState("sector-rotation-content", {
      title: "No ranked sectors available",
      detail:
        "index_ohlcv (ingestion/scrapers/nse_indices.py) doesn't yet have " +
        "21+ trading days of history for any mapped sector index. Run " +
        "scripts/backfill_index_ohlcv.py, or wait for the daily scheduled " +
        "download_index_ohlcv step to accumulate enough days.",
    });
    return;
  }

  const sorted = sortRows(sectors, sortState.key, sortState.dir);
  const onSort = (key, dir) => {
    sortState = { key, dir };
    renderSectorTable(currentSectors);
  };

  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Rank", "rank", sortState, onSort),
      sortableHeader("Sector", "sector", sortState, onSort),
      el("th", {}, ["Index"]),
      el("th", {}, ["Trend (63d)"]),
      sortableHeader("RS 1d", "rs_1d", sortState, onSort),
      sortableHeader("RS 5d", "rs_5d", sortState, onSort),
      sortableHeader("RS 21d", "rs_21d", sortState, onSort),
      sortableHeader("RS 63d", "rs_63d", sortState, onSort),
      el("th", {}, ["Top Stocks"]),
    ])]),
    el("tbody", {}, sorted.map((s) => {
      const relClass = s.relative_strength > 0 ? "pos" : s.relative_strength < 0 ? "neg" : "";
      const rsClass = (v) => (v > 0 ? "pos" : v < 0 ? "neg" : "");
      const topStocksTd = el("td", {}, []);
      topStocksTd.innerHTML = topStocksCellHtml(s.top_stocks);
      const sparkTd = el("td", {}, []);
      sparkTd.innerHTML = sparklineSvg(s.sparkline, { strokeAuto: true });
      return el("tr", {}, [
        el("td", { class: "mono" }, [String(s.rank)]),
        el("td", { style: "font-weight:600" }, [s.sector]),
        el("td", {}, [s.index_name]),
        sparkTd,
        el("td", { class: `mono ${rsClass(s.rs_1d)}` }, [fmtSectorPct(s.rs_1d)]),
        el("td", { class: `mono ${rsClass(s.rs_5d)}` }, [fmtSectorPct(s.rs_5d)]),
        el("td", { class: `mono ${rsClass(s.rs_21d) || relClass}` }, [fmtSectorPct(s.rs_21d != null ? s.rs_21d : s.relative_strength)]),
        el("td", { class: `mono ${rsClass(s.rs_63d)}` }, [fmtSectorPct(s.rs_63d)]),
        topStocksTd,
      ]);
    })),
  ]);

  c.innerHTML = "";
  c.appendChild(table);
}

async function loadSectorRotation() {
  showLoading("sector-rotation-content");
  try {
    const report = await apiGet("/api/v1/sector_rotation/report", { top_n_stocks: 5 });
    const banner = document.getElementById("as-of-banner");
    banner.textContent = report.as_of_date
      ? `As of ${report.as_of_date} — ranked by 21-trading-day relative strength vs Nifty 500; 1d/5d/21d/63d columns and 63d trend sparkline also shown (ML28)`
      : "No index_ohlcv data available yet";
    currentSectors = report.sectors;
    renderSectorTable(currentSectors);
  } catch (err) {
    showError("sector-rotation-content", err);
  }
}

loadSectorRotation();

// ===== ML29 (2026-07-13) — Sector Accumulation =====
// (sum of each constituent stock's delivery% x volume) / sector's total
// outstanding shares (a simple sum of each constituent's own
// shares_outstanding — 2026-07-13 user decision), tracked daily. Backed
// by real GET /api/v1/sector_accumulation/daily
// (features/sector_accumulation.py). Clicking a score drills down to the
// per-stock breakdown for that (sector, date) via
// GET /api/v1/sector_accumulation/drilldown.
let currentAccumRows = [];
let accumSortState = { key: "date", dir: "desc" };

function fmtAccumScore(x) {
  return x == null ? "—" : Number(x).toExponential(2);
}

function renderAccumulationTable(rows) {
  const c = document.getElementById("sector-accumulation-content");
  if (!rows.length) {
    renderEmptyState("sector-accumulation-content", {
      title: "No sector accumulation data available",
      detail:
        "Needs both ohlcv_adjusted (volume/delivery_pct) and fundamentals " +
        "(shares_outstanding) rows for at least one full sector's " +
        "constituents on a given date.",
    });
    return;
  }

  const sorted = sortRows(rows, accumSortState.key, accumSortState.dir);
  const onSort = (key, dir) => {
    accumSortState = { key, dir };
    renderAccumulationTable(currentAccumRows);
  };

  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Date", "date", accumSortState, onSort),
      sortableHeader("Sector", "sector", accumSortState, onSort),
      sortableHeader("Accumulation Score", "accumulation_score", accumSortState, onSort),
      sortableHeader("Delivery Volume", "delivery_volume", accumSortState, onSort),
      sortableHeader("Sector Shares Outstanding", "sector_shares_outstanding", accumSortState, onSort),
      sortableHeader("# Stocks", "n_stocks_included", accumSortState, onSort),
    ])]),
    el("tbody", {}, sorted.map((r) => {
      const scoreTd = el("td", { class: "mono", style: "cursor:pointer;text-decoration:underline dotted" }, [fmtAccumScore(r.accumulation_score)]);
      scoreTd.title = "Click to see the per-stock breakdown";
      scoreTd.addEventListener("click", () => loadAccumulationDrilldown(r.sector, r.date));
      return el("tr", {}, [
        el("td", { class: "mono" }, [r.date]),
        el("td", { style: "font-weight:600" }, [r.sector]),
        scoreTd,
        el("td", { class: "mono" }, [fmtNum(r.delivery_volume, 0)]),
        el("td", { class: "mono" }, [fmtNum(r.sector_shares_outstanding, 0)]),
        el("td", { class: "mono" }, [String(r.n_stocks_included)]),
      ]);
    })),
  ]);

  c.innerHTML = "";
  c.appendChild(table);
}

function renderAccumulationDrilldownTable(sector, date, rows) {
  const c = document.getElementById("sector-accumulation-drilldown");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No per-stock breakdown available for ${sector} on ${date}</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Stock"]), el("th", {}, ["Volume"]), el("th", {}, ["Delivery %"]),
      el("th", {}, ["Delivery Volume"]), el("th", {}, ["Shares Outstanding"]), el("th", {}, ["Contribution %"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      tickerCell(r.ticker),
      el("td", { class: "mono" }, [fmtNum(r.volume, 0)]),
      el("td", { class: "mono" }, [fmtPct(r.delivery_pct / 100)]),
      el("td", { class: "mono" }, [fmtNum(r.delivery_volume, 0)]),
      el("td", { class: "mono" }, [fmtNum(r.shares_outstanding, 0)]),
      el("td", { class: "mono" }, [fmtPct(r.contribution_pct / 100)]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "sec-head" }, [
    el("span", { class: "sec-title", style: "font-size:14px" }, [`Breakdown — ${sector}, ${date}`]),
  ]));
  c.appendChild(el("div", { class: "card" }, [table]));
}

async function loadAccumulationDrilldown(sector, date) {
  const c = document.getElementById("sector-accumulation-drilldown");
  c.innerHTML = `<div class="loading">Loading…</div>`;
  try {
    const rows = await apiGet("/api/v1/sector_accumulation/drilldown", { sector, date });
    renderAccumulationDrilldownTable(sector, date, rows);
  } catch (err) {
    showError("sector-accumulation-drilldown", err);
  }
}

async function loadSectorAccumulation() {
  showLoading("sector-accumulation-content");
  try {
    const rows = await apiGet("/api/v1/sector_accumulation/daily", {});
    currentAccumRows = rows;
    renderAccumulationTable(currentAccumRows);
  } catch (err) {
    showError("sector-accumulation-content", err);
  }
}

loadSectorAccumulation();
