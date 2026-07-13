// dashboard/static/ml/js/sector_rotation.js — ML12 steps 4-6
// Ranked sector table (trailing-21d relative strength vs Nifty 500) with a
// per-sector top-stocks drill-down, backed by real GET
// /api/v1/sector_rotation/report (features/sector_rotation.py).
renderAppShell("ml", "sector_rotation");

function fmtSectorPct(x) {
  return x == null ? "—" : fmtPct(x, 2);
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

  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Rank"]), el("th", {}, ["Sector"]), el("th", {}, ["Index"]),
      el("th", {}, ["Trailing 21d Return"]), el("th", {}, ["Nifty 500 21d Return"]),
      el("th", {}, ["Relative Strength"]), el("th", {}, ["Top Stocks"]),
    ])]),
    el("tbody", {}, sectors.map((s) => {
      const relClass = s.relative_strength > 0 ? "pos" : s.relative_strength < 0 ? "neg" : "";
      const topStocksCell = !s.top_stocks.length
        ? "—"
        : s.top_stocks
            .map((t) => `${t.ticker}${t.buy_prob != null ? ` (${fmtPct(t.buy_prob, 0)})` : ""}`)
            .join(", ");
      return el("tr", {}, [
        el("td", { class: "mono" }, [String(s.rank)]),
        el("td", { style: "font-weight:600" }, [s.sector]),
        el("td", {}, [s.index_name]),
        el("td", { class: "mono" }, [fmtSectorPct(s.trailing_21d_return)]),
        el("td", { class: "mono" }, [fmtSectorPct(s.nifty500_trailing_21d_return)]),
        el("td", { class: `mono ${relClass}` }, [fmtSectorPct(s.relative_strength)]),
        el("td", {}, [topStocksCell]),
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
      ? `As of ${report.as_of_date} — ranked by 21-trading-day relative strength vs Nifty 500`
      : "No index_ohlcv data available yet";
    renderSectorTable(report.sectors);
  } catch (err) {
    showError("sector-rotation-content", err);
  }
}

loadSectorRotation();
