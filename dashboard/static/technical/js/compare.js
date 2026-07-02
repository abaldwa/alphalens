// dashboard/static/technical/js/compare.js — TA-C Multi-Stock Compare
renderAppShell("technical", "compare");

function heatCellForCorr(v) {
  if (v === null || v === undefined) return el("td", { class: "heatmap-cell h-neutral" }, ["—"]);
  const cls = v > 0.5 ? "h-green" : v < 0 ? "h-red" : "h-amber";
  return el("td", { class: "heatmap-cell " + cls }, [fmtNum(v, 2)]);
}

function load() {
  const tickers = document.getElementById("tickers-input").value.trim().toUpperCase();
  if (!tickers) return;
  showLoading("rs-table");
  showLoading("corr-matrix");

  apiGet("/api/v1/ta/compare", { tickers, days: 60 })
    .then((r) => {
      const rc = document.getElementById("rs-table");
      if (!r.rows.length) {
        rc.innerHTML = `<div class="empty">No data for the given tickers</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Ticker"]), el("th", {}, ["RS vs Nifty500 (21d)"]), el("th", {}, ["Beta (63d)"]), el("th", {}, ["Alpha (21d)"])])]),
        el("tbody", {}, r.rows.map((row) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [row.ticker]),
          el("td", { class: "mono " + pnlClass(row.rs_vs_nifty500_21d) }, [fmtPct(row.rs_vs_nifty500_21d)]),
          el("td", { class: "mono" }, [fmtNum(row.beta_63d, 2)]),
          el("td", { class: "mono " + pnlClass(row.alpha_21d) }, [fmtPct(row.alpha_21d)]),
        ]))),
      ]);
      rc.innerHTML = "";
      rc.appendChild(el("div", { class: "card" }, [table]));

      const cc = document.getElementById("corr-matrix");
      const ts = Object.keys(r.correlation);
      if (!ts.length) {
        cc.innerHTML = `<div class="empty">Not enough overlapping OHLCV history to compute correlation</div>`;
        return;
      }
      const table2 = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, [""]), ...ts.map((t) => el("th", {}, [t]))])]),
        el("tbody", {}, ts.map((t1) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [t1]),
          ...ts.map((t2) => heatCellForCorr(r.correlation[t1][t2])),
        ]))),
      ]);
      cc.innerHTML = "";
      cc.appendChild(el("div", { class: "card" }, [table2]));
    })
    .catch((e) => {
      showError("rs-table", e);
      showError("corr-matrix", e);
    });
}

document.getElementById("load-btn").addEventListener("click", load);
