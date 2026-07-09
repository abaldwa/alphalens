// dashboard/static/ml/js/watchlist.js — Daily WatchList (multi-horizon +
// MultiBagger), backed by GET /api/v1/watchlist/daily and /api/v1/watchlist/current.
renderAppShell("ml", "watchlist");

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
      el("th", {}, ["Buy Prob"]), el("th", {}, ["Price"]),
      el("th", {}, ["Target"]), el("th", {}, ["Expected Return"]),
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

apiGet("/api/v1/watchlist/daily", { n_per_horizon: 10 })
  .then((r) => {
    document.getElementById("watchlist-notes").textContent =
      r.date ? `Signals for ${r.date} — targets from the model's own quantile-regression forward-return distribution (or a volatility/ATR-scaled band when unavailable), never a fixed %.` : "No signal data available yet";

    const byHorizon = { "5d": [], "21d": [], "63d": [] };
    r.rows.forEach((row) => { if (byHorizon[row.horizon]) byHorizon[row.horizon].push(row); });
    renderHorizonTable("watchlist-5d", byHorizon["5d"]);
    renderHorizonTable("watchlist-21d", byHorizon["21d"]);
    renderHorizonTable("watchlist-63d", byHorizon["63d"]);

    document.getElementById("mb-notes").textContent = r.multibagger.length ? `Top ${r.multibagger.length} by multibagger probability` : "";
    const mbContainer = document.getElementById("watchlist-mb");
    if (!r.multibagger.length) {
      mbContainer.innerHTML = `<div class="empty">No multibagger scoring data yet</div>`;
      return;
    }
    const cols = ["ticker", "mb_probability", "mb_tier", "mb_archetype", "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m"];
    const labels = ["Ticker", "MB Prob", "Deterministic Probability Band", "Archetype", "6m", "12m", "18m", "24m", "36m"];
    const mbTable = el("table", {}, [
      el("thead", {}, [el("tr", {}, labels.map((l) => el("th", {}, [l])))]),
      el("tbody", {}, r.multibagger.map((t) => el("tr", {}, cols.map((cc) => {
        if (cc === "ticker") return el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${t.ticker}` }, [t.ticker])]);
        if (cc === "mb_probability") return el("td", { class: "mono" }, [fmtPct(t[cc])]);
        if (cc === "mb_tier") return el("td", {}, [el("span", { class: "badge b-purple" }, [mbTierLabel(t[cc])])]);
        if (cc.startsWith("survival")) return el("td", { class: "mono" }, [fmtPct(t[cc])]);
        return el("td", {}, [t[cc] || "—"]);
      })))),
    ]);
    mbContainer.innerHTML = "";
    mbContainer.appendChild(el("div", { class: "card" }, [mbTable]));
  })
  .catch((e) => {
    showError("watchlist-5d", e);
    showError("watchlist-21d", e);
    showError("watchlist-63d", e);
    showError("watchlist-mb", e);
  });
