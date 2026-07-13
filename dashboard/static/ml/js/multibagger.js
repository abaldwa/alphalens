// dashboard/static/ml/js/multibagger.js — ML-C Multibagger Watchlist
renderAppShell("ml", "multibagger");

function renderMbTable(containerId, tickers) {
  const c = document.getElementById(containerId);
  if (!tickers.length) {
    c.innerHTML = `<div class="empty">No multibagger scoring data yet</div>`;
    return;
  }
  const cols = ["ticker", "mb_probability", "mb_tier", "mb_archetype", "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m"];
  const labels = ["Ticker", "MB Prob", "Deterministic Probability Band", "Archetype", "6m", "12m", "18m", "24m", "36m"];
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, labels.map((l) => el("th", {}, [l])))]),
    el("tbody", {}, tickers.map((t) => el("tr", {}, cols.map((cc) => {
      if (cc === "ticker") return el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${t.ticker}` }, [t.ticker])]);
      if (cc === "mb_probability") return el("td", { class: "mono" }, [fmtPct(t[cc])]);
      if (cc === "mb_tier") return el("td", {}, [el("span", { class: "badge b-purple" }, [mbTierLabel(t[cc])])]);
      if (cc.startsWith("survival")) return el("td", { class: "mono" }, [fmtPct(t[cc])]);
      return el("td", {}, [t[cc] || "—"]);
    })))),
  ]);
  c.innerHTML = "";
  // FutureDevelopment.md #14: multibagger now scores weekly (Sunday,
  // schedule_multibagger_scoring) rather than never — same "as of {date}"
  // staleness pattern signal_5d's top_buys/deep-dive already use (hub.js's
  // loadTopBuys/signal.js), since this watchlist can legitimately be
  // several days stale between weekly runs.
  const asOfDate = tickers.length && tickers[0].date ? String(tickers[0].date).slice(0, 10) : null;
  if (asOfDate) {
    c.appendChild(el("div", { class: "empty", style: "margin-bottom:8px" }, [`As of ${asOfDate} (scored weekly, Sunday)`]));
  }
  c.appendChild(el("div", { class: "card" }, [table]));
}

apiGet("/api/v1/watchlist/current")
  .then((r) => {
    document.getElementById("watchlist-notes").textContent = r.notes || "";
    if (!r.implemented) {
      document.getElementById("watchlist-table").innerHTML = `<div class="empty">${r.notes || "No watchlist data yet"}</div>`;
      document.getElementById("watchlist-lowliq-table").innerHTML = `<div class="empty">—</div>`;
      return;
    }
    renderMbTable("watchlist-table", r.tickers);

    document.getElementById("watchlist-lowliq-notes").textContent = r.low_liquidity_tickers.length
      ? `${r.low_liquidity_tickers.length} picks below the Rs20cr/day ADTV recommendation floor — shown separately, not filtered from the main list above (ML27)`
      : "";
    renderMbTable("watchlist-lowliq-table", r.low_liquidity_tickers);
  })
  .catch((e) => {
    showError("watchlist-table", e);
    showError("watchlist-lowliq-table", e);
  });
