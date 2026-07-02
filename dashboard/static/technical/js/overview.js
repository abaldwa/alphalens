// dashboard/static/technical/js/overview.js — TA-E Market Overview
renderAppShell("technical", "overview");

apiGet("/api/v1/ta/market_overview")
  .then((r) => {
    document.getElementById("overview-date").textContent = r.date ? r.date.slice(0, 10) : "";
    const sc = document.getElementById("breadth-stats");
    if (!r.available) {
      sc.innerHTML = `<div class="empty">No OHLCV data available</div>`;
      document.getElementById("sector-table").innerHTML = `<div class="empty">—</div>`;
      return;
    }
    sc.innerHTML = "";
    sc.appendChild(
      el("div", { class: "card-grid grid grid-3" }, [
        el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Advances"]), el("div", { class: "stat-value up" }, [String(r.advances)])])]),
        el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Declines"]), el("div", { class: "stat-value dn" }, [String(r.declines)])])]),
        el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Unchanged"]), el("div", { class: "stat-value" }, [String(r.unchanged)])])]),
      ])
    );

    const tc = document.getElementById("sector-table");
    if (!r.sector_breadth.length) {
      tc.innerHTML = `<div class="empty">No sector data</div>`;
      return;
    }
    const table = el("table", {}, [
      el("thead", {}, [el("tr", {}, [el("th", {}, ["Sector"]), el("th", {}, ["Advances"]), el("th", {}, ["Declines"]), el("th", {}, ["Avg Change %"])])]),
      el("tbody", {}, r.sector_breadth.map((s) => el("tr", {}, [
        el("td", { style: "font-weight:600" }, [s.sector]),
        el("td", { class: "mono up" }, [String(s.advances)]),
        el("td", { class: "mono dn" }, [String(s.declines)]),
        el("td", { class: "mono " + pnlClass(s.avg_change_pct) }, [fmtPct(s.avg_change_pct)]),
      ]))),
    ]);
    tc.innerHTML = "";
    tc.appendChild(el("div", { class: "card" }, [table]));
  })
  .catch((e) => {
    showError("breadth-stats", e);
    showError("sector-table", e);
  });
