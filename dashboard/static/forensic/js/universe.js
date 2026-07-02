// dashboard/static/forensic/js/universe.js — FOREN-G Universe Scan
//
// No "Δ Quarter" column — the API exposes a point-in-time snapshot, not a
// time series of composite scores per ticker, so that column is omitted
// rather than fabricated.
renderAppShell("forensic", "universe");

function loadSummary() {
  apiGet("/api/v1/signals/ml/forensic/summary")
    .then((r) => {
      document.getElementById("universe-date").textContent = r.as_of_date ? r.as_of_date.slice(0, 10) : "";
      const c = document.getElementById("universe-summary");
      if (!r.available) {
        c.innerHTML = `<div class="empty">No forensic scores yet</div>`;
        return;
      }
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "card-grid grid grid-4" }, [
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Green"]), el("div", { class: "stat-value up" }, [String(r.green_count)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Amber"]), el("div", { class: "stat-value", style: "color:var(--amber)" }, [String(r.amber_count)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Red"]), el("div", { class: "stat-value dn" }, [String(r.red_count)])])]),
          el("div", { class: "card" }, [el("div", { class: "stat" }, [el("div", { class: "stat-label" }, ["Total Scored"]), el("div", { class: "stat-value" }, [String(r.total_scored)])])]),
        ])
      );
    })
    .catch((e) => showError("universe-summary", e));
}

function loadTable() {
  apiGet("/api/v1/signals/ml/forensic/flagged", { flag: "red,amber,green" })
    .then((r) => {
      const c = document.getElementById("universe-table");
      if (!r.rows.length) {
        c.innerHTML = `<div class="empty">No forensic-scored tickers</div>`;
        return;
      }
      const sorted = [...r.rows].sort((a, b) => (b.forensic_composite || 0) - (a.forensic_composite || 0));
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["#"]), el("th", {}, ["Ticker"]), el("th", {}, ["Score"]), el("th", {}, ["Flag"])])]),
        el("tbody", {}, sorted.map((row, i) => el("tr", {}, [
          el("td", {}, [String(i + 1)]),
          el("td", { style: "font-weight:600" }, [el("a", { href: `dashboard.html?ticker=${row.ticker}` }, [row.ticker])]),
          el("td", {}, [fmtNum(row.forensic_composite, 0)]),
          el("td", {}, [el("span", { class: "badge " + badgeClass(row.forensic_flag_label === "green" ? "green" : (["red", "black"].includes(row.forensic_flag_label) ? "red" : "amber")) }, [row.forensic_flag_label || "—"])]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("universe-table", e));
}

loadSummary();
loadTable();
