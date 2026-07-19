// dashboard/static/technical/js/watchlist.js — TA Daily WatchList
// backed by GET /api/v1/ta/watchlist/daily
renderAppShell("technical", "watchlist");

function categoryBadgeClass(cat) {
  const map = { A: "b-green", B: "b-teal", C: "b-blue", D: "b-purple", E: "b-amber", F: "b-red", S: "b-gray" };
  return map[cat] || "b-gray";
}

apiGet("/api/v1/ta/watchlist/daily", { limit: 30 })
  .then((r) => {
    document.getElementById("watchlist-notes").textContent =
      r.date ? `Best template match per stock for ${r.date}` : "No TA alert data available yet";
    const c = document.getElementById("watchlist-table");
    if (!r.rows.length) {
      c.innerHTML = `<div class="empty">No screener template matches on the latest date — run the alert checker first</div>`;
      return;
    }
    const table = el("table", {}, [
      el("thead", {}, [el("tr", {}, [
        el("th", {}, ["Stock"]), el("th", {}, ["Name"]), el("th", {}, ["Sector"]),
        el("th", {}, ["Price"]), el("th", {}, ["Template"]), el("th", {}, ["Score"]),
        el("th", {}, ["Rationale"]), el("th", {}, ["Next Resistance"]), el("th", {}, ["Support"]), el("th", {}, ["Deep Dive"]),
      ])]),
      el("tbody", {}, r.rows.map((row) => el("tr", {}, [
        el("td", { style: "font-weight:600" }, [el("a", { href: `chart.html?ticker=${row.ticker}` }, [row.ticker])]),
        el("td", { style: "font-size:12px;color:var(--tx2)" }, [row.company_name || "—"]),
        el("td", {}, [row.sector || "—"]),
        el("td", { class: "mono" }, [fmtMoney(row.current_price)]),
        el("td", {}, [
          el("span", { class: "badge " + categoryBadgeClass(row.category) }, [row.category]),
          el("span", { style: "margin-left:6px" }, [row.template_name]),
        ]),
        el("td", { class: "mono" }, [fmtNum(row.score, 2)]),
        el("td", { style: "font-size:12px;color:var(--tx2);max-width:320px" }, [row.rationale]),
        el("td", { class: "mono", style: "color:var(--red)" }, [
          row.resistance_levels.length ? row.resistance_levels.map(fmtMoney).join(" / ") : "—",
        ]),
        el("td", { class: "mono", style: "color:var(--green)" }, [
          row.support_levels.length ? row.support_levels.map(fmtMoney).join(" / ") : "—",
        ]),
        el("td", {}, [
          el("a", {
            href: `deep_dive.html?ticker=${row.ticker}&reason=${encodeURIComponent(row.rationale || "")}`,
            target: "_blank", rel: "noopener", title: "Technical Deep Dive",
          }, ["🔎"]),
        ]),
      ]))),
    ]);
    c.innerHTML = "";
    c.appendChild(el("div", { class: "card" }, [table]));
  })
  .catch((e) => showError("watchlist-table", e));
