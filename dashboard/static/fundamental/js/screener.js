// dashboard/static/fundamental/js/screener.js — FA-D Fundamental Screener
//
// Only the 3 presets features/fundamental_composites.py actually computes
// (quality_compounder, garp, turnaround) — not a general criteria-builder.
renderAppShell("fundamental", "screener");

const PRESETS = [
  ["quality_compounder", "Quality Compounder"],
  ["garp", "GARP"],
  ["turnaround", "Turnaround"],
];

function load(preset) {
  showLoading("screener-table");
  apiGet("/api/v1/fundamentals/screener", { preset })
    .then((r) => {
      const c = document.getElementById("screener-table");
      if (!r.tickers.length) {
        c.innerHTML = `<div class="empty">No tickers match "${preset}" today</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["#"]), el("th", {}, ["Ticker"])])]),
        el("tbody", {}, r.tickers.map((t, i) => el("tr", {}, [
          el("td", {}, [String(i + 1)]),
          el("td", { style: "font-weight:600" }, [el("a", { href: `dashboard.html?ticker=${t}` }, [t])]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [
        el("div", { class: "stat-sub", style: "margin-bottom:8px" }, [`${r.tickers.length} matches as of ${r.date || "—"}`]),
        table,
      ]));
    })
    .catch((e) => showError("screener-table", e));
}

const buttons = document.getElementById("preset-buttons");
PRESETS.forEach(([key, label]) => {
  const btn = el("button", {}, [label]);
  btn.addEventListener("click", () => load(key));
  buttons.appendChild(btn);
});
load(PRESETS[0][0]);
