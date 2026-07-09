// dashboard/static/technical/js/screener.js — TA-D Strategy Screener
renderAppShell("technical", "screener");

let currentTemplates = [];

function templateOption(t) {
  return el("option", { value: t.name }, [`${t.name} — ${t.description} (${t.category})`]);
}

function renderResultsTable(data) {
  const c = document.getElementById("results-table");
  if (!data.rows || !data.rows.length) {
    c.innerHTML = `<div class="empty">No tickers matched "${data.template_name}" on ${data.date || "the latest date"}</div>`;
    return;
  }
  const keyCols = Object.keys(data.rows[0].key_values || {});
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]),
      el("th", {}, ["Score"]),
      el("th", {}, ["Matched"]),
      ...keyCols.map((k) => el("th", {}, [k])),
    ])]),
    el("tbody", {}, data.rows.map((r) => el("tr", {}, [
      el("td", { style: "font-weight:600" }, [
        el("a", { href: `chart.html?ticker=${r.ticker}` }, [r.ticker]),
      ]),
      el("td", { class: "mono" }, [fmtNum(r.score, 2)]),
      el("td", { class: "mono" }, [`${r.matched_conditions}/${r.total_conditions}`]),
      ...keyCols.map((k) => el("td", { class: "mono" }, [fmtNum(r.key_values[k], 2)])),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { style: "margin-bottom:8px; color:var(--muted, #888)" }, [
    `${fmtInt(data.count)} match(es) on ${data.date || "latest date"}`,
  ]));
  c.appendChild(el("div", { class: "card" }, [table]));
}

function runTemplate(name) {
  showLoading("results-table");
  apiGet(`/api/v1/ta/screener/run/${name}`)
    .then(renderResultsTable)
    .catch((e) => showError("results-table", e));
}

function init() {
  const content = document.getElementById("content");
  content.innerHTML = "";
  const select = el("select", { id: "template-select" }, []);
  content.appendChild(
    el("div", { class: "card", style: "margin-bottom:12px; display:flex; gap:8px; align-items:center" }, [
      el("label", { for: "template-select" }, ["Template:"]),
      select,
      el("button", { id: "run-btn", class: "btn" }, ["Run"]),
    ])
  );
  content.appendChild(el("div", { id: "results-table" }, []));

  apiGet("/api/v1/ta/screener/templates")
    .then((r) => {
      currentTemplates = r.templates || [];
      if (!currentTemplates.length) {
        document.getElementById("results-table").innerHTML = `<div class="empty">No screener templates available</div>`;
        return;
      }
      currentTemplates.forEach((t) => select.appendChild(templateOption(t)));
      document.getElementById("run-btn").addEventListener("click", () => runTemplate(select.value));
      runTemplate(currentTemplates[0].name);
    })
    .catch((e) => showError("results-table", e));
}

init();
