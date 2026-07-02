// dashboard/static/technical/js/alerts.js — TA-9 Alert Manager
renderAppShell("technical", "alerts");

let currentTemplates = [];

function templateOption(t) {
  return el("option", { value: t.name }, [`${t.name} — ${t.description} (${t.category})`]);
}

function renderAlertsTable(rows) {
  const c = document.getElementById("alerts-table");
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No alerts yet — create one above to start watching a ticker/template.</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Ticker"]),
      el("th", {}, ["Template"]),
      el("th", {}, ["Category"]),
      el("th", {}, ["Status"]),
      el("th", {}, ["Last Triggered"]),
      el("th", {}, [""]),
    ])]),
    el("tbody", {}, rows.map((r) => {
      const deleteBtn = el("button", { class: "btn" }, ["Delete"]);
      deleteBtn.addEventListener("click", () => deleteAlert(r.alert_id));
      return el("tr", {}, [
        el("td", { style: "font-weight:600" }, [
          el("a", { href: `chart.html?ticker=${r.ticker}` }, [r.ticker]),
        ]),
        el("td", { class: "mono" }, [r.template_name]),
        el("td", {}, [r.category]),
        el("td", {}, [
          r.triggered_today
            ? el("span", { class: "badge b-red" }, ["🔴 Triggered"])
            : el("span", { class: "badge b-blue" }, ["Watching"]),
        ]),
        el("td", { class: "mono" }, [r.last_triggered_date || "—"]),
        el("td", {}, [deleteBtn]),
      ]);
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadAlerts() {
  showLoading("alerts-table");
  apiGet("/api/v1/ta/user-alerts")
    .then((data) => renderAlertsTable(data.rows || []))
    .catch((e) => showError("alerts-table", e));
}

function deleteAlert(alertId) {
  apiDelete(`/api/v1/ta/user-alerts/${alertId}`)
    .then(loadAlerts)
    .catch((e) => showError("alerts-table", e));
}

function createAlert() {
  const ticker = document.getElementById("alert-ticker-input").value.trim().toUpperCase();
  const template = document.getElementById("alert-template-select").value;
  if (!ticker || !template) return;
  apiPost("/api/v1/ta/user-alerts", { ticker, template_name: template })
    .then(() => {
      document.getElementById("alert-ticker-input").value = "";
      loadAlerts();
    })
    .catch((e) => showError("alerts-table", e));
}

function init() {
  const content = document.getElementById("content");
  content.innerHTML = "";

  const tickerInput = el("input", {
    id: "alert-ticker-input",
    type: "text",
    placeholder: "Ticker (e.g. RELIANCE)",
  });
  const select = el("select", { id: "alert-template-select" }, []);

  content.appendChild(
    el("div", { class: "card", style: "margin-bottom:12px; display:flex; gap:8px; align-items:center" }, [
      el("label", { for: "alert-ticker-input" }, ["Ticker:"]),
      tickerInput,
      el("label", { for: "alert-template-select" }, ["Template:"]),
      select,
      el("button", { id: "create-alert-btn", class: "btn" }, ["Create Alert"]),
    ])
  );
  content.appendChild(el("div", { id: "alerts-table" }, []));

  TickerPicker.attach("alert-ticker-input");

  apiGet("/api/v1/ta/screener/templates")
    .then((data) => {
      currentTemplates = data.templates || [];
      select.innerHTML = "";
      currentTemplates.forEach((t) => select.appendChild(templateOption(t)));
    })
    .catch((e) => showError("alerts-table", e));

  document.getElementById("create-alert-btn").addEventListener("click", createAlert);
  loadAlerts();
}

init();
