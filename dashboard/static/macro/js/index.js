// dashboard/static/macro/js/index.js — A27 manual macro-entry screen.
// Promoted out of AlphaLens.Ops into its own top-level app (was
// renderAppShell("ops", "macro")).
renderAppShell("macro", "index");

// Mirrors features/real_economy_macro.py's MANUAL_ENTRY_FEATURES (the 8
// series with no free automated source — cement_dispatches_growth/
// power_consumption_growth are excluded here since they already have a
// real scraper, see datastore/api/routers/macro.py's module docstring).
const MANUAL_ENTRY_FEATURES = [
  "gst_collection_growth",
  "pmi_manufacturing",
  "pmi_services",
  "iip_growth",
  "auto_monthly_sales_growth",
  "rail_freight_growth",
  "upi_transaction_growth",
  "bank_credit_growth",
];

function currentMonthEnd() {
  const now = new Date();
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
  const monthEnd = new Date(nextMonth.getTime() - 86400000);
  return monthEnd.toISOString().slice(0, 10);
}

function renderMacroEntryForm() {
  const container = document.getElementById("macro-entry-form");
  container.innerHTML = "";

  const monthInput = el("input", { type: "date", id: "macro-month-input", value: currentMonthEnd() }, []);
  const rows = [
    el("div", { class: "kv-row" }, [
      el("span", { class: "kv-key" }, ["Month (any date in the target month)"]),
      el("span", {}, [monthInput]),
    ]),
  ];

  const valueInputs = {};
  MANUAL_ENTRY_FEATURES.forEach((feature) => {
    const input = el("input", { type: "number", step: "any", id: `macro-value-${feature}`, placeholder: "value" }, []);
    valueInputs[feature] = input;
    rows.push(
      el("div", { class: "kv-row" }, [
        el("span", { class: "kv-key" }, [feature]),
        el("span", {}, [input]),
      ])
    );
  });

  const status = el("span", { id: "macro-submit-status", style: "margin-left:8px;font-size:12px" }, []);
  const submitBtn = el("button", { id: "macro-submit-btn" }, ["Save entered values"]);
  submitBtn.addEventListener("click", () => submitMacroEntries(monthInput, valueInputs, status));

  container.appendChild(el("div", { class: "card" }, [...rows, el("div", { class: "kv-row" }, [submitBtn, status])]));
}

async function submitMacroEntries(monthInput, valueInputs, status) {
  const monthEnd = monthInput.value;
  if (!monthEnd) {
    status.textContent = "Pick a month first.";
    status.style.color = "var(--red)";
    return;
  }

  const toWrite = MANUAL_ENTRY_FEATURES.filter((f) => valueInputs[f].value !== "");
  if (toWrite.length === 0) {
    status.textContent = "Enter at least one value.";
    status.style.color = "var(--red)";
    return;
  }

  status.textContent = "Saving...";
  status.style.color = "var(--tx3)";
  let written = 0;
  let failed = 0;
  for (const feature of toWrite) {
    try {
      await apiPost("/api/v1/macro/indicators", {
        feature_name: feature,
        reference_month_end: monthEnd,
        value: parseFloat(valueInputs[feature].value),
      });
      written += 1;
      valueInputs[feature].value = "";
    } catch (e) {
      failed += 1;
    }
  }

  status.textContent = failed === 0 ? `Saved ${written} value(s).` : `Saved ${written}, ${failed} failed.`;
  status.style.color = failed === 0 ? "var(--green)" : "var(--red)";
  loadMacroHistory();
}

function loadMacroHistory() {
  const container = document.getElementById("macro-history-table");
  showLoading("macro-history-table");

  Promise.all(MANUAL_ENTRY_FEATURES.map((f) => apiGet("/api/v1/macro/indicators", { feature_name: f, limit_months: 12 })))
    .then((responses) => {
      const allRows = [];
      responses.forEach((r) => allRows.push(...r.rows));
      if (allRows.length === 0) {
        renderEmptyState("macro-history-table", {
          title: "No manual entries yet",
          detail: "Use the form above to enter this month's readings.",
        });
        return;
      }
      allRows.sort((a, b) => (a.feature_name < b.feature_name ? -1 : a.feature_name > b.feature_name ? 1 : b.reference_month_end.localeCompare(a.reference_month_end)));

      const table = el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", {}, ["Indicator"]),
            el("th", {}, ["Month"]),
            el("th", {}, ["Value"]),
            el("th", {}, ["Available Since"]),
          ]),
        ]),
        el(
          "tbody",
          {},
          allRows.map((r) =>
            el("tr", {}, [
              el("td", {}, [r.feature_name]),
              el("td", { class: "mono" }, [r.reference_month_end]),
              el("td", { class: "mono" }, [fmtNum(r.value)]),
              el("td", { class: "mono", style: "font-size:11px" }, [r.availability_date]),
            ])
          )
        ),
      ]);
      container.innerHTML = "";
      container.appendChild(table);
    })
    .catch((e) => showError("macro-history-table", e));
}

renderMacroEntryForm();
loadMacroHistory();
