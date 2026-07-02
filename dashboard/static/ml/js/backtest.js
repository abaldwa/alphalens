// dashboard/static/ml/js/backtest.js — ML-E Backtest Dashboard
//
// Reports are free-form dicts (no Pydantic schema) — keys are whatever
// backtest/engine.py wrote, rendered generically rather than hardcoded to
// the prototype's exact integrity-checklist text.
renderAppShell("ml", "backtest");

function renderPhaseSection(phaseKey, phase) {
  if (!phase) return null;
  const agg = phase.aggregate || {};
  const integrityOk = phase.integrity_passed;
  const failures = (phase.integrity_detail && phase.integrity_detail.critical_failures) || [];

  const aggCards = [
    ["CAGR (mean)", fmtPct(agg.cagr_mean)],
    ["Sharpe (mean)", fmtNum(agg.sharpe_mean)],
    ["Max Drawdown (worst)", fmtPct(agg.max_drawdown_worst)],
    ["Win Rate (mean)", fmtPct(agg.win_rate_mean)],
    ["Profit Factor (mean)", fmtNum(agg.profit_factor_mean)],
    ["Total Trades", String(agg.total_trades ?? "—")],
  ];

  const foldCols = ["fold_index", "train_start", "train_end", "test_start", "test_end", "cagr", "sharpe", "max_drawdown", "win_rate", "profit_factor", "n_trades"];
  const folds = phase.folds || [];

  const section = el("div", { class: "sec" }, [
    el("div", { class: "sec-head" }, [
      el("span", { class: "sec-title" }, [`${phaseKey} — ${phase.model_name || ""}`]),
      el("span", { class: "badge " + (integrityOk ? "b-green" : "b-red") }, [integrityOk ? "integrity passed" : "integrity FAILED"]),
    ]),
    el("div", { class: "grid grid-3", style: "margin-bottom:14px" }, aggCards.map(([label, v]) => el("div", { class: "card" }, [
      el("div", { class: "stat-label" }, [label]),
      el("div", { class: "stat-value" }, [String(v)]),
    ]))),
  ]);

  if (failures.length) {
    const fc = el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Critical Failures"])]);
    failures.forEach((f) => fc.appendChild(el("div", { class: "alert alert-high" }, [String(f)])));
    section.appendChild(fc);
  }

  if (folds.length) {
    const table = el("table", {}, [
      el("thead", {}, [el("tr", {}, foldCols.map((c) => el("th", {}, [c])))]),
      el("tbody", {}, folds.map((f) => el("tr", {}, foldCols.map((c) => {
        let v = f[c];
        if (["cagr", "max_drawdown", "win_rate"].includes(c)) v = fmtPct(v);
        else if (typeof v === "number") v = fmtNum(v, c === "sharpe" || c === "profit_factor" ? 2 : 0);
        else if (typeof v === "string" && v.includes(" 00:00:00")) v = v.slice(0, 10);
        return el("td", {}, [v === null || v === undefined ? "—" : String(v)]);
      })))),
    ]);
    section.appendChild(el("div", { class: "card" }, [table]));
  }

  return section;
}

function loadReport(name) {
  const body = document.getElementById("report-body");
  body.innerHTML = `<div class="loading">Loading…</div>`;
  apiGet(`/api/v1/backtest/reports/${name}`)
    .then((report) => {
      body.innerHTML = "";
      body.appendChild(el("div", { class: "stat-sub", style: "margin-bottom:14px" }, [
        `Generated ${report.generated_at || "—"} · watchlist size ${report.watchlist_size ?? "—"}`,
      ]));
      ["phase1", "phase2", "phase3"].forEach((k) => {
        const sec = renderPhaseSection(k, report[k]);
        if (sec) body.appendChild(sec);
      });
    })
    .catch((e) => showError("report-body", e));
}

apiGet("/api/v1/backtest/reports")
  .then((r) => {
    const sel = document.getElementById("report-select");
    sel.innerHTML = "";
    if (!r.reports.length) {
      sel.innerHTML = `<option>No reports available</option>`;
      document.getElementById("report-body").innerHTML = `<div class="empty">No backtest reports found in backtest/reports/</div>`;
      return;
    }
    r.reports.forEach((name) => sel.appendChild(el("option", { value: name }, [name])));
    sel.addEventListener("change", () => loadReport(sel.value));
    loadReport(r.reports[r.reports.length - 1]);
  })
  .catch((e) => showError("report-body", e));
