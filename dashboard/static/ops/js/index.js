// dashboard/static/ops/js/index.js — Job Autoruns (Ops)
renderAppShell("ops", "index");

// sortRows/sortableHeader moved to ../../js/api.js (#21 — shared with
// Signal Deep Dive's full-universe table and Exit Urgency).

// 2026-07-05: daily_pipeline.py now runs as the alphalens-scheduler.service
// systemd --user unit + a 30-min resource-monitor timer (see
// scripts/monitor_scheduler_resources.py, datastore/api/routers/ops.py's
// get_ops_scheduler_resources) — decoupled from VS Code/Claude Code so a
// closed session no longer stops the pipeline. This card shows whether the
// always-on service is actually up, current memory/load, and whether
// worker counts are currently throttled or a restart was deferred to avoid
// interrupting an in-progress training/inference step.
function loadSchedulerResources() {
  apiGet("/api/v1/ops/scheduler-resources")
    .then((r) => {
      const c = document.getElementById("scheduler-resources-card");
      const rows = [
        el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Service"]),
          el("span", {}, [
            r.service_active
              ? el("span", { class: "badge b-green" }, ["ACTIVE"])
              : el("span", { class: "badge b-red" }, [r.service_state || "DOWN"]),
          ]),
        ]),
        el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Memory Available"]),
          el("span", { class: "mono" }, [r.mem_available_pct != null ? `${r.mem_available_pct.toFixed(1)}%` : "—"]),
        ]),
        el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Load (1 min)"]),
          el("span", { class: "mono" }, [r.load1 != null ? r.load1.toFixed(2) : "—"]),
        ]),
        el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Worker Counts"]),
          el("span", {}, [
            r.hmm_feature_workers != null
              ? el("span", { class: "badge " + (r.throttled ? "b-amber" : "b-green") }, [
                  `HMM=${r.hmm_feature_workers} · preload=${r.feature_cache_preload_workers}` + (r.throttled ? " (throttled)" : ""),
                ])
              : "—",
          ]),
        ]),
        el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Last Monitor Check"]),
          el("span", { class: "mono", style: "font-size:11px" }, [r.last_monitor_run_at || "never"]),
        ]),
      ];
      if (r.last_deferred_step) {
        rows.push(el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Deferred Throttle"]),
          el("span", { class: "badge b-amber" }, [`waited for '${r.last_deferred_step}' to finish — training never interrupted`]),
        ]));
      }
      if (r.error) {
        rows.push(el("div", { class: "kv-row" }, [
          el("span", { class: "kv-key" }, ["Note"]),
          el("span", { style: "font-size:11px;color:var(--red)" }, [r.error]),
        ]));
      }
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, rows));
    })
    .catch((e) => showError("scheduler-resources-card", e));
}

function loadHeartbeats() {
  apiGet("/api/v1/ops/heartbeats")
    .then((rows) => {
      const c = document.getElementById("heartbeats-table");
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Job"]), el("th", {}, ["Last Run Date"]), el("th", {}, ["Last Status"]),
          el("th", {}, ["Next Scheduled Run"]), el("th", {}, ["Staleness"]),
        ])]),
        el("tbody", {}, rows.map((r) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [r.job_id]),
          el("td", { class: "mono" }, [r.last_attempt_at ? r.last_attempt_at.slice(0, 19).replace("T", " ") : "never"]),
          el("td", {}, [r.last_status ? el("span", { class: "badge " + (r.last_status === "success" ? "b-green" : "b-red") }, [r.last_status]) : "—"]),
          el("td", { class: "mono" }, [r.next_run_time ? r.next_run_time.slice(0, 19).replace("T", " ") : "—"]),
          el("td", {}, [el("span", { class: "badge " + (r.is_stale ? "b-red" : "b-green") }, [r.is_stale ? "STALE" : "OK"])]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("heartbeats-table", e));
}

function forceStep(stepName, row) {
  row.style.opacity = "0.5";
  apiPost(`/api/v1/ops/steps/${stepName}/force`)
    .then((r) => {
      row.style.opacity = "1";
      loadSteps();
    })
    .catch((e) => {
      row.style.opacity = "1";
      alert(`Force-run failed: ${e.message}`);
      loadSteps();
    });
}

function renderStepsTable(steps) {
  const c = document.getElementById("steps-table");
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Step"]), el("th", {}, ["Backfillable"]), el("th", {}, ["Status"]),
      el("th", {}, ["Started"]), el("th", {}, ["Completed"]), el("th", {}, ["Last Successful Run"]),
      el("th", {}, ["Next Scheduled Run"]), el("th", {}, ["Error"]), el("th", {}, ["Run Type"]), el("th", {}, ["Action"]),
    ])]),
    el("tbody", {}, steps.map((s) => {
      const statusCls = { success: "b-green", failed: "b-red", running: "b-amber", never_run: "b-gray", skipped: "b-gray" }[s.status] || "b-gray";
      const row = el("tr", {}, [
        el("td", { style: "font-weight:600" }, [s.step_name]),
        el("td", {}, [s.is_backfillable ? "yes" : "no"]),
        el("td", {}, [el("span", { class: "badge " + statusCls }, [s.status])]),
        el("td", { class: "mono", style: "font-size:11px" }, [s.started_at ? s.started_at.slice(0, 19).replace("T", " ") : "—"]),
        el("td", { class: "mono", style: "font-size:11px" }, [s.completed_at ? s.completed_at.slice(0, 19).replace("T", " ") : "—"]),
        el("td", { class: "mono", style: "font-size:11px" }, [s.last_success_date || "never"]),
        el("td", { class: "mono", style: "font-size:11px" }, [s.next_scheduled_run ? s.next_scheduled_run.slice(0, 19).replace("T", " ") : "—"]),
        el("td", { style: "font-size:11px;color:var(--red)" }, [s.error_message || ""]),
        el("td", {}, [
          s.is_backfill
            ? el("span", { class: "badge b-amber", title: "Produced by a backfill/catch-up run, not the same-day live run" }, ["BACKFILLED"])
            : (s.status === "success" ? el("span", { class: "badge b-green" }, ["LIVE"]) : "—"),
        ]),
        el("td", {}, []),
      ]);
      const actionCell = row.lastChild;
      if (s.status !== "success" && s.status !== "running") {
        const btn = el("button", {}, ["Force Start"]);
        btn.addEventListener("click", () => forceStep(s.step_name, row));
        actionCell.appendChild(btn);
      }
      return row;
    })),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadSteps() {
  apiGet("/api/v1/ops/steps")
    .then((r) => {
      document.getElementById("steps-date").textContent = r.date;
      renderStepsTable(r.steps);
    })
    .catch((e) => showError("steps-table", e));
}

const runsSortState = { key: "run_id", dir: "desc" };
let lastRunsData = [];

function renderRunsTable(runs) {
  const c = document.getElementById("runs-table");
  if (!runs.length) {
    c.innerHTML = `<div class="empty">No pipeline runs recorded yet</div>`;
    return;
  }
  const sorted = sortRows(runs, runsSortState.key, runsSortState.dir);
  const onSort = (key, dir) => {
    runsSortState.key = key;
    runsSortState.dir = dir;
    renderRunsTable(lastRunsData);
  };
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      sortableHeader("Run ID", "run_id", runsSortState, onSort),
      sortableHeader("Date", "date", runsSortState, onSort),
      sortableHeader("Status", "status", runsSortState, onSort),
      el("th", {}, ["Failed Step(s)"]),
      sortableHeader("Stocks", "stocks_processed", runsSortState, onSort),
      sortableHeader("Started", "started_at", runsSortState, onSort),
      sortableHeader("Completed", "completed_at", runsSortState, onSort),
      el("th", {}, ["Sanity Check"]),
      el("th", {}, ["Run Type"]),
    ])]),
    el("tbody", {}, sorted.map((run) => el("tr", {}, [
      el("td", { class: "mono" }, [String(run.run_id ?? "—")]),
      el("td", { class: "mono" }, [run.date || "—"]),
      el("td", {}, [el("span", { class: "badge " + (run.status === "success" ? "b-green" : "b-red") }, [run.status || "—"])]),
      el("td", { style: "font-size:11px" }, [
        run.failed_steps && run.failed_steps.length
          ? el("div", {}, run.failed_steps.map((fs) => el("div", {}, [
              el("span", { style: "font-weight:600" }, [fs.step_name]),
              fs.error_message ? `: ${fs.error_message}` : "",
            ])))
          : (run.status === "failed" ? "(retried successfully since — checkpoint no longer failed)" : ""),
      ]),
      el("td", { class: "mono" }, [String(run.stocks_processed ?? "—")]),
      el("td", { class: "mono", style: "font-size:11px" }, [run.started_at ? run.started_at.slice(0, 19).replace("T", " ") : "—"]),
      el("td", { class: "mono", style: "font-size:11px" }, [run.completed_at ? run.completed_at.slice(0, 19).replace("T", " ") : "—"]),
      el("td", {}, [
        run.sanity_check_passed === true
          ? el("span", { class: "badge b-green" }, ["PASSED"])
          : run.sanity_check_passed === false
          ? el("span", { class: "badge b-red" }, ["FAILED"])
          : el("span", { class: "badge b-gray" }, ["—"]),
      ]),
      el("td", {}, [
        run.is_backfill
          ? el("span", { class: "badge b-amber", title: "At least one step for this date was produced by a backfill/catch-up run" }, ["BACKFILLED"])
          : el("span", { class: "badge b-green" }, ["LIVE"]),
      ]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadRuns() {
  apiGet("/api/v1/ops/runs", { limit: 20 })
    .then((r) => {
      lastRunsData = r.runs;
      renderRunsTable(r.runs);
    })
    .catch((e) => showError("runs-table", e));
}

// #4: DataStore API Console (freshness rollup) — last-write timestamp +
// row count per data source table.
function loadFreshness() {
  apiGet("/api/v1/ops/freshness")
    .then((r) => {
      const c = document.getElementById("freshness-table");
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Source"]), el("th", {}, ["Row Count"]),
          el("th", {}, ["Latest Data Date"]), el("th", {}, ["Last Write"]), el("th", {}, ["Error"]),
        ])]),
        el("tbody", {}, r.sources.map((s) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [s.source]),
          el("td", { class: "mono" }, [s.row_count != null ? s.row_count.toLocaleString("en-IN") : "—"]),
          el("td", { class: "mono" }, [s.latest_data_date || "—"]),
          el("td", { class: "mono", style: "font-size:11px" }, [s.last_write_at ? s.last_write_at.slice(0, 19).replace("T", " ") : "—"]),
          el("td", { style: "font-size:11px;color:var(--red)" }, [s.error || ""]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("freshness-table", e));
}

// A20 (Data Integrity Checker): pending findings from the four checks
// (corporate-action cross-check, null/NaN sweep, holiday-leakage,
// random 5yr spot-check) — approve/reject here is the only path that
// writes production data on A20's behalf, matching this project's
// "flag, don't silently write" discipline.
function loadIntegrityFindings() {
  apiGet("/api/v1/ops/integrity-findings", { status: "pending" })
    .then((r) => {
      const c = document.getElementById("integrity-findings-table");
      if (!r.findings || r.findings.length === 0) {
        renderEmptyState("integrity-findings-table", { icon: "✓", title: "No pending findings", detail: "All four A20 checks are clean as of the last run." });
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Check"]), el("th", {}, ["Ticker"]), el("th", {}, ["Date"]),
          el("th", {}, ["Severity"]), el("th", {}, ["Description"]), el("th", {}, ["Fix Proposed"]),
          el("th", {}, ["Action"]),
        ])]),
        el("tbody", {}, r.findings.map((f) => {
          const row = el("tr", {}, [
            el("td", { style: "font-weight:600" }, [f.check_name]),
            el("td", { class: "mono" }, [f.ticker || "—"]),
            el("td", { class: "mono" }, [f.finding_date]),
            el("td", {}, [el("span", { class: "badge " + (f.severity === "critical" ? "b-red" : f.severity === "warning" ? "b-amber" : "b-green") }, [f.severity])]),
            el("td", { style: "font-size:12px" }, [f.description]),
            el("td", {}, [f.proposed_fix_sql ? el("span", { class: "badge b-amber" }, ["yes"]) : "—"]),
            el("td", {}, []),
          ]);
          const actionsCell = row.lastChild;
          const approveBtn = el("button", {}, ["Approve"]);
          const rejectBtn = el("button", { style: "margin-left:6px" }, ["Reject"]);
          approveBtn.addEventListener("click", () => actOnFinding(f.id, "approve", row));
          rejectBtn.addEventListener("click", () => actOnFinding(f.id, "reject", row));
          actionsCell.appendChild(approveBtn);
          actionsCell.appendChild(rejectBtn);
          return row;
        })),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("integrity-findings-table", e));
}

function actOnFinding(findingId, action, row) {
  row.style.opacity = "0.5";
  apiPost(`/api/v1/ops/integrity-findings/${findingId}/${action}?reviewed_by=operator`)
    .then(() => loadIntegrityFindings())
    .catch((e) => {
      row.style.opacity = "1";
      alert(`${action} failed: ${e.message}`);
    });
}

// A21 (Pipeline Health Checker): pending "missed job" findings — a
// registered job (daily_pipeline, weekend_feature_backfill,
// weekend_fundamentals, mf_holdings_ingestion, etc.) that didn't record
// a success on a calendar date it was expected to in the trailing 7
// days. Approve/reject here is the only path that triggers a catch-up
// run on A21's behalf, matching this project's "flag, don't silently
// write" discipline — a catch-up can take a while (e.g. re-running a
// weekend script), so the button stays disabled until it resolves.
function loadMissedJobs() {
  apiGet("/api/v1/ops/missed-jobs", { status: "pending" })
    .then((r) => {
      if (!r.findings || r.findings.length === 0) {
        renderEmptyState("missed-jobs-table", { icon: "✓", title: "No missed jobs", detail: "Every registered job recorded a success on every expected date in the trailing window." });
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [
          el("th", {}, ["Job"]), el("th", {}, ["Missed Date"]),
          el("th", {}, ["Severity"]), el("th", {}, ["Description"]), el("th", {}, ["Catch-Up Action"]),
          el("th", {}, ["Action"]),
        ])]),
        el("tbody", {}, r.findings.map((f) => {
          const row = el("tr", {}, [
            el("td", { style: "font-weight:600" }, [f.job_id]),
            el("td", { class: "mono" }, [f.missed_date]),
            el("td", {}, [el("span", { class: "badge " + (f.severity === "critical" ? "b-red" : f.severity === "warning" ? "b-amber" : "b-green") }, [f.severity])]),
            el("td", { style: "font-size:12px" }, [f.description]),
            el("td", { class: "mono", style: "font-size:11px" }, [f.proposed_catchup_action || "—"]),
            el("td", {}, []),
          ]);
          const actionsCell = row.lastChild;
          const approveBtn = el("button", {}, ["Approve"]);
          const rejectBtn = el("button", { style: "margin-left:6px" }, ["Reject"]);
          approveBtn.addEventListener("click", () => actOnMissedJob(f.id, "approve", row));
          rejectBtn.addEventListener("click", () => actOnMissedJob(f.id, "reject", row));
          actionsCell.appendChild(approveBtn);
          actionsCell.appendChild(rejectBtn);
          return row;
        })),
      ]);
      const c = document.getElementById("missed-jobs-table");
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("missed-jobs-table", e));
}

function actOnMissedJob(findingId, action, row) {
  row.style.opacity = "0.5";
  row.querySelectorAll("button").forEach((b) => (b.disabled = true));
  apiPost(`/api/v1/ops/missed-jobs/${findingId}/${action}?reviewed_by=operator`)
    .then(() => loadMissedJobs())
    .catch((e) => {
      row.style.opacity = "1";
      row.querySelectorAll("button").forEach((b) => (b.disabled = false));
      alert(`${action} failed: ${e.message}`);
    });
}

loadSchedulerResources();
loadHeartbeats();
loadSteps();
loadRuns();
loadFreshness();
loadIntegrityFindings();
loadMissedJobs();
