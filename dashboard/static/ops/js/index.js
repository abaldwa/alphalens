// dashboard/static/ops/js/index.js — Job Autoruns (Ops)
renderAppShell("ops", "index");

// Generic client-side column sort: click a <th data-key> to sort by it,
// click again to reverse. Rows here are always small (<=100), so no need
// for a server-side sort param — this just re-renders from the same data.
function sortRows(rows, key, dir) {
  const factor = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const va = a[key];
    const vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return 1; // nulls last regardless of direction
    if (vb == null) return -1;
    if (va < vb) return -1 * factor;
    if (va > vb) return 1 * factor;
    return 0;
  });
}

function sortableHeader(label, key, sortState, onSort) {
  const isActive = sortState.key === key;
  const arrow = isActive ? (sortState.dir === "asc" ? " ▲" : " ▼") : "";
  const th = el("th", { style: "cursor:pointer;user-select:none" }, [label + arrow]);
  th.addEventListener("click", () => {
    const nextDir = isActive && sortState.dir === "asc" ? "desc" : "asc";
    onSort(key, nextDir);
  });
  return th;
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
      el("th", {}, ["Next Scheduled Run"]), el("th", {}, ["Error"]), el("th", {}, ["Action"]),
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

loadHeartbeats();
loadSteps();
loadRuns();
