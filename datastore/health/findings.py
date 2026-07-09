"""
datastore/health/findings.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler
Consumers: datastore/health/checks.py, datastore/health/runner.py,
    datastore/api/routers/ops.py

Read/write API for the `missed_job_findings` table (DDL in
datastore/schema/create_normalised.py). Mirrors
datastore/integrity/findings.py's shape exactly, swapping "proposed SQL
fix" for "proposed catch-up action" — a missed job isn't a bad row to
correct, it's work that never happened, so approving a finding here
triggers a catch-up RUN (datastore/health/catchup.py) instead of
executing SQL. Findings always land as status='pending' — the only way
to move one to 'applied' is an explicit approve_finding() call
(human-in-the-loop), matching A12/A20/A25's "flag, don't silently write"
discipline. A rejected finding stays visible as status='rejected' for
audit history, never deleted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

_VALID_SEVERITIES = {"info", "warning", "critical"}


@dataclass
class Finding:
    job_id: str
    missed_date: date_type
    severity: str
    description: str
    proposed_catchup_action: Optional[str] = None
    proposed_catchup_params: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(f"severity must be one of {_VALID_SEVERITIES}, got {self.severity!r}")


def insert_finding(conn, finding: Finding) -> int:
    """Insert `finding` as status='pending'. Returns the new row's id."""
    row = conn.execute(
        """
        INSERT INTO missed_job_findings
            (job_id, missed_date, severity, description,
             proposed_catchup_action, proposed_catchup_params_json, status)
        VALUES (?, ?, ?, ?, ?, ?, 'pending')
        RETURNING id
        """,
        [
            finding.job_id,
            finding.missed_date,
            finding.severity,
            finding.description,
            finding.proposed_catchup_action,
            json.dumps(finding.proposed_catchup_params, default=str)
            if finding.proposed_catchup_params is not None
            else None,
        ],
    ).fetchone()
    return int(row[0])


def list_findings(
    conn,
    status: Optional[str] = None,
    job_id: Optional[str] = None,
) -> pd.DataFrame:
    """List findings, optionally filtered by status and/or job_id, most recent first."""
    clauses = []
    params: List[Any] = []
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    if job_id is not None:
        clauses.append("job_id = ?")
        params.append(job_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"SELECT * FROM missed_job_findings {where} ORDER BY created_at DESC",
        params,
    ).df()


def _load_pending(conn, finding_id: int) -> tuple:
    row = conn.execute(
        "SELECT status, job_id, missed_date, proposed_catchup_action, proposed_catchup_params_json "
        "FROM missed_job_findings WHERE id = ?",
        [finding_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"no finding with id={finding_id}")
    status, job_id, missed_date, action, params_json = row
    if status != "pending":
        raise ValueError(f"finding id={finding_id} is not pending (status={status!r})")
    return job_id, missed_date, action, params_json


def begin_approve(conn, finding_id: int) -> tuple:
    """
    Validate that `finding_id` is pending and return
    `(job_id, missed_date, action, params)` for the caller to dispatch via
    datastore.health.catchup.run_catchup — read-only, does NOT write any
    status change. Split out from approve_finding so a caller that needs
    to dispatch a long-running catch-up (e.g. a weekend script that can
    run for hours) doesn't have to hold a DuckDB write connection open
    for the whole duration — see complete_approve for the matching write
    step, and datastore/api/routers/ops.py::approve_missed_job_finding
    for the two-phase caller.
    """
    job_id, missed_date, action, params_json = _load_pending(conn, finding_id)
    params = json.loads(params_json) if params_json else {}
    return job_id, missed_date, action, params


def complete_approve(conn, finding_id: int, new_status: str, reviewed_by: str) -> None:
    """Write the final status ('applied' or 'approved') after begin_approve + (optionally) run_catchup."""
    conn.execute(
        "UPDATE missed_job_findings SET status = ?, reviewed_by = ?, reviewed_at = current_timestamp WHERE id = ?",
        [new_status, reviewed_by, finding_id],
    )


def approve_finding(conn, finding_id: int, reviewed_by: str) -> None:
    """
    Approve a pending finding in one call: if it has a
    `proposed_catchup_action`, dispatch it via
    datastore.health.catchup.run_catchup (executes the actual catch-up
    job/script) and mark status='applied'; if it has no proposed action,
    just mark status='approved'. Either way this is the ONLY code path
    that triggers a catch-up run on A21's behalf — never automatic.

    Convenience wrapper around begin_approve/complete_approve for callers
    that don't need to release the connection during dispatch (tests, a
    synchronous CLI). The Ops HTTP endpoint uses begin_approve/
    complete_approve directly instead, since holding a single DuckDB
    connection open for a possibly-hours-long catch-up would lock the
    whole database for that entire time.
    """
    from datastore.health.catchup import run_catchup

    job_id, missed_date, action, params = begin_approve(conn, finding_id)
    if action:
        run_catchup(action, job_id, missed_date, params)
        new_status = "applied"
    else:
        new_status = "approved"
    complete_approve(conn, finding_id, new_status, reviewed_by)


def reject_finding(conn, finding_id: int, reviewed_by: str) -> None:
    """Reject a pending finding. No catch-up run is triggered."""
    _load_pending(conn, finding_id)
    conn.execute(
        "UPDATE missed_job_findings SET status = 'rejected', reviewed_by = ?, reviewed_at = current_timestamp WHERE id = ?",
        [reviewed_by, finding_id],
    )
