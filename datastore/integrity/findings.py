"""
datastore/integrity/findings.py

Phase: A20 (Data Integrity Checker)
Specs: FeatureBacklog.md A20
Owner: Data Layer / Ops / Scheduler
Consumers: datastore/integrity/checks.py, datastore/integrity/runner.py,
    datastore/api/routers/ops.py

Read/write API for the `data_integrity_findings` table (DDL in
datastore/schema/create_normalised.py). A finding is a check's evidence
that something in production data looks wrong, plus an optional proposed
fix. Findings always land as status='pending' — the only way to move a
finding to 'applied' is an explicit approve_finding() call (human-in-the-
loop), matching this project's existing "flag, don't silently write"
discipline (A12's null-flagging, A25's staging.rejected_rows). A rejected
finding is not deleted — it stays visible as status='rejected' for audit
history, same as staging.rejected_rows never deletes rejected rows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

_VALID_SEVERITIES = {"info", "warning", "critical"}
_VALID_STATUSES = {"pending", "approved", "rejected", "applied"}


@dataclass
class Finding:
    check_name: str
    finding_date: date_type
    severity: str
    description: str
    ticker: Optional[str] = None
    evidence: Optional[Dict[str, Any]] = None
    proposed_fix_sql: Optional[str] = None
    proposed_fix_params: Optional[List[Any]] = None

    def __post_init__(self) -> None:
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(f"severity must be one of {_VALID_SEVERITIES}, got {self.severity!r}")


def insert_finding(conn, finding: Finding) -> int:
    """Insert `finding` as status='pending'. Returns the new row's id."""
    row = conn.execute(
        """
        INSERT INTO data_integrity_findings
            (check_name, ticker, finding_date, severity, description,
             evidence_json, proposed_fix_sql, proposed_fix_params_json, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        RETURNING id
        """,
        [
            finding.check_name,
            finding.ticker,
            finding.finding_date,
            finding.severity,
            finding.description,
            json.dumps(finding.evidence, default=str) if finding.evidence is not None else None,
            finding.proposed_fix_sql,
            json.dumps(finding.proposed_fix_params, default=str)
            if finding.proposed_fix_params is not None
            else None,
        ],
    ).fetchone()
    return int(row[0])


def list_findings(
    conn,
    status: Optional[str] = None,
    check_name: Optional[str] = None,
) -> pd.DataFrame:
    """List findings, optionally filtered by status and/or check_name, most recent first."""
    clauses = []
    params: List[Any] = []
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    if check_name is not None:
        clauses.append("check_name = ?")
        params.append(check_name)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"SELECT * FROM data_integrity_findings {where} ORDER BY created_at DESC",
        params,
    ).df()


def _load_pending(conn, finding_id: int) -> tuple:
    row = conn.execute(
        "SELECT status, proposed_fix_sql, proposed_fix_params_json FROM data_integrity_findings WHERE id = ?",
        [finding_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"no finding with id={finding_id}")
    status, fix_sql, fix_params_json = row
    if status != "pending":
        raise ValueError(f"finding id={finding_id} is not pending (status={status!r})")
    return fix_sql, fix_params_json


def approve_finding(conn, finding_id: int, reviewed_by: str) -> None:
    """
    Approve a pending finding: if it has a `proposed_fix_sql`, execute it
    (with `proposed_fix_params_json` as parameters) and mark status='applied';
    if it has no proposed fix (e.g. an informational finding), just mark
    status='approved'. Either way this is the only code path that writes
    production data on A20's behalf — never called automatically.
    """
    fix_sql, fix_params_json = _load_pending(conn, finding_id)
    if fix_sql:
        params = json.loads(fix_params_json) if fix_params_json else []
        conn.execute(fix_sql, params)
        new_status = "applied"
    else:
        new_status = "approved"
    conn.execute(
        "UPDATE data_integrity_findings SET status = ?, reviewed_by = ?, reviewed_at = current_timestamp WHERE id = ?",
        [new_status, reviewed_by, finding_id],
    )


def reject_finding(conn, finding_id: int, reviewed_by: str) -> None:
    """Reject a pending finding. No production data is touched."""
    _load_pending(conn, finding_id)
    conn.execute(
        "UPDATE data_integrity_findings SET status = 'rejected', reviewed_by = ?, reviewed_at = current_timestamp WHERE id = ?",
        [reviewed_by, finding_id],
    )
