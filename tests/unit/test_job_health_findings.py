"""
tests/unit/test_job_health_findings.py

Phase: A21 (Pipeline Health Checker)
Owner: Platform / QA

Tests datastore/health/findings.py against a private in-memory DuckDB
connection (never the real alphalens.duckdb).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.health.findings import (
    Finding,
    approve_finding,
    begin_approve,
    complete_approve,
    insert_finding,
    list_findings,
    reject_finding,
)
from datastore.schema.create_normalised import create_schema


@pytest.fixture
def conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM missed_job_findings")


def test_insert_finding_lands_as_pending(conn):
    finding = Finding(
        job_id="weekend_feature_backfill",
        missed_date=date(2026, 6, 6),
        severity="warning",
        description="missed last Saturday",
    )
    finding_id = insert_finding(conn, finding)
    assert finding_id > 0

    df = list_findings(conn)
    row = df[df["id"] == finding_id].iloc[0]
    assert row["status"] == "pending"
    assert row["job_id"] == "weekend_feature_backfill"


def test_insert_finding_rejects_invalid_severity():
    with pytest.raises(ValueError):
        Finding(job_id="x", missed_date=date(2026, 6, 6), severity="oops", description="bad")


def test_list_findings_filters_by_status_and_job_id(conn):
    insert_finding(conn, Finding("weekend_feature_backfill", date(2026, 6, 6), "warning", "a"))
    fid2 = insert_finding(conn, Finding("daily_pipeline", date(2026, 6, 6), "critical", "b"))
    complete_approve(conn, fid2, "approved", reviewed_by="tester")

    pending = list_findings(conn, status="pending")
    assert set(pending["job_id"]) == {"weekend_feature_backfill"}

    by_job = list_findings(conn, job_id="daily_pipeline")
    assert len(by_job) == 1
    assert by_job.iloc[0]["status"] == "approved"


def test_approve_finding_without_action_marks_approved(conn):
    fid = insert_finding(conn, Finding("daily_backup", date(2026, 6, 6), "info", "no catchup registered"))
    approve_finding(conn, fid, reviewed_by="tester")

    row = list_findings(conn, status="approved")
    assert len(row) == 1
    assert row.iloc[0]["reviewed_by"] == "tester"
    assert row.iloc[0]["reviewed_at"] is not None


def test_approve_finding_with_action_dispatches_and_marks_applied(conn, monkeypatch):
    calls = []

    def fake_run_catchup(action, job_id, missed_date, params):
        calls.append((action, job_id, missed_date, params))

    import datastore.health.catchup as catchup_mod

    monkeypatch.setattr(catchup_mod, "run_catchup", fake_run_catchup)

    finding = Finding(
        job_id="weekend_feature_backfill",
        missed_date=date(2026, 6, 6),
        severity="warning",
        description="missed Saturday",
        proposed_catchup_action="rerun_script",
        proposed_catchup_params={"script": "scripts/feature_backfill_hybrid.py", "args": []},
    )
    fid = insert_finding(conn, finding)
    approve_finding(conn, fid, reviewed_by="tester")

    applied = list_findings(conn, status="applied")
    assert len(applied) == 1
    assert len(calls) == 1
    assert calls[0][0] == "rerun_script"
    assert calls[0][1] == "weekend_feature_backfill"


def test_begin_approve_does_not_write_status(conn):
    finding = Finding(
        job_id="daily_pipeline",
        missed_date=date(2026, 6, 6),
        severity="critical",
        description="missed",
        proposed_catchup_action="force_run_daily_pipeline",
        proposed_catchup_params={"missed_dates": ["2026-06-06"]},
    )
    fid = insert_finding(conn, finding)
    job_id, missed_date, action, params = begin_approve(conn, fid)
    assert job_id == "daily_pipeline"
    assert action == "force_run_daily_pipeline"

    still_pending = list_findings(conn, status="pending")
    assert len(still_pending) == 1


def test_reject_finding_triggers_no_catchup(conn, monkeypatch):
    calls = []
    import datastore.health.catchup as catchup_mod

    monkeypatch.setattr(catchup_mod, "run_catchup", lambda *a, **k: calls.append(a))

    finding = Finding(
        job_id="weekend_feature_backfill",
        missed_date=date(2026, 6, 6),
        severity="warning",
        description="missed",
        proposed_catchup_action="rerun_script",
        proposed_catchup_params={"script": "x.py", "args": []},
    )
    fid = insert_finding(conn, finding)
    reject_finding(conn, fid, reviewed_by="tester")

    rejected = list_findings(conn, status="rejected")
    assert len(rejected) == 1
    assert calls == []


def test_approve_non_pending_finding_raises(conn):
    fid = insert_finding(conn, Finding("daily_backup", date(2026, 6, 6), "info", "x"))
    approve_finding(conn, fid, reviewed_by="tester")
    with pytest.raises(ValueError):
        approve_finding(conn, fid, reviewed_by="tester2")
