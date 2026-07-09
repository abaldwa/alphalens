"""
tests/unit/test_integrity_findings.py

Phase: A20 (Data Integrity Checker)
Owner: Platform / QA

Tests datastore/integrity/findings.py against a private in-memory DuckDB
connection (never the real alphalens.duckdb — see feedback memory on
never inserting test rows into the real DB).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.integrity.findings import (
    Finding,
    approve_finding,
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
        c.execute("DELETE FROM data_integrity_findings")


def test_insert_finding_lands_as_pending(conn):
    finding = Finding(
        check_name="null_sweep",
        finding_date=date(2026, 7, 9),
        severity="warning",
        description="test finding",
        ticker="RELIANCE",
    )
    finding_id = insert_finding(conn, finding)
    assert finding_id > 0

    df = list_findings(conn)
    row = df[df["id"] == finding_id].iloc[0]
    assert row["status"] == "pending"
    assert row["check_name"] == "null_sweep"
    assert row["ticker"] == "RELIANCE"


def test_insert_finding_rejects_invalid_severity():
    with pytest.raises(ValueError):
        Finding(
            check_name="null_sweep",
            finding_date=date(2026, 7, 9),
            severity="oops",
            description="bad severity",
        )


def test_list_findings_filters_by_status_and_check_name(conn):
    insert_finding(conn, Finding("null_sweep", date(2026, 7, 9), "warning", "a"))
    fid2 = insert_finding(conn, Finding("holiday_leakage", date(2026, 7, 9), "critical", "b"))
    approve_finding(conn, fid2, reviewed_by="tester")

    pending = list_findings(conn, status="pending")
    assert set(pending["check_name"]) == {"null_sweep"}

    by_check = list_findings(conn, check_name="holiday_leakage")
    assert len(by_check) == 1
    assert by_check.iloc[0]["status"] == "approved"


def test_approve_finding_without_fix_marks_approved(conn):
    fid = insert_finding(conn, Finding("spot_check", date(2026, 7, 9), "critical", "no fix here"))
    approve_finding(conn, fid, reviewed_by="tester")

    row = list_findings(conn, status="approved")
    assert len(row) == 1
    assert row.iloc[0]["reviewed_by"] == "tester"
    assert row.iloc[0]["reviewed_at"] is not None


def test_approve_finding_with_fix_executes_sql_and_marks_applied(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS _integrity_test_target (ticker VARCHAR)")
    finding = Finding(
        check_name="corporate_actions",
        finding_date=date(2026, 7, 9),
        severity="critical",
        description="propose an insert",
        proposed_fix_sql="INSERT INTO _integrity_test_target (ticker) VALUES (?)",
        proposed_fix_params=["RELIANCE"],
    )
    fid = insert_finding(conn, finding)
    approve_finding(conn, fid, reviewed_by="tester")

    applied = list_findings(conn, status="applied")
    assert len(applied) == 1
    target_rows = conn.execute("SELECT * FROM _integrity_test_target").df()
    assert list(target_rows["ticker"]) == ["RELIANCE"]
    conn.execute("DROP TABLE _integrity_test_target")


def test_reject_finding_touches_no_data(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS _integrity_test_target2 (ticker VARCHAR)")
    finding = Finding(
        check_name="corporate_actions",
        finding_date=date(2026, 7, 9),
        severity="critical",
        description="propose an insert",
        proposed_fix_sql="INSERT INTO _integrity_test_target2 (ticker) VALUES (?)",
        proposed_fix_params=["TCS"],
    )
    fid = insert_finding(conn, finding)
    reject_finding(conn, fid, reviewed_by="tester")

    rejected = list_findings(conn, status="rejected")
    assert len(rejected) == 1
    target_rows = conn.execute("SELECT * FROM _integrity_test_target2").df()
    assert target_rows.empty
    conn.execute("DROP TABLE _integrity_test_target2")


def test_approve_non_pending_finding_raises(conn):
    fid = insert_finding(conn, Finding("null_sweep", date(2026, 7, 9), "info", "x"))
    approve_finding(conn, fid, reviewed_by="tester")
    with pytest.raises(ValueError):
        approve_finding(conn, fid, reviewed_by="tester2")
