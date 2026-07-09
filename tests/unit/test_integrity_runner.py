"""
tests/unit/test_integrity_runner.py

Phase: A20 (Data Integrity Checker)
Owner: Platform / QA

Tests datastore/integrity/runner.py's orchestration against a private
in-memory DuckDB connection (never the real alphalens.duckdb).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.integrity.findings import Finding, list_findings
from datastore.integrity.runner import run_integrity_checks
from datastore.schema.create_normalised import create_schema


@pytest.fixture
def conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM data_integrity_findings")
        c.execute("DELETE FROM ohlcv_adjusted")


def test_run_integrity_checks_writes_findings_and_counts_critical(conn, monkeypatch):
    def fake_check_ok(conn, as_of_date):
        return []

    def fake_check_warns(conn, as_of_date):
        return [Finding("null_sweep", as_of_date, "warning", "some warning")]

    def fake_check_critical(conn, as_of_date):
        return [Finding("holiday_leakage", as_of_date, "critical", "some critical issue")]

    import datastore.integrity.runner as runner_mod

    monkeypatch.setitem(runner_mod._CHECKS, "corporate_actions", fake_check_ok)
    monkeypatch.setitem(runner_mod._CHECKS, "null_sweep", fake_check_warns)
    monkeypatch.setitem(runner_mod._CHECKS, "holiday_leakage", fake_check_critical)
    monkeypatch.setitem(runner_mod._CHECKS, "spot_check", fake_check_ok)

    as_of = date(2026, 6, 1)
    result = run_integrity_checks(conn, as_of)

    assert result.findings_by_check == {
        "corporate_actions": 0,
        "null_sweep": 1,
        "holiday_leakage": 1,
        "spot_check": 0,
    }
    assert result.total_findings == 2
    assert result.critical_count == 1

    stored = list_findings(conn)
    assert len(stored) == 2
    assert set(stored["status"]) == {"pending"}


def test_a_failing_check_does_not_take_down_the_others(conn, monkeypatch):
    def fake_check_raises(conn, as_of_date):
        raise RuntimeError("upstream Fyers outage")

    def fake_check_ok(conn, as_of_date):
        return []

    import datastore.integrity.runner as runner_mod

    monkeypatch.setitem(runner_mod._CHECKS, "corporate_actions", fake_check_raises)
    monkeypatch.setitem(runner_mod._CHECKS, "null_sweep", fake_check_ok)
    monkeypatch.setitem(runner_mod._CHECKS, "holiday_leakage", fake_check_ok)
    monkeypatch.setitem(runner_mod._CHECKS, "spot_check", fake_check_ok)

    result = run_integrity_checks(conn, date(2026, 6, 1))
    assert result.findings_by_check["corporate_actions"] == 0
    assert result.total_findings == 0
