"""
tests/unit/test_job_health_runner.py

Phase: A21 (Pipeline Health Checker)
Owner: Platform / QA

Tests datastore/health/runner.py's orchestration against a private
in-memory DuckDB connection (never the real alphalens.duckdb).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.health.findings import Finding, list_findings
from datastore.health.runner import run_job_health_check
from datastore.schema.create_normalised import create_schema


@pytest.fixture
def conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM missed_job_findings")
        c.execute("DELETE FROM job_run_log")


def test_run_job_health_check_writes_findings_and_counts_critical(conn, monkeypatch):
    def fake_check(conn, as_of_date):
        return [
            Finding("weekend_feature_backfill", as_of_date, "warning", "one miss"),
            Finding("daily_pipeline", as_of_date, "critical", "two misses"),
        ]

    import datastore.health.runner as runner_mod

    monkeypatch.setitem(runner_mod._CHECKS, "job_completeness", fake_check)

    as_of = date(2026, 6, 7)
    result = run_job_health_check(conn, as_of)

    assert result.findings_by_check == {"job_completeness": 2}
    assert result.total_findings == 2
    assert result.critical_count == 1

    stored = list_findings(conn)
    assert len(stored) == 2
    assert set(stored["status"]) == {"pending"}


def test_a_failing_check_does_not_take_down_the_others(conn, monkeypatch):
    def fake_check_raises(conn, as_of_date):
        raise RuntimeError("job_run_log query failed")

    import datastore.health.runner as runner_mod

    monkeypatch.setitem(runner_mod._CHECKS, "job_completeness", fake_check_raises)

    result = run_job_health_check(conn, date(2026, 6, 7))
    assert result.findings_by_check["job_completeness"] == 0
    assert result.total_findings == 0
