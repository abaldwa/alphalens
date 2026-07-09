"""
tests/unit/test_job_health_checks.py

Phase: A21 (Pipeline Health Checker)
Owner: Platform / QA

Tests datastore/health/checks.py::check_job_completeness against a
private in-memory DuckDB connection seeded with synthetic job_run_log
rows (never the real alphalens.duckdb).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.health.checks import check_job_completeness
from datastore.schema.create_normalised import create_schema


@pytest.fixture
def conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM job_run_log")


def _log_success(conn, job_id, d):
    conn.execute(
        "INSERT INTO job_run_log (job_id, status, recorded_at) VALUES (?, 'success', ?)",
        [job_id, f"{d.isoformat()} 10:00:00"],
    )


class TestCheckJobCompleteness:
    def test_flags_missing_saturday_run(self, conn):
        # Trailing 7 days from Sunday 2026-06-07 back to Monday 2026-06-01
        # includes exactly one Saturday (2026-06-06) — no success logged for it.
        findings = check_job_completeness(conn, date(2026, 6, 7), lookback_days=7)
        wfb = [f for f in findings if f.job_id == "weekend_feature_backfill"]
        assert len(wfb) == 1
        assert wfb[0].severity == "warning"
        assert wfb[0].proposed_catchup_action == "rerun_script"

    def test_no_finding_when_fully_populated(self, conn):
        for d in [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3), date(2026, 6, 4), date(2026, 6, 5)]:
            _log_success(conn, "daily_pipeline", d)
        _log_success(conn, "weekend_feature_backfill", date(2026, 6, 6))
        _log_success(conn, "weekend_fundamentals", date(2026, 6, 6))
        _log_success(conn, "nse_xbrl_fundamentals", date(2026, 6, 6))
        _log_success(conn, "mf_holdings_ingestion", date(2026, 6, 6))
        _log_success(conn, "multibagger_scoring", date(2026, 6, 7))
        _log_success(conn, "forensic_scoring", date(2026, 6, 7))
        for d in [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3), date(2026, 6, 4),
                  date(2026, 6, 5), date(2026, 6, 6), date(2026, 6, 7)]:
            _log_success(conn, "daily_backup", d)

        findings = check_job_completeness(conn, date(2026, 6, 7), lookback_days=7)
        assert findings == []

    def test_two_consecutive_misses_are_critical(self, conn):
        # No daily_pipeline success logged at all across the mon-fri window.
        findings = check_job_completeness(conn, date(2026, 6, 7), lookback_days=7)
        dp = [f for f in findings if f.job_id == "daily_pipeline"]
        assert len(dp) == 1
        assert dp[0].severity == "critical"

    def test_model_training_never_flagged(self, conn):
        # model_training is excluded from JOB_REGISTRY entirely, even
        # though it fires mon-fri like daily_pipeline and often 'skipped'.
        findings = check_job_completeness(conn, date(2026, 6, 7), lookback_days=7)
        assert not any(f.job_id == "model_training" for f in findings)
