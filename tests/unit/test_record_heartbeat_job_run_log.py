"""
tests/unit/test_record_heartbeat_job_run_log.py

Phase: A21 (Pipeline Health Checker) / A23 (Job benchmark history)
Owner: Platform / QA

Tests ingestion/scheduler/pipeline_scheduler.py::_record_heartbeat's
job_run_log write (added alongside its existing scheduler_heartbeats
upsert) — against a temp SQLite path and a private in-memory DuckDB
connection (never the real alphalens.duckdb / pipeline_log.db). Also
covers A23's duration_seconds/peak_rss_mb columns and the
_job_timer_start/_job_timer_stats helpers used to populate them.
"""

import pytest

import ingestion.scheduler.pipeline_scheduler as ps
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import create_schema
from datastore.schema.create_signals import create_scheduler_heartbeats_schema


@pytest.fixture
def sqlite_path(tmp_path):
    db_path = tmp_path / "pipeline_log.db"
    create_scheduler_heartbeats_schema(db_path=db_path)
    return db_path


@pytest.fixture
def duck_conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM job_run_log")


def test_record_heartbeat_writes_scheduler_heartbeat_and_job_run_log(sqlite_path, duck_conn, monkeypatch):
    monkeypatch.setattr("config.settings.DUCKDB_PATH", None)

    ps._record_heartbeat("weekend_feature_backfill", "success", db_path=sqlite_path)

    from datastore.api.db import get_sqlite_connection

    with get_sqlite_connection(sqlite_path) as conn:
        row = conn.execute(
            "SELECT job_id, last_status FROM scheduler_heartbeats WHERE job_id = ?",
            ("weekend_feature_backfill",),
        ).fetchone()
    assert row == ("weekend_feature_backfill", "success")

    log_rows = duck_conn.execute(
        "SELECT job_id, status FROM job_run_log WHERE job_id = ?", ["weekend_feature_backfill"]
    ).fetchall()
    assert log_rows == [("weekend_feature_backfill", "success")]


def test_record_heartbeat_writes_duration_and_peak_rss(sqlite_path, duck_conn, monkeypatch):
    """A23: duration_seconds/peak_rss_mb, when passed, land in job_run_log."""
    monkeypatch.setattr("config.settings.DUCKDB_PATH", None)

    ps._record_heartbeat(
        "daily_backup", "success", db_path=sqlite_path,
        duration_seconds=12.5, peak_rss_mb=256.0,
    )

    log_rows = duck_conn.execute(
        "SELECT job_id, duration_seconds, peak_rss_mb FROM job_run_log WHERE job_id = ?",
        ["daily_backup"],
    ).fetchall()
    assert log_rows == [("daily_backup", 12.5, 256.0)]


def test_record_heartbeat_leaves_duration_and_peak_rss_null_when_not_passed(sqlite_path, duck_conn, monkeypatch):
    """Callers that haven't been instrumented yet must not break the write — NULL, not an error."""
    monkeypatch.setattr("config.settings.DUCKDB_PATH", None)

    ps._record_heartbeat("daily_backup", "failed", "boom", db_path=sqlite_path)

    log_rows = duck_conn.execute(
        "SELECT job_id, duration_seconds, peak_rss_mb FROM job_run_log WHERE job_id = ?",
        ["daily_backup"],
    ).fetchall()
    assert log_rows == [("daily_backup", None, None)]


def test_job_timer_stats_measures_positive_duration_and_rss():
    """
    A23: _job_timer_start/_job_timer_stats is the pair every job wrapper
    uses to measure its own run. duration_seconds must reflect real
    elapsed time and peak_rss_mb must be a positive number (this test
    process itself has nonzero RSS).
    """
    import time as time_module

    start = ps._job_timer_start()
    time_module.sleep(0.01)
    duration_seconds, peak_rss_mb = ps._job_timer_stats(start)

    assert duration_seconds >= 0.01
    assert peak_rss_mb > 0


def test_record_heartbeat_survives_duckdb_write_failure(sqlite_path, monkeypatch):
    """A DuckDB write hiccup for job_run_log must never break the SQLite heartbeat write."""

    def _raise(*a, **k):
        raise RuntimeError("duckdb file locked")

    monkeypatch.setattr("datastore.api.db.get_duckdb_connection", _raise)

    ps._record_heartbeat("daily_backup", "success", db_path=sqlite_path)

    from datastore.api.db import get_sqlite_connection

    with get_sqlite_connection(sqlite_path) as conn:
        row = conn.execute(
            "SELECT job_id, last_status FROM scheduler_heartbeats WHERE job_id = ?",
            ("daily_backup",),
        ).fetchone()
    assert row == ("daily_backup", "success")
