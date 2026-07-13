"""
tests/unit/test_pipeline_scheduler_utils.py

A65 (2026-07-13) — coverage for a few of ingestion/scheduler/
pipeline_scheduler.py's standalone helper functions (previously 41.40%
covered, 744 stmts at the time of that measurement) that don't require a
live scheduler/systemd or the production DuckDB file:

- create_jobstore / create_scheduler: build real APScheduler objects
  against a tmp_path SQLite file — no live scheduler process involved.
- _job_timer_start / _job_timer_stats: pure timing/rusage helpers.
- _record_heartbeat: real SQLite (PIPELINE_LOG_DB_PATH) + DuckDB
  (DUCKDB_PATH) writes, both pointed at tmp_path fixtures via monkeypatch
  — never the production alphalens.duckdb file.

Deliberately NOT covered here (each requires a live scheduler/systemd
process, real network I/O, or actual model training/retraining, all out
of scope for a unit test per this session's charter): run_steps_for_date's
step-execution loop (already covered by tests/unit/test_scheduler.py),
run_startup_sequence/run_morning_catchup_sequence/run_backfill (ditto),
every _execute_*_job function (each is an APScheduler job target that
calls real scrapers/model-training code), and
_determine_groww_live_snapshot_month (live network scrape).
"""


import pytest
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from datastore.api.db import close_all_connections, get_duckdb_connection, get_sqlite_connection
from datastore.schema import create_normalised, create_signals
from ingestion.scheduler.pipeline_scheduler import (
    _job_timer_stats,
    _job_timer_start,
    _record_heartbeat,
    create_jobstore,
    create_scheduler,
)


class TestCreateJobstoreAndScheduler:
    def test_create_jobstore_builds_sqlalchemy_store_at_given_path(self, tmp_path):
        db_path = tmp_path / "nested" / "scheduler.db"
        store = create_jobstore(db_path)
        assert isinstance(store, SQLAlchemyJobStore)
        # create_jobstore must create the parent directory if missing.
        assert db_path.parent.exists()

    def test_create_jobstore_defaults_to_settings_scheduler_db_path(self, tmp_path, monkeypatch):
        import config.settings as settings_mod

        default_path = tmp_path / "default_scheduler.db"
        monkeypatch.setattr(settings_mod, "SCHEDULER_DB_PATH", default_path)
        store = create_jobstore()
        assert isinstance(store, SQLAlchemyJobStore)
        assert default_path.parent.exists()

    def test_create_scheduler_returns_unstarted_background_scheduler(self, tmp_path):
        scheduler = create_scheduler(tmp_path / "scheduler2.db")
        assert isinstance(scheduler, BackgroundScheduler)
        assert scheduler.running is False
        assert "default" in scheduler._jobstores


class TestJobTimer:
    def test_timer_stats_reports_nonnegative_duration_and_rss(self):
        start = _job_timer_start()
        # do a small amount of real work so duration is > 0
        total = sum(i * i for i in range(200_000))
        assert total > 0
        duration_seconds, peak_rss_mb = _job_timer_stats(start)
        assert duration_seconds >= 0.0
        assert peak_rss_mb > 0.0  # this test process itself has nonzero RSS

    def test_timer_stats_duration_increases_between_two_calls(self):
        start = _job_timer_start()
        d1, _ = _job_timer_stats(start)
        d2, _ = _job_timer_stats(start)
        assert d2 >= d1


@pytest.fixture
def heartbeat_dbs(tmp_path):
    pipeline_log_path = tmp_path / "pipeline_log.db"
    duckdb_path = tmp_path / "normalised_test.duckdb"
    create_signals.create_pipeline_runs_schema(db_path=pipeline_log_path)
    create_signals.create_scheduler_heartbeats_schema(db_path=pipeline_log_path)
    create_normalised.create_schema(db_path=duckdb_path)
    close_all_connections()
    return pipeline_log_path, duckdb_path


class TestRecordHeartbeat:
    def test_success_upserts_heartbeat_and_appends_job_run_log(self, heartbeat_dbs, monkeypatch):
        pipeline_log_path, duckdb_path = heartbeat_dbs
        import config.settings as settings_mod

        monkeypatch.setattr(settings_mod, "DUCKDB_PATH", duckdb_path)

        _record_heartbeat(
            "daily_pipeline", "success", db_path=pipeline_log_path,
            duration_seconds=12.5, peak_rss_mb=256.0,
        )

        with get_sqlite_connection(pipeline_log_path) as conn:
            row = conn.execute(
                "SELECT job_id, last_status, last_error, last_success_at IS NOT NULL "
                "FROM scheduler_heartbeats WHERE job_id = ?",
                ("daily_pipeline",),
            ).fetchone()
        assert row == ("daily_pipeline", "success", None, 1)

        with get_duckdb_connection(duckdb_path, persist=False, read_only=True) as conn:
            log_row = conn.execute(
                "SELECT job_id, status, duration_seconds, peak_rss_mb FROM job_run_log "
                "WHERE job_id = ?",
                ["daily_pipeline"],
            ).fetchone()
        assert log_row == ("daily_pipeline", "success", 12.5, 256.0)

    def test_failure_upsert_preserves_previous_last_success_at(self, heartbeat_dbs, monkeypatch):
        pipeline_log_path, duckdb_path = heartbeat_dbs
        import config.settings as settings_mod

        monkeypatch.setattr(settings_mod, "DUCKDB_PATH", duckdb_path)

        _record_heartbeat("backfill_catchup", "success", db_path=pipeline_log_path)
        with get_sqlite_connection(pipeline_log_path) as conn:
            first_success_at = conn.execute(
                "SELECT last_success_at FROM scheduler_heartbeats WHERE job_id = ?",
                ("backfill_catchup",),
            ).fetchone()[0]
        assert first_success_at is not None

        _record_heartbeat(
            "backfill_catchup", "failed", error="connection refused", db_path=pipeline_log_path,
        )
        with get_sqlite_connection(pipeline_log_path) as conn:
            row = conn.execute(
                "SELECT last_status, last_error, last_success_at FROM scheduler_heartbeats "
                "WHERE job_id = ?",
                ("backfill_catchup",),
            ).fetchone()
        assert row[0] == "failed"
        assert row[1] == "connection refused"
        # COALESCE keeps the prior success timestamp on a failed attempt.
        assert row[2] == first_success_at

    def test_sqlite_write_failure_is_swallowed_not_raised(self, tmp_path, monkeypatch):
        import config.settings as settings_mod

        # A nonexistent directory with no pipeline_checkpoints/scheduler_heartbeats
        # schema at all -> the SQLite write inside _record_heartbeat raises,
        # which must be caught and logged, never propagated.
        bogus_db_path = tmp_path / "does_not_exist_dir" / "pipeline_log.db"
        monkeypatch.setattr(settings_mod, "DUCKDB_PATH", tmp_path / "no_such.duckdb")

        _record_heartbeat("daily_pipeline", "success", db_path=bogus_db_path)  # must not raise
