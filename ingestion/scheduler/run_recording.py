"""
ingestion/scheduler/run_recording.py

Run lifecycle recording for the daily pipeline: pipeline_runs INSERT/UPDATE,
scheduler_heartbeats upsert, and job timing utilities.

Extracted from pipeline_scheduler.py (A46 — per-concern module split).

Consumers: pipeline_startup.py, scheduler_jobs.py (also re-exported via
           pipeline_scheduler.py for backward compat)
"""

import logging
import resource
import time
from datetime import date as date_type
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection

logger = logging.getLogger(__name__)

_INSERT_PIPELINE_RUN = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, ?, ?, ?, ?)
"""

_INSERT_PIPELINE_RUN_STARTED = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, NULL, 'running', 0, NULL)
"""

_UPDATE_PIPELINE_RUN_FINISHED = """
    UPDATE pipeline_runs
    SET completed_at = ?, status = ?, error_message = ?
    WHERE run_id = ?
"""

_UPSERT_SCHEDULER_HEARTBEAT = """
    INSERT INTO scheduler_heartbeats (job_id, last_attempt_at, last_status, last_error, last_success_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(job_id) DO UPDATE SET
        last_attempt_at = excluded.last_attempt_at,
        last_status = excluded.last_status,
        last_error = excluded.last_error,
        last_success_at = COALESCE(excluded.last_success_at, scheduler_heartbeats.last_success_at)
"""


def _record_pipeline_run_started(
    run_date: date_type,
    started_at: datetime,
    db_path: Optional[Path] = None,
) -> int:
    """
    Insert the 'running' row for a pipeline_runs invocation as it begins.

    Returns the new row's run_id (SQLite ROWID), which should be passed to
    _record_pipeline_run so the same row is UPDATEd in place on completion.
    """
    if db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH
        db_path = PIPELINE_LOG_DB_PATH

    with get_sqlite_connection(db_path) as conn:
        cursor = conn.execute(
            _INSERT_PIPELINE_RUN_STARTED,
            (run_date.isoformat(), started_at.isoformat()),
        )
        conn.commit()
        return cursor.lastrowid


def _record_pipeline_run(
    run_date: date_type,
    success: bool,
    started_at: datetime,
    db_path: Optional[Path] = None,
    run_id: Optional[int] = None,
) -> None:
    """Write the whole-day summary row to pipeline_runs (SPEC-SCHED-005)."""
    if db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH
        db_path = PIPELINE_LOG_DB_PATH

    status = "success" if success else "failed"
    with get_sqlite_connection(db_path) as conn:
        if run_id is not None:
            conn.execute(
                _UPDATE_PIPELINE_RUN_FINISHED,
                (now_ist().isoformat(), status, None, run_id),
            )
        else:
            conn.execute(
                _INSERT_PIPELINE_RUN,
                (
                    run_date.isoformat(),
                    started_at.isoformat(),
                    now_ist().isoformat(),
                    status,
                    0,
                    None,
                ),
            )
        conn.commit()


def _job_timer_start() -> float:
    """A23: call at the top of a job-runner's try block; pair with _job_timer_stats."""
    return time.monotonic()


def _job_timer_stats(start: float) -> tuple:
    """A23: (duration_seconds, peak_rss_mb) since `start`."""
    duration_seconds = time.monotonic() - start
    self_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    peak_rss_mb = round((self_kb + children_kb) / 1024, 1)
    return duration_seconds, peak_rss_mb


def _record_heartbeat(
    job_id: str,
    status: str,
    error: Optional[str] = None,
    db_path: Optional[Path] = None,
    duration_seconds: Optional[float] = None,
    peak_rss_mb: Optional[float] = None,
) -> None:
    """
    Upsert scheduler_heartbeats for one recurring job (SPEC-SCHED-013).
    Also appends to job_run_log (DuckDB) for health-check queries.
    """
    if db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH
        db_path = PIPELINE_LOG_DB_PATH

    now_iso = now_ist().isoformat()
    try:
        with get_sqlite_connection(db_path) as conn:
            conn.execute(
                _UPSERT_SCHEDULER_HEARTBEAT,
                (job_id, now_iso, status, error, now_iso if status == "success" else None),
            )
            conn.commit()
    except Exception as exc:
        logger.warning(f"Could not record scheduler heartbeat for '{job_id}': {exc}")

    # Also append to job_run_log (DuckDB) for health-check queries.
    try:
        from config.settings import DUCKDB_PATH
        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(DUCKDB_PATH, persist=False) as duck_conn:
            duck_conn.execute(
                "INSERT INTO job_run_log (job_id, status, error, duration_seconds, peak_rss_mb) "
                "VALUES (?, ?, ?, ?, ?)",
                [job_id, status, error, duration_seconds, peak_rss_mb],
            )
    except Exception as exc:
        logger.warning(f"Could not record job_run_log entry for '{job_id}': {exc}")