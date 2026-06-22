"""
ingestion/scheduler/gap_detector.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-003, SPEC-SCHED-004, SPEC-SCHED-005, SPEC-SCHED-008
Owner: Platform / Scheduler
Consumers: ingestion/scheduler/pipeline_scheduler

Finds every NSE trading day missed since the last successful pipeline run.
No maximum gap window (SPEC-SCHED-003) — one missed day or a hundred are
handled identically. Weekends and NSE holidays (config/nse_holidays.py,
SPEC-SCHED-008) are never treated as gaps.

Out of scope here: SPEC-SCHED-009 (laptop-only operation; NSE-archive
sourcing during backfill) is a data-fetching concern for each gap date's
step_runner, not a gap-*detection* concern — this module only identifies
which dates are missing, never how their data gets fetched.
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

from config.nse_holidays import is_nse_holiday
from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection

logger = logging.getLogger(__name__)


def is_trading_day(check_date: date) -> bool:
    """
    Return whether `check_date` is an NSE trading day.

    Parameters
    ----------
    check_date : date

    Returns
    -------
    bool
        True iff check_date is a weekday and not a declared NSE holiday.

    Spec References
    ----------------
    SPEC-SCHED-008: NSE holiday calendar excludes holidays from gap
        detection — no backfill is attempted for them.

    PIT Assumptions
    ----------------
    None — this is a static calendar lookup.

    Raises
    ------
    None
    """
    return check_date.weekday() < 5 and not is_nse_holiday(check_date)


def get_last_successful_run_date(db_path: Optional[Path] = None) -> Optional[date]:
    """
    Look up the most recent successful pipeline_runs date.

    Parameters
    ----------
    db_path : Path, optional
        Path to the pipeline log SQLite file. If None, uses
        config.settings.PIPELINE_LOG_DB_PATH.

    Returns
    -------
    date or None
        None if no successful run has ever been recorded (first run), or
        if pipeline_runs does not exist yet (fresh install).

    Spec References
    ----------------
    SPEC-SCHED-005: pipeline_runs is the source of truth for gap detection.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    if db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH

        db_path = PIPELINE_LOG_DB_PATH

    try:
        with get_sqlite_connection(db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM pipeline_runs WHERE status = 'success'"
            ).fetchone()
    except Exception as exc:
        logger.warning(f"Could not read pipeline_runs for gap detection: {exc}")
        return None

    if row is None or row[0] is None:
        return None
    return date.fromisoformat(row[0])


def detect_gaps(
    last_run_date: Optional[date] = None,
    today: Optional[date] = None,
    db_path: Optional[Path] = None,
) -> List[date]:
    """
    Return every NSE trading day missed since the last successful run.

    The window is (last_run_date, today), exclusive of both ends: today
    itself is excluded because it is handled by the normal (non-backfill)
    pipeline run, not backfill — SPEC-SCHED-006 reserves model inference
    for today only, so today must never be folded into the backfill list.

    Parameters
    ----------
    last_run_date : date, optional
        Last successful run date. If None, looked up from pipeline_runs
        via `db_path` (SPEC-SCHED-005). If no successful run has ever been
        recorded, returns [] — first run, nothing to backfill.
    today : date, optional
        Reference "today". Defaults to now_ist().date() (IST, never UTC
        or naive OS-local time); exposed as a parameter for testability.
    db_path : Path, optional
        pipeline_runs SQLite path, used only when last_run_date is None.

    Returns
    -------
    list of date
        Missed NSE trading dates, ascending — oldest first (SPEC-SCHED-004).
        Empty list if there is no gap (or no run history yet).

    Spec References
    ----------------
    SPEC-SCHED-003: no maximum gap window.
    SPEC-SCHED-004: chronological order, oldest first; never skip/reorder.
    SPEC-SCHED-008: NSE holidays excluded from the gap list.

    PIT Assumptions
    ----------------
    None — this enumerates calendar dates only; PIT correctness for the
    data fetched on each gap date is enforced downstream during backfill
    (SPEC-SCHED-004: "features use only data as-of that gap day").

    Raises
    ------
    None
    """
    today = today or now_ist().date()

    if last_run_date is None:
        last_run_date = get_last_successful_run_date(db_path)
        if last_run_date is None:
            logger.info("No pipeline history — first run, nothing to backfill")
            return []

    gaps: List[date] = []
    cursor = last_run_date + timedelta(days=1)
    while cursor < today:
        if is_trading_day(cursor):
            gaps.append(cursor)
        cursor += timedelta(days=1)

    if gaps:
        logger.warning(
            f"GAPS DETECTED: {len(gaps)} trading days missed ({gaps[0]} to {gaps[-1]})"
        )
    else:
        logger.info("No gaps detected")

    return gaps
