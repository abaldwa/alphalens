"""
ingestion/quality/structured_logger.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-OBS-003, SPEC-SEC-001
Owner: Platform / Observability
Consumers: ingestion/scheduler/daily_pipeline, ingestion/scheduler/checkpoint

log_pipeline_step(): one JSON line per pipeline step outcome, written to
logs/pipeline_YYYY-MM-DD.jsonl (one file per trading day — SPEC-OBS-003's
"Log rotation: daily, 30-day retention" implemented as one file per day
plus prune_old_logs() rather than a single rotated file, so a partial day's
log is trivially greppable on its own).

Dates here are IST (config.timezone.now_ist()), not UTC — this module
previously named/pruned log files by datetime.now(timezone.utc).date(),
which silently disagreed with IST for the ~5.5 hours/day the two are on
different calendar dates (caught when the system clock crossed a date
boundary mid-session: the file the logger wrote didn't match the file the
test expected). See config/timezone.py's module docstring.

SPEC-SEC-001 ("Never logs raw financial data values"): the function
signature only accepts scalar step/status/stocks/duration/error fields —
there is no way to pass a DataFrame, price array, or similar through this
API, by construction rather than by convention. Gated through
config.observability so the master switch (SPEC-OBS-001) and level
(SPEC-OBS-002) apply here exactly as everywhere else in the system.
"""

import json
import logging
from datetime import date as date_type
from numbers import Number
from pathlib import Path
from typing import Optional

from config.observability import is_enabled, should_log
from config.settings import LOGS_DIR, OBSERVABILITY_LOG_RETENTION_DAYS
from config.timezone import now_ist

logger = logging.getLogger(__name__)

# Matches ingestion/scheduler/checkpoint.py's _VALID_STATUSES — one home for
# pipeline status vocabulary would require checkpoint.py to import this
# module (or vice versa) and create a cross-layer dependency neither needs;
# duplicated here as the small, stable contract it is.
_VALID_STATUSES = {"running", "success", "failed", "skipped"}

_LOG_FILENAME_PREFIX = "pipeline_"
_LOG_FILENAME_SUFFIX = ".jsonl"


def _log_path_for(run_date: date_type) -> Path:
    return LOGS_DIR / f"{_LOG_FILENAME_PREFIX}{run_date.isoformat()}{_LOG_FILENAME_SUFFIX}"


def log_pipeline_step(
    step: str,
    status: str,
    stocks: int,
    duration_s: float,
    error: Optional[str] = None,
) -> None:
    """
    Append one structured JSON-line event for a pipeline step's outcome.

    Parameters
    ----------
    step : str
        Step name (e.g. one of ingestion.scheduler.checkpoint.STEP_NAMES).
    status : str
        One of 'running', 'success', 'failed', 'skipped'.
    stocks : int
        Count of stocks/rows processed by this step. A count, never the
        underlying values (SPEC-SEC-001).
    duration_s : float
        Step duration in seconds.
    error : str, optional
        Error message when status='failed'. Must be a string (or None) —
        never an exception object or raw data structure.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-OBS-003: structured JSON-line logging, one event per step.
    SPEC-SEC-001: never logs raw financial data values — enforced by this
    function's scalar-only signature and the type checks below.

    PIT Assumptions
    ----------------
    None — this is operational metadata, not market data.

    Raises
    ------
    ValueError
        If status is not one of _VALID_STATUSES.
    TypeError
        If stocks/duration_s are not numbers, or error is not None/str
        (guards against accidentally passing a DataFrame/array — SPEC-SEC-001).
    """
    if status not in _VALID_STATUSES:
        raise ValueError(f"Unknown status '{status}'. Must be one of {sorted(_VALID_STATUSES)}")
    if not isinstance(stocks, Number) or isinstance(stocks, bool):
        raise TypeError(f"stocks must be a number, got {type(stocks).__name__}")
    if not isinstance(duration_s, Number) or isinstance(duration_s, bool):
        raise TypeError(f"duration_s must be a number, got {type(duration_s).__name__}")
    if error is not None and not isinstance(error, str):
        raise TypeError(f"error must be None or str, got {type(error).__name__}")

    # SPEC-OBS-001: master switch — zero overhead, not even a file stat, when disabled.
    if not is_enabled():
        return

    # SPEC-OBS-002: failures always get through at any level above 'off';
    # routine step start/complete events require at least 'info'.
    event_level = "error" if status == "failed" else "info"
    if not should_log(event_level):
        return

    now = now_ist()
    event = {
        "event_type": "pipeline_step",
        "step": step,
        "status": status,
        "stocks_processed": stocks,
        "duration_seconds": duration_s,
        "error": error,
        "timestamp": now.isoformat(),
    }

    log_path = _log_path_for(now.date())
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as f:
        f.write(json.dumps(event) + "\n")


def prune_old_logs(retention_days: int = OBSERVABILITY_LOG_RETENTION_DAYS) -> int:
    """
    Delete pipeline_YYYY-MM-DD.jsonl files older than retention_days.

    Parameters
    ----------
    retention_days : int
        Defaults to config.settings.OBSERVABILITY_LOG_RETENTION_DAYS (30).

    Returns
    -------
    int
        Number of files deleted.

    Spec References
    ----------------
    SPEC-OBS-003: "Log rotation: daily, 30-day retention."

    Raises
    ------
    None
    """
    if not LOGS_DIR.exists():
        return 0

    cutoff = now_ist().date()
    deleted = 0
    for path in LOGS_DIR.glob(f"{_LOG_FILENAME_PREFIX}*{_LOG_FILENAME_SUFFIX}"):
        date_str = path.name[len(_LOG_FILENAME_PREFIX):-len(_LOG_FILENAME_SUFFIX)]
        try:
            file_date = date_type.fromisoformat(date_str)
        except ValueError:
            continue
        if (cutoff - file_date).days > retention_days:
            path.unlink()
            deleted += 1

    if deleted:
        logger.info(f"Pruned {deleted} pipeline log file(s) older than {retention_days} days")
    return deleted
