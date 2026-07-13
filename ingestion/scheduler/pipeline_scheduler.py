"""
ingestion/scheduler/pipeline_scheduler.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-001, SPEC-SCHED-002, SPEC-SCHED-003, SPEC-SCHED-004,
       SPEC-SCHED-006, SPEC-SCHED-008
Owner: Platform / Scheduler
Consumers: ingestion/scheduler (entry point), tests/integration

APScheduler-backed daily pipeline scheduler: three trigger modes (linear,
timestamp, manual — SPEC-SCHED-001), startup gap backfill (oldest-first,
no ML inference — SPEC-SCHED-003/004/006), and NSE holiday awareness
(SPEC-SCHED-008).

This module owns scheduling and step sequencing only. The actual step
implementations (download_bhavcopy, compute_features, run_models, ...) are
injected as a `step_runner` callback — SPEC-SOLID-005 (Dependency
Inversion): the scheduler depends on the StepRunner abstraction, not on
concrete scraper/feature/model functions, which are built in later phases.

Out of scope here, deferred to later phases:
- SPEC-SCHED-007 (Retrain Catch-Up): no model registry or retrain trigger
  exists yet; nothing to check overdue-ness against.
- SPEC-SCHED-009 (Laptop-Only Operation): which data source a step
  actually fetches from (NSE archives; Oracle Cloud is deferred) is a
  step implementation concern (`ingestion/scheduler/daily_pipeline.py`'s
  `step_runner`), not a scheduler-engine concern.
- SPEC-SCHED-011 (Step Dependencies): STEPS is a simple ordered list with
  implicit linear dependency (each step requires the previous one to have
  succeeded); the full depends_on graph from 13_scheduler_resilience.md
  applies once steps stop being strictly linear.
"""

import contextlib
import fcntl
import logging
import resource
import time
from datetime import date as date_type
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator, List, Optional

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from ingestion.scheduler.checkpoint import STEP_NAMES, STEPS, CheckpointManager
from ingestion.scheduler.gap_detector import detect_gaps, is_trading_day

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def pipeline_run_lock() -> Iterator[bool]:
    """
    Cross-process, non-blocking advisory lock (fcntl.flock) guarding every
    call to run_steps_for_date.

    2026-07-05: root-caused why pipeline_runs recorded 'failed' for
    2026-07-02/03 despite every individual step's checkpoint showing
    'success' — the scheduler's own daily_pipeline and morning_catchup
    jobs both call run_startup_sequence -> run_steps_for_date(today, ...)
    (see _execute_daily_job's docstring: it is reused verbatim by both
    jobs), and a systemd-restarted process re-fires an overdue coalesced
    cron trigger for "today" via APScheduler's misfire_grace_time
    (86400s) at the same time run_daily_pipeline_once() may still be
    mid-run from this same startup — plus datastore/api/routers/ops.py's
    force_run_step is a *separate OS process* again. Two concurrent
    invocations racing on the same date's pipeline_checkpoints rows both
    see steps as 'running' (non-terminal, so both attempt them) and each
    records its own (often False, from lock contention) outcome.

    Yields
    ------
    bool
        True if this call acquired the lock (caller should proceed).
        False if another process/thread already holds it (caller must
        skip running steps for this invocation entirely rather than
        race — see run_steps_for_date's use of this).

    Raises
    ------
    None — a failure to even open the lock file is logged and treated as
    "lock acquired" (fail open) rather than blocking the pipeline forever
    over a filesystem issue.
    """
    from config.settings import PIPELINE_RUN_LOCK_PATH

    try:
        PIPELINE_RUN_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_file = open(PIPELINE_RUN_LOCK_PATH, "w")
    except OSError as exc:
        logger.warning(f"pipeline_run_lock: could not open lock file ({exc}) — proceeding without it")
        yield True
        return

    acquired = False
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        yield True
    except BlockingIOError:
        yield False
    finally:
        if acquired:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()

# Pre-compute the depends_on lookup for fast access in run_steps_for_date.
# {step_name: [dep_name, ...]} — empty list means no hard prerequisites.
_STEP_DEPS: dict = {step["name"]: step.get("depends_on", []) for step in STEPS}

_INSERT_PIPELINE_RUN = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, ?, ?, ?, ?)
"""

# Phase 1 (Pipeline & Monitoring Remediation, 2026-07-10): a "running" row
# is inserted the moment a run starts, and later UPDATEd (by run_id, not
# re-INSERTed) once it finishes. Previously pipeline_runs only ever got a
# row at the END of a run (_record_pipeline_run) — if the process was
# killed mid-run (e.g. OOM), NO row was ever written for that date, and
# GET /api/v1/ops/runs would keep showing the last N *prior* rows as "most
# recent", which reads to an operator as "today completed fine" when in
# fact today never ran at all. Writing 'running' up front means a crash
# leaves a diagnosable row (status='running', started_at in the past,
# never completed) instead of silence.
_INSERT_PIPELINE_RUN_STARTED = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, NULL, 'running', 0, NULL)
"""

_UPDATE_PIPELINE_RUN_FINISHED = """
    UPDATE pipeline_runs
    SET completed_at = ?, status = ?, error_message = ?
    WHERE run_id = ?
"""

# SPEC-SCHED-013: upsert — one row per job_id, overwritten on every
# invocation attempt. last_success_at only advances on a real success
# (COALESCE keeps the previous value on a failed/skipped attempt) so
# "last successful run" and "last attempt at all" stay independently
# queryable — see ingestion/scheduler/pipeline_scheduler.py's
# _record_heartbeat and datastore/api/routers/system.py's GET /health.
_UPSERT_SCHEDULER_HEARTBEAT = """
    INSERT INTO scheduler_heartbeats (job_id, last_attempt_at, last_status, last_error, last_success_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(job_id) DO UPDATE SET
        last_attempt_at = excluded.last_attempt_at,
        last_status = excluded.last_status,
        last_error = excluded.last_error,
        last_success_at = COALESCE(excluded.last_success_at, scheduler_heartbeats.last_success_at)
"""

# Signature: step_runner(run_date, step_name) -> None. Must raise on failure.
StepRunner = Callable[[date_type, str], None]

_VALID_MODES = ("linear", "timestamp", "manual")


def create_jobstore(db_path: Optional[Path] = None) -> SQLAlchemyJobStore:
    """
    Build the persistent APScheduler job store (SPEC-SCHED-001).

    Parameters
    ----------
    db_path : Path, optional
        SQLite file backing the job store. If None, uses
        config.settings.SCHEDULER_DB_PATH.

    Returns
    -------
    SQLAlchemyJobStore

    Spec References
    ----------------
    SPEC-SCHED-001: persistent job store so scheduled jobs survive restarts.

    Raises
    ------
    None
    """
    if db_path is None:
        from config.settings import SCHEDULER_DB_PATH

        db_path = SCHEDULER_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SQLAlchemyJobStore(url=f"sqlite:///{db_path}")


def create_scheduler(db_path: Optional[Path] = None) -> BackgroundScheduler:
    """
    Build a BackgroundScheduler backed by SQLAlchemyJobStore.

    Parameters
    ----------
    db_path : Path, optional
        Forwarded to create_jobstore.

    Returns
    -------
    BackgroundScheduler
        Not yet started — call .start() once jobs are registered.

    Spec References
    ----------------
    SPEC-SCHED-001

    Raises
    ------
    None
    """
    return BackgroundScheduler(jobstores={"default": create_jobstore(db_path)})


def run_steps_for_date(
    run_date: date_type,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    is_backfill: bool = False,
) -> bool:
    """
    Execute STEPS for one date with checkpoint-resume, backfill ML-skip,
    and dependency-based fallback (SPEC-SCHED-011).

    Resumes from CheckpointManager.get_resume_step(run_date) rather than
    always starting at step 0, so already-succeeded steps are never
    re-executed (SPEC-SCHED-002).

    On a step failure the pipeline does NOT immediately abort. Instead it
    evaluates each subsequent step's depends_on list against the current
    success/failure state for this date. A step is skipped (not failed) if
    any of its hard prerequisites did not succeed — allowing independent
    steps to still execute. A step whose dependencies are all 'success'
    runs normally even if an unrelated earlier step failed.

    Parameters
    ----------
    run_date : date
    step_runner : StepRunner
        Callable invoked as step_runner(run_date, step_name); must raise
        on failure.
    checkpoint_manager : CheckpointManager
    is_backfill : bool
        If True, steps with is_backfillable=False are skipped entirely
        (SPEC-SCHED-006) rather than executed or marked failed.

    Returns
    -------
    bool
        True if every applicable step succeeded or was intentionally
        skipped (non-backfillable or dependency not met).
        False if any step that was attempted actually raised an exception.

    Spec References
    ----------------
    SPEC-SCHED-002: checkpoint-resume on failure.
    SPEC-SCHED-006: no model inference during backfill.
    SPEC-SCHED-011: step dependency evaluation; fallback mechanism.

    PIT Assumptions
    ----------------
    None at this layer — PIT correctness is the responsibility of each
    step's own implementation (e.g. compute_features must only use data
    as-of run_date).

    Raises
    ------
    None — step_runner exceptions are caught and recorded as a failed
    checkpoint, not propagated.
    """
    # 2026-07-05: cross-process guard (see pipeline_run_lock's docstring)
    # — if another process (the scheduler's other recurring job, or the
    # Ops API's force_run_step) is already executing steps for any date,
    # skip this call entirely rather than racing on pipeline_checkpoints.
    # Returning True (not False) here is deliberate: this invocation did
    # not attempt anything, so it must never be recorded as a failed run
    # — the in-progress invocation is the one that will record the real
    # outcome.
    with pipeline_run_lock() as acquired:
        if not acquired:
            logger.warning(
                f"run_steps_for_date({run_date}): another run is already in progress "
                "(cross-process lock held) — skipping this call to avoid racing "
                "checkpoints; the in-progress run will complete normally"
            )
            return True

        resume_step = checkpoint_manager.get_resume_step(run_date)
        if resume_step is None:
            logger.info(f"All steps already succeeded for {run_date} — nothing to do")
            return True

        resume_index = STEP_NAMES.index(resume_step)

        # Pre-seed with steps that already succeeded (from prior runs of this
        # date). SPEC-SCHED-011: dependency checks must honour cross-run state
        # so a fixed prerequisite unlocks its dependents on resume.
        succeeded_this_run: set = checkpoint_manager.get_succeeded_steps(run_date)

        any_step_failed = False

        for index, step in enumerate(STEPS):
            step_name = step["name"]

            if index < resume_index:
                # SPEC-SCHED-002: already succeeded in a previous run; treat as
                # succeeded for dependency resolution.
                succeeded_this_run.add(step_name)
                continue

            if is_backfill and not step["is_backfillable"]:
                logger.info(
                    f"Skipping non-backfillable step '{step_name}' for {run_date} (backfill)"
                )
                continue

            # SPEC-SCHED-011: dependency check. If any required predecessor did
            # not succeed (failed, was skipped, or never ran), skip this step and
            # record it as 'skipped' with the blocking dependency named.
            deps = _STEP_DEPS.get(step_name, [])
            unmet = [d for d in deps if d not in succeeded_this_run]
            if unmet:
                reason = f"dependency not met: {unmet}"
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="skipped", error_message=reason, is_backfill=is_backfill
                )
                logger.warning(
                    f"Skipping '{step_name}' for {run_date} — {reason}"
                )
                continue

            checkpoint_manager.save_checkpoint(run_date, step_name, status="running", is_backfill=is_backfill)
            try:
                step_runner(run_date, step_name)
            except Exception as exc:
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="failed", error_message=str(exc), is_backfill=is_backfill
                )
                logger.error(f"Step '{step_name}' failed for {run_date}: {exc}")
                any_step_failed = True
                # Do NOT return immediately — continue evaluating later steps
                # whose dependencies may still be fully met (SPEC-SCHED-011).
            else:
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="success", is_backfill=is_backfill
                )
                succeeded_this_run.add(step_name)

        return not any_step_failed


def run_backfill(
    gap_dates: List[date_type],
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
) -> List[date_type]:
    """
    Process missing dates chronologically, oldest first.

    Parameters
    ----------
    gap_dates : list of date
        Dates to backfill, in any order — this function sorts them.
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager

    Returns
    -------
    list of date
        The subset of gap_dates that completed successfully, in the
        chronological order they were processed.

    Spec References
    ----------------
    SPEC-SCHED-003: unlimited backfill, no maximum gap window.
    SPEC-SCHED-004: chronological backfill order — oldest first, never
        skip or reorder. A failed date does not stop the rest: it is
        logged and retried on the next startup, while later dates still
        process.
    SPEC-SCHED-006: no ML inference during backfill (delegated to
        run_steps_for_date's is_backfill flag).

    PIT Assumptions
    ----------------
    None at this layer.

    Raises
    ------
    None
    """
    ordered = sorted(gap_dates)
    succeeded = []
    for gap_date in ordered:
        ok = run_steps_for_date(gap_date, step_runner, checkpoint_manager, is_backfill=True)
        if ok:
            succeeded.append(gap_date)
        else:
            logger.warning(f"Backfill for {gap_date} incomplete — will retry on next startup")
    return succeeded


def _record_pipeline_run_started(
    run_date: date_type,
    started_at: datetime,
    db_path: Optional[Path] = None,
) -> int:
    """
    Insert the 'running' row for a pipeline_runs invocation as it begins.

    Phase 1 (Pipeline & Monitoring Remediation): called at the top of
    run_startup_sequence, before run_steps_for_date executes anything, so
    that a process kill mid-run (OOM, crash) still leaves a row behind —
    status='running', completed_at=NULL — instead of no row at all. The
    returned run_id is passed to _record_pipeline_run so the same row is
    UPDATEd in place on completion rather than a second row being INSERTed.

    Parameters
    ----------
    run_date : date
    started_at : datetime
    db_path : Path, optional
        Defaults to config.settings.PIPELINE_LOG_DB_PATH.

    Returns
    -------
    int
        The new row's run_id (SQLite ROWID / AUTOINCREMENT value).

    Raises
    ------
    None
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
    """
    Write the whole-day summary row to pipeline_runs (SPEC-SCHED-005).

    This is the table gap_detector.get_last_successful_run_date() reads to
    find the last successful date — without a writer here, gap detection
    would never see any run history and the next startup's catch-up would
    never trigger. pipeline_checkpoints (the per-*step* log) is written by
    CheckpointManager already; pipeline_runs (the per-*day* summary) is
    written here, the one place every path that finishes a day's run
    (startup catch-up and the recurring scheduled job alike) passes
    through.

    Parameters
    ----------
    run_date : date
    success : bool
    started_at : datetime
    db_path : Path, optional
        Defaults to config.settings.PIPELINE_LOG_DB_PATH (same file
        pipeline_checkpoints lives in).
    run_id : int, optional
        Phase 1: if provided (the id returned by
        _record_pipeline_run_started), UPDATEs that row's completed_at/
        status in place instead of INSERTing a new row — this is the
        normal path as of the Pipeline & Monitoring Remediation session.
        None is accepted only for backward compatibility with any direct
        caller that hasn't been updated to the started/finished pair.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-005

    PIT Assumptions
    ----------------
    None — operational metadata, not market data.

    Raises
    ------
    None
    """
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
                    0,  # stocks_processed: not threaded through StepRunner's fire-and-forget contract yet
                    None,
                ),
            )
        conn.commit()


def _job_timer_start() -> float:
    """A23 (benchmark history): call at the top of a job-runner's try block; pair with _job_timer_stats."""
    return time.monotonic()


def _job_timer_stats(start: float) -> tuple:
    """
    A23 (benchmark history): (duration_seconds, peak_rss_mb) since `start`
    (a _job_timer_start() value), for passing into _record_heartbeat.

    peak_rss_mb sums RUSAGE_SELF (this process) and RUSAGE_CHILDREN (any
    subprocess.run'd since process start) ru_maxrss, converted KB -> MB.
    Both are process-lifetime HIGH-WATER MARKS the OS never resets — not
    a precise per-run delta. In this long-lived scheduler process, a job
    that runs shortly after an even memory-heavier one will under-report
    its own peak. Accepted as a known limitation (see FeatureBacklog.md
    A23): the ticket's own stated use — comparing weekday vs. weekend job
    footprints once weeks of data accumulate — is a relative-trend
    comparison, not a precise per-run figure, so this approximation is
    good enough without adding a new out-of-process measurement system
    (e.g. psutil polling a child PID).

    Returns
    -------
    tuple of (float, float)
        (duration_seconds, peak_rss_mb).
    """
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

    Called on EVERY invocation attempt of a recurring job — success,
    failure, or a deliberate early skip (e.g. backfill_catchup's "no
    cached FYERS token" guard) — so GET /health (and an operator) can see
    "this job hasn't fired in N hours" as a distinct, queryable fact,
    rather than having to read the scheduler process's own log file by
    hand to notice it has gone silent.

    Parameters
    ----------
    job_id : str
        'daily_pipeline' | 'backfill_catchup'.
    status : str
        'success' | 'failed' | 'skipped'.
    error : str, optional
        Error or skip-reason message. None on a clean success.
    db_path : Path, optional
        Defaults to config.settings.PIPELINE_LOG_DB_PATH.
    duration_seconds : float, optional
        A23: wall-clock duration of this run, from _job_timing(). None if
        the caller didn't measure it (e.g. not yet instrumented).
    peak_rss_mb : float, optional
        A23: approximate peak RSS in MB, from _job_timing(). See that
        function's docstring for why this is a high-water-mark
        approximation, not an exact per-run figure.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-013.

    PIT Assumptions
    ----------------
    None — operational metadata, not market data.

    Raises
    ------
    None — this function deliberately swallows its own exceptions
    (logged, not raised). A heartbeat write failing must never be the
    reason a scheduled job's own outcome goes unrecorded or, worse,
    propagates up and destabilizes the caller.
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

    # A21 (Pipeline Health Checker): scheduler_heartbeats above only ever
    # holds the latest attempt per job_id — append a row to job_run_log
    # (DuckDB, alongside data_integrity_findings) too, so
    # datastore/health/checks.py can answer "did this job actually
    # succeed on each of the last 7 days" for weekly/weekend jobs that
    # have no other per-date history. Best-effort: a DuckDB write hiccup
    # must never take down the SQLite heartbeat write above, which many
    # other jobs already depend on.
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


def run_startup_sequence(
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    today: Optional[date_type] = None,
    db_path: Optional[Path] = None,
) -> bool:
    """
    On-startup sequence: detect and backfill gaps, then run today's pipeline.

    Used both for the one-off catch-up call at process start and as the
    target of the recurring scheduled job (_execute_daily_job) — there is
    only one code path that finishes a day's run, so pipeline_runs is
    always recorded regardless of which caller triggered it.

    Parameters
    ----------
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager
    today : date, optional
        Defaults to now_ist().date() (IST); exposed for testability.
    db_path : Path, optional
        pipeline_runs SQLite path used by gap detection and recording.

    Returns
    -------
    bool
        True if today's own pipeline run succeeded, or was skipped as an
        NSE holiday; False if it failed. Gap-backfill outcomes for past
        dates don't affect this return value — a failed gap date is
        retried on the next startup, per SPEC-SCHED-004.

    Spec References
    ----------------
    SPEC-SCHED-001: "On startup: query pipeline_runs table, find all
        trading dates since last successful run."
    SPEC-SCHED-003, SPEC-SCHED-004: unlimited, chronological backfill.
    SPEC-SCHED-005: today's outcome is recorded to pipeline_runs.
    SPEC-SCHED-008: today's run is skipped if today is not a trading day
        (weekend or declared NSE holiday) — the market never traded, so
        there is nothing to process. Mirrors gap_detector.is_trading_day(),
        which already excludes weekends for gap-day backfill; this check
        was previously is_nse_holiday() alone (declared holidays only),
        which let the pipeline run on ordinary Saturdays/Sundays whenever
        the scheduler process happened to start on one.
    2026-07-08 follow-up to SPEC-SCHED-014: today's own run is also
        skipped if called before DAILY_PIPELINE_SCHEDULE_TIME (18:00 IST).
        This function runs unconditionally on every process (re)start
        (main()'s startup catch-up call, daily_pipeline.py) as well as via
        the recurring cron job — so a systemd restart (crash, OOM-guard,
        manual) at, say, 07:09 IST previously hit the exact same
        guaranteed-404 bug that run_morning_catchup_sequence's docstring
        already documents for the 07:30 job: NSE has not published
        "today"'s bhavcopy yet, so download_bhavcopy always fails and
        cascades (via depends_on) to skip adjust_prices, compute_features,
        run_models, write_signals, sanity_check, and paper_trade for
        today — even though gap-backfill for prior days succeeded.

    PIT Assumptions
    ----------------
    None at this layer.

    Raises
    ------
    None
    """
    today = today or now_ist().date()

    gaps = detect_gaps(today=today, db_path=db_path)
    if gaps:
        run_backfill(gaps, step_runner, checkpoint_manager)

    if not is_trading_day(today):
        logger.info(f"{today} is not a trading day (weekend or NSE holiday) — skipping today's pipeline run")
        return True

    now = now_ist()
    if today == now.date():
        # Only the real "today" can possibly be before its own bhavcopy is
        # published — a backfill/test call with an explicit past `today`
        # is never subject to this check, regardless of wall-clock time.
        from config.settings import DAILY_PIPELINE_SCHEDULE_TIME

        schedule_hour, schedule_minute = (int(part) for part in DAILY_PIPELINE_SCHEDULE_TIME.split(":"))
        if (now.hour, now.minute) < (schedule_hour, schedule_minute):
            logger.info(
                f"{today}: called at {now.strftime('%H:%M')} IST, before the "
                f"{DAILY_PIPELINE_SCHEDULE_TIME} bhavcopy publish time — skipping "
                f"today's own pipeline run (gap-backfill for prior days already ran above)"
            )
            return True

    started_at = now
    run_id = _record_pipeline_run_started(today, started_at, db_path)
    ok = run_steps_for_date(today, step_runner, checkpoint_manager, is_backfill=False)
    _record_pipeline_run(today, ok, started_at, db_path, run_id=run_id)
    return ok


def run_morning_catchup_sequence(
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    today: Optional[date_type] = None,
    db_path: Optional[Path] = None,
) -> bool:
    """
    Backward-only catch-up: retry gap days strictly before `today`, never
    "today" itself (2026-07, SPEC-SCHED-014 follow-up bug fix).

    schedule_morning_catchup previously reused run_startup_sequence
    wholesale, which — after its gap-backfill — always went on to call
    run_steps_for_date(today, ...) too. At 07:30 IST NSE has not published
    "today"'s bhavcopy yet (it typically appears only after that day's own
    market close), so that second call was always a guaranteed 404 for
    every step in STEPS, every single morning. detect_gaps(today=today) is
    already exclusive of `today` (see gap_detector.detect_gaps's
    docstring: "window is (last_run_date, today), exclusive of both
    ends") — so the gap-backfill half of run_startup_sequence was already
    correctly backward-only; only the trailing "run today" call was the
    bug. This function keeps exactly that gap-backfill half and drops the
    trailing call entirely, so a morning firing can only ever retry
    previous trading day(s) whose steps failed (e.g. a transient
    download_fno/download_macro/download_corporate_actions/
    download_large_deals network error) — never attempt today's own run.

    Parameters
    ----------
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager
    today : date, optional
        Defaults to now_ist().date() (IST); exposed for testability. Only
        used as detect_gaps's exclusive upper bound — never passed to
        run_steps_for_date/run_backfill directly.
    db_path : Path, optional
        pipeline_runs SQLite path used by gap detection.

    Returns
    -------
    bool
        True if there were no gaps, or every gap date backfilled
        successfully. False if at least one gap date is still incomplete
        after this attempt (it remains queryable via pipeline_checkpoints
        and will be retried on the next morning-catchup or 18:00 firing).
        Unlike run_startup_sequence's return value, this never reflects
        "today"'s own outcome — there is no such outcome here.

    Spec References
    ----------------
    SPEC-SCHED-003, SPEC-SCHED-004: unlimited, chronological backfill.
    SPEC-SCHED-014 follow-up (2026-07): morning catch-up scope fix.

    PIT Assumptions
    ----------------
    None at this layer — same as run_startup_sequence's gap-backfill half.

    Raises
    ------
    None
    """
    today = today or now_ist().date()

    gaps = detect_gaps(today=today, db_path=db_path)
    if not gaps:
        logger.info(f"Morning catch-up: no gap days before {today} — nothing to do")
        return True

    succeeded = run_backfill(gaps, step_runner, checkpoint_manager)
    ok = len(succeeded) == len(gaps)
    if not ok:
        logger.warning(
            f"Morning catch-up: {len(succeeded)}/{len(gaps)} gap day(s) backfilled "
            f"successfully before {today}; remaining will retry on next firing"
        )
    return ok


def _execute_morning_catchup_job(
    step_runner: StepRunner, checkpoint_manager: CheckpointManager, job_id: str = "morning_catchup"
) -> None:
    """
    Module-level job target for schedule_morning_catchup (2026-07,
    SPEC-SCHED-014 follow-up). Top-level and picklable, same
    SQLAlchemyJobStore constraint as _execute_daily_job.

    Distinct from _execute_daily_job: calls run_morning_catchup_sequence
    (backward-only) rather than run_startup_sequence, so this job never
    attempts "today"'s own pipeline steps — see that function's docstring
    for why the previous shared-function approach always 404'd at 07:30.

    Parameters
    ----------
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager
    job_id : str
        Recorded to scheduler_heartbeats under its own id, same reasoning
        as _execute_daily_job's job_id parameter.

    Returns
    -------
    None

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013), same reasoning as
    _execute_daily_job.
    """
    today = now_ist().date()
    _t0 = _job_timer_start()
    duration_seconds, peak_rss_mb = None, None
    try:
        ok = run_morning_catchup_sequence(step_runner, checkpoint_manager, today=today)

        # 2026-07 (backlog #1/#2/#3, Sub-tasks B/C): VIX/FII-DII/USD-INR plus
        # the new global index snapshots are captured here, once per calendar
        # day for "today" only — deliberately outside run_morning_catchup_
        # sequence's gap-backfill (which only ever walks days strictly
        # before today; these indicators' PIT design specifically wants a
        # pre-market snapshot of *today*, not a backfill of past days -- see
        # ingestion.scheduler.daily_pipeline.step_download_macro_morning's
        # docstring). A failure here must never affect the gap-backfill
        # heartbeat outcome above (non-critical, SPEC-PIPE-006), so it's
        # caught independently.
        try:
            from ingestion.scheduler.daily_pipeline import step_download_macro_morning

            step_download_macro_morning(today)
        except Exception as exc:
            logger.warning(f"morning_catchup: step_download_macro_morning failed for {today}: {exc}")

        error = None if ok else "one or more gap days still incomplete"
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "success" if ok else "failed", error,
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"{job_id} job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def _execute_daily_job(
    step_runner: StepRunner, checkpoint_manager: CheckpointManager, job_id: str = "daily_pipeline"
) -> None:
    """
    Module-level job target invoked by the persistent scheduler.

    APScheduler's SQLAlchemyJobStore pickles every job to persist it
    (SPEC-SCHED-001), which requires the job's `func` to be a top-level,
    importable callable — never a lambda or closure, which cannot be
    pickled by reference. This function exists so `schedule_daily_pipeline`
    can register a picklable job; `args=[step_runner, checkpoint_manager]`
    are pickled alongside it, which in turn requires `step_runner` to
    itself be a plain module-level function (not a lambda/closure) for the
    same reason.

    Parameters
    ----------
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager
    job_id : str
        Which recurring job is calling this (2026-07: also reused by
        schedule_morning_catchup for an earlier-in-the-day second trigger
        of the identical catch-up-then-today logic). Threaded through so
        each job's heartbeat is recorded under its own id — hardcoding
        "daily_pipeline" here would make the two jobs' attempts
        indistinguishable in scheduler_heartbeats/the Ops page.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-001, SPEC-SCHED-013 (heartbeat + exception containment).

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013): run_startup_sequence
    itself never raises, but this is the function APScheduler invokes
    directly as the job target, and the recurring job's ability to fire
    on its NEXT scheduled time must never depend on every line inside
    this call staying exception-free forever. Caught a real, multi-day
    scheduler process whose job silently stopped firing entirely after
    one unrelated job's exception — see BuildLog.md "Scheduler/DuckDB
    concurrency resilience". This wrapper, plus the heartbeat write
    below, ensures (a) no exception from here can ever propagate further
    than this function, and (b) every attempt — success or failure — is
    independently observable via GET /health, not just inferable from
    log files.
    """
    _t0 = _job_timer_start()
    duration_seconds, peak_rss_mb = None, None
    try:
        ok = run_startup_sequence(step_runner, checkpoint_manager, today=now_ist().date())
        error = None if ok else "pipeline run returned False"
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "success" if ok else "failed", error,
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"{job_id} job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def _execute_backfill_catchup() -> None:
    """
    Module-level job target for the recurring backfill-catchup job
    (SPEC-SCHED-012). Top-level and picklable, same constraint as
    _execute_daily_job — required by SQLAlchemyJobStore.

    Safety guard (the reason this is its own function rather than a
    direct call to ingestion.backfill_runner.run_backfill): FYERS access
    tokens expire daily and can only be renewed via an *interactive*
    OAuth2 login (no refresh-token mechanism in FYERS' retail API) — see
    ingestion/scrapers/fyers_backfill.py's module docstring. An unattended
    scheduled job must never reach FYERSBackfill.get_access_token()'s
    interactive fallback, which blocks forever on input() with no
    connected stdin. This function checks for a valid, already-cached
    token *before* calling run_backfill, and skips cleanly (logging why)
    if none is available — the operator must run `python3 -m
    ingestion.scrapers.fyers_backfill login` / `... exchange <url>`
    themselves at least once that day before this job can do real work.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-012, SPEC-PIPE-001, SPEC-SCHED-013 (heartbeat + exception containment).

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013). This job is the one
    that originally crashed (a DuckDB lock conflict against the
    DataStore API process — see datastore/api/db.py's module docstring
    for the underlying fix) and, separately, was followed by the
    scheduler going silent for both this job AND the unrelated daily
    pipeline job. Every exit path here — clean skip, success, or
    failure — now records a heartbeat (GET /health surfaces it) and no
    exception escapes this function.
    """
    from datetime import timedelta

    from config.settings import BACKFILL_YEARS
    from config.universe import get_tickers
    from ingestion.backfill_runner import run_backfill
    from ingestion.scrapers.fyers_backfill import FYERSBackfill

    _t0 = _job_timer_start()
    try:
        fb = FYERSBackfill()
        cached_token = fb._load_cached_token()
        if not cached_token or not fb._validate_token(cached_token):
            skip_reason = "no valid (same-day) FYERS token cached"
            logger.warning(
                "Backfill catch-up skipped: no valid (same-day) FYERS token cached. "
                "Run `python3 -m ingestion.scrapers.fyers_backfill login` / "
                "`... exchange <redirected URL>` first, then this job will pick "
                "up the cached token on its next scheduled run today."
            )
            duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
            _record_heartbeat(
                "backfill_catchup", "skipped", skip_reason,
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
            return

        to_date = now_ist().date()
        from_date = to_date - timedelta(days=365 * BACKFILL_YEARS)
        tickers = get_tickers()
        logger.info(f"Backfill catch-up starting: {len(tickers)} universe tickers, {from_date}..{to_date}")
        run_backfill(tickers, from_date.isoformat(), to_date.isoformat())
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "backfill_catchup", "success",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"backfill_catchup job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "backfill_catchup", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_backfill_catchup(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the recurring backfill catch-up job (SPEC-SCHED-012).

    Distinct from the daily pipeline (schedule_daily_pipeline): this job
    re-runs ingestion.backfill_runner against the full current universe
    every day, relying entirely on has_sufficient_history()'s coverage
    check and the resume checkpoint to make it a fast no-op for tickers
    already backfilled — its only real work is newly-added tickers, or
    grinding through a large outstanding backfill (e.g. a multi-month BSE
    expansion) a day's FYERS_MAX_CALLS_PER_DAY budget at a time.

    Parameters
    ----------
    scheduler : BackgroundScheduler
    schedule_time : str, optional
        "HH:MM". Defaults to config.settings.BACKFILL_CATCHUP_TIME (20:00
        IST — after the 18:00 daily pipeline, so the two never compete for
        the trigger window; they don't compete for FYERS call budget
        either, since the daily pipeline's steps never call FYERS).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-012, SPEC-PIPE-001.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    if schedule_time is None:
        from config.settings import BACKFILL_CATCHUP_TIME

        schedule_time = BACKFILL_CATCHUP_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))

    scheduler.add_job(
        _execute_backfill_catchup,
        CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
        id="backfill_catchup",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Backfill catch-up scheduled: time={schedule_time} IST, daily")


def schedule_daily_pipeline(
    scheduler: BackgroundScheduler,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    mode: Optional[str] = None,
    schedule_time: str = "18:00",
) -> None:
    """
    Register the recurring daily pipeline job under one of three modes.

    Parameters
    ----------
    scheduler : BackgroundScheduler
    step_runner : StepRunner
        Must be a plain, importable, module-level function — never a
        lambda, closure, or bound method on a non-picklable object.
        SQLAlchemyJobStore pickles this alongside the job to persist it
        across restarts; an unpicklable callable raises ValueError at
        scheduler.start() time, not at registration time.
    checkpoint_manager : CheckpointManager
    mode : str, optional
        'linear' | 'timestamp' | 'manual'. Defaults to
        config.settings.SCHEDULER_MODE.
    schedule_time : str
        "HH:MM" used as the cron fire time. SPEC-SCHED-001: the pipeline
        itself has no hard clock dependency (15-hour window, 3:30 PM to
        9:15 AM) — this is only when the *trigger* fires, not a deadline.

    Returns
    -------
    None
        In 'manual' mode, no job is registered — the caller triggers runs
        explicitly via run_steps_for_date / run_startup_sequence.

    Spec References
    ----------------
    SPEC-SCHED-001: three scheduling modes; misfire_grace_time=86400 (a
        full day) so a missed trigger is still honored, since the
        pipeline's actual window is 15 hours, not a fixed minute.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    ValueError
        If mode is not one of the three valid modes.
    """
    if mode is None:
        from config.settings import SCHEDULER_MODE

        mode = SCHEDULER_MODE

    if mode not in _VALID_MODES:
        raise ValueError(f"Unknown scheduler mode '{mode}'. Must be one of {_VALID_MODES}")

    if mode == "manual":
        logger.info("SCHEDULER_MODE=manual — no recurring job registered")
        return

    hour, minute = (int(part) for part in schedule_time.split(":"))

    scheduler.add_job(
        _execute_daily_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        args=[step_runner, checkpoint_manager, "daily_pipeline"],
        id="daily_pipeline",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Daily pipeline scheduled: mode={mode}, time={schedule_time} IST")


def schedule_morning_catchup(
    scheduler: BackgroundScheduler,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    schedule_time: str = "07:30",
) -> None:
    """
    Register a recurring backward-only gap catch-up job, fired earlier in
    the day than the main 18:00 pipeline (2026-07, SPEC-SCHED-014
    follow-up; fixed same session — see run_morning_catchup_sequence's
    docstring).

    This job runs _execute_morning_catchup_job / run_morning_catchup_sequence
    — gap-backfill of previous trading day(s) only. It deliberately does
    NOT run "today"'s own pipeline steps: NSE typically doesn't publish a
    trading day's bhavcopy until after that day's own market close, so
    attempting "today" at 07:30 IST always 404s. This job exists so a step
    that failed on an earlier date (e.g. download_fno/
    download_corporate_actions/download_large_deals hitting a transient
    network error) gets retried hours sooner than waiting for the 18:00
    IST run, rather than sitting visibly "never run" on the Ops page until
    evening. As of 2026-07 this firing also captures VIX/FII-DII/USD-INR
    plus Nasdaq/Dow/S&P 500/Nikkei/Hang Seng (see
    ingestion.scheduler.daily_pipeline.step_download_macro) — moved here
    from the 18:00 download_macro step for PIT reasons: see
    step_download_macro's module docstring. NSE-sourced only, same as the
    main job — no FYERS dependency (contrast with the removed
    backfill_catchup job).

    Parameters
    ----------
    scheduler : BackgroundScheduler
    step_runner : StepRunner
        Same picklability constraint as schedule_daily_pipeline.
    checkpoint_manager : CheckpointManager
    schedule_time : str
        "HH:MM", Asia/Kolkata, mon-fri (same trading-day cadence as the
        main job).

    Returns
    -------
    None

    Raises
    ------
    None
    """
    hour, minute = (int(part) for part in schedule_time.split(":"))

    scheduler.add_job(
        _execute_morning_catchup_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        args=[step_runner, checkpoint_manager, "morning_catchup"],
        id="morning_catchup",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Morning catch-up scheduled: time={schedule_time} IST")


def _determine_groww_live_snapshot_month():
    """
    Sample one real scheme to find out which (year, month) Groww's live
    holdings snapshot actually represents — needed because Groww (the
    primary MF-holdings source as of P2.2's pivot, see SPEC-MFHOLD-001 and
    ingestion/scrapers/groww_mf_holdings.py's module docstring) exposes no
    historical archive, only "whatever is live right now". Blindly
    assuming "the previous calendar month" (the original, pre-Groww
    design) is unsafe: depending on exactly when each AMC publishes,
    Groww may already be showing the current month's snapshot, or may
    still be lagging — sampling avoids guessing.

    Returns
    -------
    tuple of (int, int)
        (year, month) of the live snapshot.

    Raises
    ------
    ConnectionError
        If no live snapshot date can be determined at all.
    """
    from ingestion.scrapers.groww_mf_holdings import _fetch_scheme_detail, _list_scheme_ids

    scheme_ids = _list_scheme_ids("SBI Mutual Fund")
    for scheme_id in scheme_ids:
        detail = _fetch_scheme_detail(scheme_id)
        holdings = (detail or {}).get("holdings") or []
        if holdings:
            portfolio_date = holdings[0].get("portfolio_date")
            if portfolio_date:
                snapshot_dt = datetime.fromisoformat(portfolio_date.replace("Z", "+00:00"))
                return snapshot_dt.year, snapshot_dt.month
    raise ConnectionError("Could not determine Groww's live snapshot month — no scheme returned a portfolio_date")


def _execute_mf_holdings_job() -> None:
    """
    APScheduler job target for the weekly MF-holdings ingestion
    (SPEC-SCHED-009, P2.2 — pivoted to Groww as primary source; changed
    from twice-monthly to weekly per user decision, Big Investor Activity
    Phase C, 2026-07-05). Module-level function, not a closure/lambda —
    SQLAlchemyJobStore must be able to pickle it (same constraint
    documented on _execute_daily_job).

    Registers every Groww-listed AMC (a real network call — AMC_REGISTRY
    starts empty for Groww until this is called, see SPEC-MFHOLD-001),
    determines which month Groww's live snapshot actually represents
    (never assumes — see _determine_groww_live_snapshot_month), then
    ingests that month for every registered AMC (Groww's 49 — the SBI
    direct-Excel cross-check source was retired 2026-07-04: Groww alone
    was judged sufficient, see FutureDevelopment.md).

    Fires every Saturday (config.settings.MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK)
    regardless of whether the underlying AMC disclosure actually changed
    that week — save_monthly_parquet's merge-not-overwrite behavior (P2.2
    continued) makes re-ingesting an unchanged month a safe no-op, it just
    refreshes rows for schemes whose data has changed, never duplicates.
    """
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.amfi_holdings import run_monthly_ingestion, sync_duckdb_table
    from ingestion.scrapers.groww_mf_holdings import register_all_amcs

    _t0 = _job_timer_start()
    try:
        register_all_amcs()
        year, month = _determine_groww_live_snapshot_month()
        run_monthly_ingestion(year, month)
        # Phase C (Big Investor Activity): mirror the just-written parquet
        # into the mf_holdings DuckDB table so the API can query it.
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            sync_duckdb_table(conn, year, month)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "mf_holdings_ingestion", "success",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except RuntimeError as exc:
        # AMC_REGISTRY empty (no real source configured yet) — a known,
        # documented gap, not an unexpected failure. Recorded as
        # "skipped", not "failed".
        logger.warning(f"mf_holdings_ingestion skipped: {exc}")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "mf_holdings_ingestion", "skipped", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"mf_holdings_ingestion job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "mf_holdings_ingestion", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_mf_holdings_ingestion(
    scheduler: BackgroundScheduler,
    day_of_week: Optional[str] = None,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the recurring weekly MF-holdings ingestion job (SPEC-SCHED-009
    — laptop-only APScheduler job store, not a separate Oracle/OS-level
    cron entry, same precedent as schedule_backfill_catchup).

    Parameters
    ----------
    scheduler : BackgroundScheduler
    day_of_week : str, optional
        Cron day-of-week field, e.g. "sat". Defaults to
        config.settings.MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK.
    schedule_time : str, optional
        "HH:MM". Defaults to config.settings.AMFI_SCHEDULE_TIME (13:00 IST).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-009, SPEC-PIPE-003.

    Raises
    ------
    None
    """
    if day_of_week is None:
        from config.settings import MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK

        day_of_week = MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK
    if schedule_time is None:
        from config.settings import AMFI_SCHEDULE_TIME

        schedule_time = AMFI_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))

    scheduler.add_job(
        _execute_mf_holdings_job,
        CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute, timezone="Asia/Kolkata"),
        id="mf_holdings_ingestion",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"MF holdings ingestion scheduled: day_of_week={day_of_week}, time={schedule_time} IST")


# ---------------------------------------------------------------------------
# Model training job (SPEC-SCHED-007) — weekday, after daily pipeline
# ---------------------------------------------------------------------------

def _execute_model_training_job(model_names: Optional[List[str]] = None, job_id: str = "model_training") -> None:
    """
    Check whether any model is overdue for retraining (SPEC-SCHED-007) and,
    if so, trigger training. This job fires on weekday evenings (~20:00 IST),
    after the 18:00 daily pipeline has finished writing signals, so the
    training data it consumes is today-complete.

    Model training can take 2–8 hours (TFT/BiLSTM on CPU) — well within the
    6 PM–5 PM next-day 23-hour window (SPEC-SYS-002 update, 2026-07-02).

    Which models to retrain is checked against datastore/models/registry.json
    (SPEC-MODEL-005): a model is overdue if
    `days_since_last_train > training_interval_days × RETRAIN_OVERDUE_MULTIPLIER`.
    If no model is overdue, the job completes immediately (fast no-op).

    Parameters
    ----------
    model_names : list of str, optional
        Pipeline & Monitoring Remediation Phase 4 (A52): if given, only
        these models are considered — used by
        schedule_model_training_nightly's per-group jobs to spread
        training across different nights of the week instead of one
        weekly job checking (and potentially training) everything
        back-to-back. None (default) checks every known model, the
        original single-job behavior (still used by schedule_model_training).
    job_id : str
        Which heartbeat/job_run_log id to record under — lets each
        nightly group be independently observable on the Ops dashboard
        instead of all sharing "model_training".

    Top-level function (not a closure/lambda) — APScheduler SQLAlchemyJobStore
    picklability requirement, same as _execute_daily_job.

    Spec References
    ----------------
    SPEC-SCHED-007, SPEC-MODEL-005, SPEC-MODEL-008.

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013).
    """
    import json
    from pathlib import Path

    from config.settings import DEFAULT_TRAINING_INTERVAL_DAYS, MODELS_DIR, RETRAIN_OVERDUE_MULTIPLIER

    _t0 = _job_timer_start()
    try:
        registry_path = Path(MODELS_DIR) / "registry.json"
        if not registry_path.exists():
            logger.info(f"{job_id}: registry.json not found — no trained models yet, skipping")
            duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
            _record_heartbeat(
                job_id, "skipped", "registry.json not found",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
            return

        with registry_path.open() as f:
            registry = json.load(f)

        # Check every model this scheduler knows how to train, not just the
        # ones already present in registry.json. A model that is mapped in
        # _MODEL_TRAINING_SCRIPT_MAP but has never been trained (e.g.
        # multibagger before its first real run) has no registry entry at
        # all, so iterating registry.items() alone silently never considers
        # it — a permanent blind spot for any newly-added model. Models
        # explicitly mapped to None (tft/bilstm — Phase 3, not built) are
        # excluded.
        known_models = set(registry.keys()) | {
            name for name, script in _MODEL_TRAINING_SCRIPT_MAP.items() if script is not None
        }
        if model_names is not None:
            known_models &= set(model_names)

        today = now_ist().date()
        overdue_models = []
        for model_name in known_models:
            meta = registry.get(model_name, {})
            last_train_str = meta.get("last_trained_date")
            interval_days = meta.get("training_interval_days", DEFAULT_TRAINING_INTERVAL_DAYS)
            if not last_train_str:
                overdue_models.append((model_name, "never trained"))
                continue
            from datetime import date as date_cls
            last_train = date_cls.fromisoformat(last_train_str)
            days_since = (today - last_train).days
            threshold = interval_days * RETRAIN_OVERDUE_MULTIPLIER
            if days_since > threshold:
                overdue_models.append((model_name, f"{days_since}d since last train, threshold {threshold:.0f}d"))

        if not overdue_models:
            logger.info(f"{job_id}: no models overdue — skipping")
            duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
            _record_heartbeat(
                job_id, "skipped", "no models overdue",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
            return

        logger.info(f"{job_id}: {len(overdue_models)} model(s) overdue: {overdue_models}")
        # 2026-07-05: several overdue model names share the same underlying
        # training script (train_all_phase1.py trains hmm_market +
        # pnd_detector + signal_5d + signal_21d + meta_labeler +
        # conformal_signal5d in one combined run — see
        # _trigger_model_retrain's script_map) — dedupe by resolved script
        # path so one retrain check cycle doesn't invoke the same 2-8 hour
        # subprocess up to 6 times back-to-back for the same overdue reason.
        seen_scripts: set = set()
        for model_name, reason in overdue_models:
            script = _MODEL_TRAINING_SCRIPT_MAP.get(model_name)
            if script is not None and script in seen_scripts:
                logger.info(f"  Skipping '{model_name}' retrain — already covered by this cycle's '{script}' run")
                continue
            logger.info(f"  Queuing retrain for '{model_name}' ({reason})")
            # Phase 3 retrain protocol (SPEC-MODEL-008): snapshot → train →
            # shadow-test → compare → promote.  The actual training scripts
            # (scripts/train_*.py) are invoked as subprocess calls here so
            # they run in their own process and don't hold DuckDB write locks
            # for the life of the scheduler process.
            _trigger_model_retrain(model_name)
            if script is not None:
                seen_scripts.add(script)

        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "success",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )

    except Exception as exc:
        logger.error(f"{job_id} job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            job_id, "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


# 2026-07-05 (part 1): signal_63d/tft/bilstm pointed at scripts that do
# not exist on disk (confirmed via `ls` — scripts/run_phase1_backtest.py,
# scripts/run_phase2_backtest.py, scripts/train_tft.py,
# scripts/train_bilstm.py are all missing; tft/bilstm are Phase 3 and
# legitimately not built yet). signal_5d/signal_21d/meta_labeler/
# conformal_signal5d were ALSO pointed at the nonexistent
# run_phase1_backtest.py — confirmed via train_all_phase1.py's actual code
# that it is the real trainer for hmm_market + pnd_detector + signal_5d +
# signal_21d + meta_labeler + conformal_signal5d in one combined run, so
# those six were remapped there.
#
# 2026-07-05 (part 2, same day): the "part 1" fix above was still broken —
# _trigger_model_retrain ran `[sys.executable, <file path>]` as a bare
# script, not `-m <module>`. Verified directly: `.venv/bin/python
# systems/ml_signal_engine/inference/train_all_phase1.py --help` raises
# `ModuleNotFoundError: No module named 'backtest'`, because running a
# .py by file path only puts that file's own directory on sys.path, not
# the repo root — every absolute `from backtest...`/`from config...`
# import in these training modules would fail. This map now holds dotted
# module names (each module's own docstring already documents itself as
# "operator CLI (python3 -m ...)"); _trigger_model_retrain below invokes
# them with `-m` and cwd=repo root instead of a bare file path.
#
# signal_63d: no standalone trainer exists, but
# systems/ml_signal_engine/inference/retrain_phase2.py IS a real, working
# trainer for it (see that module's docstring — "trains Signal63D ...
# out of scope until now"). It also retrains signal_5d/signal_21d in the
# same run with the expanded Phase 2 feature set (fundamental/governance/
# MF-holdings/corp-action/F&O on top of the Phase 1 70 technical
# features) and only overwrites the registry entry for each horizon if
# the Phase 2 Sharpe is >= the Phase 1 Sharpe (see its own
# `improved_or_neutral` check) — so pointing signal_63d here is not a
# silent scope change, it's the documented real retrain protocol
# (SPEC-MODEL-008) for all three signal horizons.
#
# multibagger: previously had no standalone periodic-retrain CLI —
# score_multibagger.py only trains inline as a fallback when no cached
# artifact exists yet (see its own module docstring, backlog #27) and
# does not decide when to retrain. Closed 2026-07-05 (same day, part 3):
# added systems/ml_signal_engine/inference/train_multibagger.py, a real
# standalone trainer built from the already-real
# load_multibagger_training_data_from_db() + MultibaggerModel.train_full()
# + train_all_phase1.py's _save_model() convention — no gap left.
#
# tft / bilstm: previously had no standalone periodic-retrain CLI either
# (mapped to None, "Phase 3, not built yet"). Closed 2026-07-09: found
# systems/ml_signal_engine/inference/train_deep_models.py already existed
# as a real, working CLI for both (schedule_overnight_training() in
# tft_model.py/bilstm_model.py) but had never been run and never wrote a
# registry.json entry — so even a successful run was invisible to this
# job's overdue-check. Fixed same day: both schedule_overnight_training()
# functions now return {"folds_trained", "last_model_path"}, and
# train_deep_models.py's _update_registry() writes last_trained_date/
# training_interval_days from it (train_all_phase1.py::_save_model's
# convention). One shared module trains both --model tft and --model
# bilstm sequentially (train_deep_models.py --model all, the CLI's
# default) — same shared-script-covers-multiple-registry-keys pattern as
# train_all_phase1 covering 6 models, so the dedup loop below still only
# invokes one subprocess per cycle even though both keys map here. See
# FeatureBacklog.md A38.
#
# Module-level (not local to _trigger_model_retrain) so
# _execute_model_training_job's dedup loop can also read it.
_MODEL_TRAINING_SCRIPT_MAP = {
    "hmm_market": "systems.ml_signal_engine.inference.train_all_phase1",
    "pnd_detector": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_5d": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_21d": "systems.ml_signal_engine.inference.train_all_phase1",
    "meta_labeler": "systems.ml_signal_engine.inference.train_all_phase1",
    "conformal_signal5d": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_63d": "systems.ml_signal_engine.inference.retrain_phase2",
    "multibagger": "systems.ml_signal_engine.inference.train_multibagger",
    "tft": "systems.ml_signal_engine.inference.train_deep_models",
    "bilstm": "systems.ml_signal_engine.inference.train_deep_models",
}

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _trigger_model_retrain(model_name: str) -> None:
    """
    Invoke the appropriate training module for model_name as a subprocess,
    run as `python -m <module>` (not a bare file path) with cwd=repo root —
    every training module imports absolute packages (config, backtest,
    features, systems...) that only resolve when the repo root is on
    sys.path, which `-m` guarantees and a bare script path does not (see
    2026-07-05 "part 2" comment above _MODEL_TRAINING_SCRIPT_MAP).

    Subprocess isolation (not a direct function call) ensures the training
    job doesn't hold DuckDB write locks, does not share memory with the
    scheduler process, and doesn't destabilize APScheduler if it crashes.

    Spec References
    ----------------
    SPEC-MODEL-008, SPEC-SCHED-007.
    """
    import importlib.util
    import subprocess
    import sys

    module = _MODEL_TRAINING_SCRIPT_MAP.get(model_name)
    if module is None:
        logger.warning(f"_trigger_model_retrain: no training module known for '{model_name}' — skipping")
        return
    try:
        spec_found = importlib.util.find_spec(module) is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        spec_found = False
    if not spec_found:
        logger.error(
            f"_trigger_model_retrain: mapped module '{module}' for '{model_name}' does not resolve — "
            "skipping rather than letting subprocess.run fail silently into a caught exception"
        )
        return

    extra_args = []
    if module == "systems.ml_signal_engine.inference.retrain_phase2":
        # ML21 (2026-07-10): the scheduler's unattended retrain is exactly the
        # unbounded, 3-horizons-in-one-process run that OOM-killed the box
        # twice on 2026-07-09. --subprocess-per-horizon runs signal_5d/21d/63d
        # as 3 isolated OS processes so the OS reclaims each horizon's
        # SMOTETomek-oversampled matrix + Optuna/stacking refit memory before
        # the next horizon starts, instead of it accumulating in one process's
        # RSS for the whole run.
        extra_args = ["--subprocess-per-horizon"]

    try:
        result = subprocess.run(
            [sys.executable, "-m", module, *extra_args],
            cwd=str(_REPO_ROOT),
            capture_output=False,
            timeout=3600 * 8,  # 8-hour hard cap per model (within 23-hour window)
        )
        if result.returncode != 0:
            logger.error(f"_trigger_model_retrain: '{model_name}' script exited with code {result.returncode}")
        else:
            logger.info(f"_trigger_model_retrain: '{model_name}' retrain completed successfully")
    except subprocess.TimeoutExpired:
        logger.error(f"_trigger_model_retrain: '{model_name}' exceeded 8-hour timeout")
    except Exception as exc:
        logger.error(f"_trigger_model_retrain: '{model_name}' failed to start: {exc}")


def trigger_stacking_ensemble_retrain(
    dry_run: bool = True, timeout_seconds: int = 3600 * 8, output_dir: Optional[str] = None,
) -> int:
    """
    A40 (2026-07-13) — StackingEnsemble's own subprocess-isolation trigger,
    mirroring `_trigger_model_retrain`'s `python -m <module>` / cwd=repo-root
    / timeout pattern (ML21) rather than calling `scripts.train_stacking`'s
    `train_stacking()` in-process: scoring all 5 M-13 base models (3 heavy
    BacktestEngine OOF passes + 2 deep-model forward passes) in the same
    process as the caller is the exact "everything in one process" shape
    that silently OOM-killed the one real 2026-07-02 run (see A40's note in
    scripts/train_stacking.py's module docstring) — an isolated OS process
    lets the kernel reclaim all of that memory regardless of any lingering
    Python references, and a SIGKILL here only takes down the training
    subprocess, not the scheduler itself.

    Deliberately **not** registered in `_MODEL_TRAINING_SCRIPT_MAP` (so it is
    not yet picked up by the weekly overdue-retrain check) — per A40's
    2026-07-10 decision, StackingEnsemble is not yet trusted to run
    unattended (its own OOM history + A42's still-partial TFT/BiLSTM feature
    audit). This function exists so the subprocess-isolation call path can
    be wired and exercised (e.g. via `dry_run=True`, which passes
    `scripts.train_stacking`'s own `--dry-run` flag and never runs the real
    training job) ahead of that trust decision, without an operator having
    to hand-invoke `python -m scripts.train_stacking` to prove it out.

    Parameters
    ----------
    dry_run : bool
        If True (default — the safe choice for exercising this path),
        passes `--dry-run` through to `scripts/train_stacking.py`, which
        only verifies argument parsing/module resolution/status-marker
        writes and returns without training or touching any DB.
    timeout_seconds : int
        Hard subprocess timeout (default matches `_trigger_model_retrain`'s
        8-hour cap).
    output_dir : str, optional
        Passed through as `--output-dir` (default None leaves
        `scripts/train_stacking.py`'s own default, `datastore/models`).
        Tests pass a `tmp_path` here so even the dry-run's STARTED/
        COMPLETED status-marker JSON never lands under the real repo's
        `datastore/models/`.

    Returns
    -------
    int
        The subprocess's return code (0 on success).
    """
    import subprocess
    import sys

    module = "scripts.train_stacking"
    extra_args = ["--dry-run"] if dry_run else []
    if output_dir is not None:
        extra_args += ["--output-dir", output_dir]
    try:
        result = subprocess.run(
            [sys.executable, "-m", module, *extra_args],
            cwd=str(_REPO_ROOT),
            capture_output=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            logger.error(f"trigger_stacking_ensemble_retrain: exited with code {result.returncode}")
        else:
            logger.info("trigger_stacking_ensemble_retrain: subprocess completed successfully")
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error(f"trigger_stacking_ensemble_retrain: exceeded {timeout_seconds}s timeout")
        return -1


def schedule_model_training(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekly model-training check job (SPEC-SCHED-007).

    Fires at MODEL_TRAINING_SCHEDULE_TIME on MODEL_TRAINING_DAY_OF_WEEK
    (default 12:00 IST Saturday, moved off weekdays 2026-07-07 — a real
    retrain runs 3-4+ hours per model and was contending with the 18:00
    daily pipeline / DuckDB's single-writer lock on trading days; see
    BuildLog.md 2026-07-07). Runs after the Saturday morning
    WEEKEND_FEATURE_BACKFILL_TIME/WEEKEND_FUNDAMENTALS_TIME jobs, with
    markets closed and full CPU/DB available for the rest of the weekend.
    Checks registry.json for overdue models (DEFAULT_TRAINING_INTERVAL_DAYS
    x RETRAIN_OVERDUE_MULTIPLIER) and triggers retraining if needed —
    most Saturdays are a fast no-op. Training runs as subprocesses.

    Spec References
    ----------------
    SPEC-SCHED-007, SPEC-MODEL-008.
    """
    from config.settings import MODEL_TRAINING_DAY_OF_WEEK

    if schedule_time is None:
        from config.settings import MODEL_TRAINING_SCHEDULE_TIME
        schedule_time = MODEL_TRAINING_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_model_training_job,
        CronTrigger(hour=hour, minute=minute, day_of_week=MODEL_TRAINING_DAY_OF_WEEK, timezone="Asia/Kolkata"),
        id="model_training",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Model training check scheduled: {schedule_time} IST ({MODEL_TRAINING_DAY_OF_WEEK})")


# Pipeline & Monitoring Remediation Phase 4 (A52, 2026-07-10): spread model
# training across nightly 11pm-6am windows through the week instead of one
# big Saturday job checking (and potentially training) every model
# back-to-back. Each group maps to one of _MODEL_TRAINING_SCRIPT_MAP's
# distinct underlying scripts, so a given night's job only ever triggers
# ONE training subprocess even if several of its models are overdue —
# same dedup-by-script behavior _execute_model_training_job already had,
# just partitioned by night instead of collapsed into one weekly run.
# Mon-Thu chosen deliberately: weekday nights, well clear of the 18:00
# daily pipeline (11pm is 5 hours after it finishes) and clear of the
# next weekday's own 18:00 run the following evening; Friday/weekend
# nights are left free for the existing weekend_feature_backfill/
# weekend_fundamentals/multibagger_scoring/forensic_scoring jobs
# (schedule_weekend_feature_backfill etc.) so nothing new contends with
# those on the one genuinely free multi-day window.
_MODEL_TRAINING_GROUPS: dict = {
    "phase1": {
        "day_of_week": "mon",
        "models": [
            "hmm_market", "pnd_detector", "signal_5d",
            "signal_21d", "meta_labeler", "conformal_signal5d",
        ],
    },
    "phase2": {"day_of_week": "tue", "models": ["signal_63d"]},
    "multibagger": {"day_of_week": "wed", "models": ["multibagger"]},
    "deep_models": {"day_of_week": "thu", "models": ["tft", "bilstm"]},
}


def _execute_model_training_job_for_group(group_name: str) -> None:
    """
    Top-level, picklable wrapper: checks/trains only `group_name`'s models
    (_MODEL_TRAINING_GROUPS[group_name]) under its own job_id
    ("model_training_{group_name}") so each night's run is independently
    observable in scheduler_heartbeats/job_run_log/the Ops dashboard,
    rather than all sharing the single "model_training" id.

    Parameters
    ----------
    group_name : str
        Must be a key in _MODEL_TRAINING_GROUPS.

    Raises
    ------
    None — delegates to _execute_model_training_job, which already never
    raises (SPEC-SCHED-013).
    """
    group = _MODEL_TRAINING_GROUPS.get(group_name)
    if group is None:
        logger.error(f"_execute_model_training_job_for_group: unknown group '{group_name}' — skipping")
        return
    _execute_model_training_job(model_names=group["models"], job_id=f"model_training_{group_name}")


def schedule_model_training_nightly(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register one nightly cron job per _MODEL_TRAINING_GROUPS entry,
    spreading model-training checks across Mon-Thu nights instead of one
    weekly Saturday job (A52) — an alternative to schedule_model_training,
    not layered on top of it (a caller should register one or the other,
    not both, to avoid double-training the same model in the same week).

    Parameters
    ----------
    scheduler : BackgroundScheduler
    schedule_time : str, optional
        "HH:MM", defaults to config.settings.MODEL_TRAINING_NIGHTLY_TIME
        (23:00 IST) — applied to every group; only the day_of_week
        differs per group.

    Spec References
    ----------------
    SPEC-SCHED-007, SPEC-MODEL-008.
    """
    if schedule_time is None:
        from config.settings import MODEL_TRAINING_NIGHTLY_TIME
        schedule_time = MODEL_TRAINING_NIGHTLY_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    for group_name, group in _MODEL_TRAINING_GROUPS.items():
        scheduler.add_job(
            _execute_model_training_job_for_group,
            CronTrigger(hour=hour, minute=minute, day_of_week=group["day_of_week"], timezone="Asia/Kolkata"),
            id=f"model_training_{group_name}",
            args=[group_name],
            replace_existing=True,
            misfire_grace_time=86400,
            coalesce=True,
        )
        logger.info(
            f"Model training check scheduled: '{group_name}' ({group['models']}) at "
            f"{schedule_time} IST ({group['day_of_week']})"
        )


# ---------------------------------------------------------------------------
# Weekend jobs — feature backfill + fundamentals (SPEC-SCHED-012 extension)
# ---------------------------------------------------------------------------

def _execute_weekend_feature_backfill_job() -> None:
    """
    Saturday morning feature-engineering backfill.

    Runs scripts/feature_backfill_hybrid.py for any trading dates that have
    OHLCV in the DataStore but no corresponding feature Parquet. This catches
    dates that had a compute_features failure during the week (their deps
    are now met after the adjuster has run successfully, but compute_features
    was skipped due to a transient failure).

    The gap-detector-driven daily pipeline already handles most of this, but
    a dedicated weekend run ensures the feature store is fully up-to-date
    before Monday's pipeline.

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).

    Spec References
    ----------------
    SPEC-SCHED-003, SPEC-SCHED-004 (unlimited backfill, oldest-first).
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("weekend_feature_backfill: starting feature Parquet gap scan")
        result = subprocess.run(
            [sys.executable, "scripts/feature_backfill_hybrid.py",
             "--stage2-chunk-size", "400"],
            capture_output=False,
            timeout=3600 * 6,  # 6-hour cap (stage 2 is the slow part)
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"weekend_feature_backfill: script exited with code {result.returncode}")
            _record_heartbeat(
                "weekend_feature_backfill", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("weekend_feature_backfill: completed successfully")
            _record_heartbeat(
                "weekend_feature_backfill", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("weekend_feature_backfill: exceeded 6-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "weekend_feature_backfill", "failed", "timeout after 6h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"weekend_feature_backfill job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "weekend_feature_backfill", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_weekend_feature_backfill(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the Saturday feature-backfill job.

    Fires at WEEKEND_FEATURE_BACKFILL_TIME (default 09:00 IST, Saturday).

    Spec References
    ----------------
    SPEC-SCHED-003, SPEC-SCHED-004.
    """
    if schedule_time is None:
        from config.settings import WEEKEND_FEATURE_BACKFILL_TIME
        schedule_time = WEEKEND_FEATURE_BACKFILL_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_weekend_feature_backfill_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sat", timezone="Asia/Kolkata"),
        id="weekend_feature_backfill",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Weekend feature backfill scheduled: {schedule_time} IST (saturday)")


def _execute_weekend_fundamentals_job() -> None:
    """
    Saturday fundamentals catch-up (Screener.in / Trendlyne).

    Runs scripts/backfill_fundamentals_trendlyne.py to refresh the
    fundamentals store with any new quarterly results published during the
    week. Fires Saturday at WEEKEND_FUNDAMENTALS_TIME so it doesn't compete
    with the weekday daily pipeline for DuckDB write access.

    Non-critical: a failure here does NOT block the following week's
    pipeline. Fundamentals are forward-filled from the most recent available
    quarter (SPEC-PIPE-003 PIT alignment).

    Top-level + picklable.

    Spec References
    ----------------
    SPEC-PIPE-003 (PIT for fundamentals), SPEC-PIPE-007.
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("weekend_fundamentals: starting fundamentals backfill")
        result = subprocess.run(
            [sys.executable, "scripts/backfill_fundamentals_trendlyne.py"],
            capture_output=False,
            timeout=3600 * 4,  # 4-hour cap
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"weekend_fundamentals: script exited with code {result.returncode}")
            _record_heartbeat(
                "weekend_fundamentals", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("weekend_fundamentals: completed successfully")
            _record_heartbeat(
                "weekend_fundamentals", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("weekend_fundamentals: exceeded 4-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "weekend_fundamentals", "failed", "timeout after 4h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"weekend_fundamentals job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "weekend_fundamentals", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def _execute_daily_backup_job() -> None:
    """
    Daily off-machine backup: rclone-syncs the small, non-re-derivable
    subset of datastore/ (normalised, signals, models, paper_trading,
    config — explicitly NOT raw/ or features/, both fully re-derivable)
    to Backblaze B2 via scripts/backup_to_b2.py.

    Runs every day (not just trading days) at BACKUP_SCHEDULE_TIME —
    paper_trading/ state and config/ can change independent of whether
    NSE was open, so a weekday-only schedule would miss those.

    Non-critical: a failure here does NOT block the following day's
    pipeline. If BACKUP_ENABLED is False or BACKBLAZE_* credentials are
    unset (see scripts/backup_to_b2.py's module docstring), this records a
    "skipped" heartbeat, not "failed" — a fresh checkout with no B2
    credentials configured yet is an expected state, not an error.

    Top-level + picklable.
    """
    import subprocess

    from scripts.backup_to_b2 import run_backup

    _t0 = _job_timer_start()
    try:
        results = run_backup()
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if results["failed"]:
            logger.error(f"daily_backup: failed for {results['failed']}, synced {results['synced']}")
            _record_heartbeat(
                "daily_backup", "failed", f"failed dirs: {results['failed']}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info(f"daily_backup: synced {results['synced']}")
            _record_heartbeat(
                "daily_backup", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except RuntimeError as exc:
        # BACKUP_ENABLED is False, or BACKBLAZE_* credentials unset — known,
        # documented gap until backblaze.com setup is done (see
        # run_backup's docstring).
        logger.warning(f"daily_backup skipped: {exc}")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "daily_backup", "skipped", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except FileNotFoundError as exc:
        logger.error(f"daily_backup: rclone binary not found — {exc}")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "daily_backup", "failed", "rclone not installed",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except subprocess.SubprocessError as exc:
        logger.error(f"daily_backup: subprocess error — {exc}")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "daily_backup", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"daily_backup job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "daily_backup", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_daily_backup(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the daily off-machine backup job (2026-07-04 architecture
    review, user decision: rclone to Backblaze B2, daily not weekly —
    switched from an initial Google Drive design; see
    scripts/backup_to_b2.py's module docstring for why).

    Fires at BACKUP_SCHEDULE_TIME (default 22:30 IST) every day of the
    week — unlike the trading-day-only jobs, paper_trading/ and config/
    can change regardless of whether NSE was open.

    Spec References
    ----------------
    SPEC-SYS-005 (Storage Budgets).
    """
    if schedule_time is None:
        from config.settings import BACKUP_SCHEDULE_TIME
        schedule_time = BACKUP_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_daily_backup_job,
        CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
        id="daily_backup",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Daily off-machine backup scheduled: {schedule_time} IST (every day)")


def _execute_job_health_check_job() -> None:
    """
    A21 (Pipeline Health Checker): weekly job-completeness audit. Reads
    job_run_log (populated by every _record_heartbeat call above) to
    confirm every registered job (datastore/health/job_registry.py) fired
    successfully on each calendar date it was expected to in the trailing
    7 days, and records a Finding (pending, never auto-applied) for any
    gap — see datastore/health/runner.py::run_job_health_check.

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013), same as every other
    scheduled job here.
    """
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from datastore.health.runner import run_job_health_check

    _t0 = _job_timer_start()
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            result = run_job_health_check(conn, now_ist().date())
        logger.info(
            f"job_health_check: findings_by_check={result.findings_by_check} "
            f"critical_count={result.critical_count}"
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "job_health_check", "success",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"job_health_check job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "job_health_check", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_job_health_check(
    scheduler: BackgroundScheduler,
    day_of_week: Optional[str] = None,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekly Pipeline Health Checker job (A21).

    Fires at JOB_HEALTH_CHECK_SCHEDULE_TIME (default 11:00 IST,
    JOB_HEALTH_CHECK_DAY_OF_WEEK default Sunday) — after both Saturday's
    weekend batch and Sunday's multibagger/forensic scoring jobs have had
    a chance to record their own job_run_log rows for the week, so this
    audit isn't racing the very jobs it's checking.
    """
    if day_of_week is None:
        from config.settings import JOB_HEALTH_CHECK_DAY_OF_WEEK
        day_of_week = JOB_HEALTH_CHECK_DAY_OF_WEEK
    if schedule_time is None:
        from config.settings import JOB_HEALTH_CHECK_SCHEDULE_TIME
        schedule_time = JOB_HEALTH_CHECK_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_job_health_check_job,
        CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute, timezone="Asia/Kolkata"),
        id="job_health_check",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Job health check scheduled: {schedule_time} IST ({day_of_week})")


def schedule_weekend_fundamentals(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the Saturday fundamentals-backfill job.

    Fires at WEEKEND_FUNDAMENTALS_TIME (default 10:30 IST, Saturday) —
    after weekend_feature_backfill has had 90 minutes to run.

    Spec References
    ----------------
    SPEC-PIPE-003.
    """
    if schedule_time is None:
        from config.settings import WEEKEND_FUNDAMENTALS_TIME
        schedule_time = WEEKEND_FUNDAMENTALS_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_weekend_fundamentals_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sat", timezone="Asia/Kolkata"),
        id="weekend_fundamentals",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Weekend fundamentals backfill scheduled: {schedule_time} IST (saturday)")


def _execute_promoter_pledge_backfill_job() -> None:
    """
    Saturday promoter-pledge catch-up (NSE SAST Reg 31(4) disclosures).

    Runs scripts/backfill_promoter_pledge_nse.py so newly-disclosed
    pledge/encumbrance events are picked up on a recurring cadence instead
    of only whenever someone remembers to run the script by hand (A54,
    2026-07-10) — see that script's module docstring for the real,
    live-verified (2026-07-07) NSE endpoint it reads.

    Non-critical: a failure here does NOT block the following week's
    pipeline.

    Top-level + picklable.
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("promoter_pledge_backfill: starting")
        result = subprocess.run(
            [sys.executable, "scripts/backfill_promoter_pledge_nse.py"],
            capture_output=False,
            timeout=3600 * 4,  # 4-hour cap — per-ticker HTTP loop over the full universe
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"promoter_pledge_backfill: script exited with code {result.returncode}")
            _record_heartbeat(
                "promoter_pledge_backfill", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("promoter_pledge_backfill: completed successfully")
            _record_heartbeat(
                "promoter_pledge_backfill", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("promoter_pledge_backfill: exceeded 4-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "promoter_pledge_backfill", "failed", "timeout after 4h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"promoter_pledge_backfill job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "promoter_pledge_backfill", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_promoter_pledge_backfill(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the Saturday promoter-pledge backfill job (A54).

    Fires at PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME (default 11:00 IST,
    Saturday) — after weekend_fundamentals (10:30) has refreshed the base
    shareholding rows this backfill enriches.
    """
    if schedule_time is None:
        from config.settings import PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME
        schedule_time = PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_promoter_pledge_backfill_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sat", timezone="Asia/Kolkata"),
        id="promoter_pledge_backfill",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Promoter pledge backfill scheduled: {schedule_time} IST (saturday)")


def _execute_balance_sheet_backfill_job() -> None:
    """
    Saturday balance-sheet catch-up (Screener.in cached pages).

    Runs scripts/backfill_balance_sheet_from_screener.py so
    total_assets/cwip (and their consumers cwip_ratio/asset_inflation_flag)
    get refreshed on a recurring cadence instead of only whenever someone
    remembers to run the script by hand (A54, 2026-07-10).

    Non-critical: a failure here does NOT block the following week's
    pipeline.

    Top-level + picklable.
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("balance_sheet_backfill: starting")
        result = subprocess.run(
            [sys.executable, "scripts/backfill_balance_sheet_from_screener.py"],
            capture_output=False,
            timeout=3600 * 2,  # 2-hour cap — reads from the existing local cache, no network
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"balance_sheet_backfill: script exited with code {result.returncode}")
            _record_heartbeat(
                "balance_sheet_backfill", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("balance_sheet_backfill: completed successfully")
            _record_heartbeat(
                "balance_sheet_backfill", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("balance_sheet_backfill: exceeded 2-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "balance_sheet_backfill", "failed", "timeout after 2h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"balance_sheet_backfill job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "balance_sheet_backfill", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_balance_sheet_backfill(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the Saturday balance-sheet backfill job (A54).

    Fires at BALANCE_SHEET_BACKFILL_SCHEDULE_TIME (default 11:30 IST,
    Saturday) — after promoter_pledge_backfill (11:00) and before
    model_training (12:00).
    """
    if schedule_time is None:
        from config.settings import BALANCE_SHEET_BACKFILL_SCHEDULE_TIME
        schedule_time = BALANCE_SHEET_BACKFILL_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_balance_sheet_backfill_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sat", timezone="Asia/Kolkata"),
        id="balance_sheet_backfill",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Balance sheet backfill scheduled: {schedule_time} IST (saturday)")


# ---------------------------------------------------------------------------
# FutureDevelopment.md #14 — weekly multibagger + forensic scoring
# ---------------------------------------------------------------------------

def _execute_multibagger_scoring_job() -> None:
    """
    Weekly full-universe multibagger scoring (M-08).

    score_multibagger.py (systems/ml_signal_engine/inference/) was, until
    2026-07-04, operator-CLI only — its `main()` entrypoint was never
    invoked by anything scheduled, so ml_multibagger only ever had rows
    from manual runs. Invoked here as a subprocess (same isolation
    rationale as _trigger_model_retrain: don't hold DuckDB write locks or
    share memory with the long-lived scheduler process) against the full
    universe, no --limit.

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013).
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("multibagger_scoring: starting full-universe scoring run")
        result = subprocess.run(
            [sys.executable, "-m", "systems.ml_signal_engine.inference.score_multibagger"],
            capture_output=False,
            timeout=3600 * 2,  # 2-hour cap — full universe, real OHLCV fetches per ticker
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"multibagger_scoring: script exited with code {result.returncode}")
            _record_heartbeat(
                "multibagger_scoring", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("multibagger_scoring: completed successfully")
            _record_heartbeat(
                "multibagger_scoring", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("multibagger_scoring: exceeded 2-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "multibagger_scoring", "failed", "timeout after 2h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"multibagger_scoring job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "multibagger_scoring", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_multibagger_scoring(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekly multibagger scoring job (FutureDevelopment.md #14).

    Fires at MULTIBAGGER_SCORING_SCHEDULE_TIME (default 09:30 IST, Sunday)
    — markets closed, no contention with the weekday daily pipeline or the
    Saturday feature-backfill/fundamentals jobs.
    """
    if schedule_time is None:
        from config.settings import MULTIBAGGER_SCORING_SCHEDULE_TIME
        schedule_time = MULTIBAGGER_SCORING_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_multibagger_scoring_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sun", timezone="Asia/Kolkata"),
        id="multibagger_scoring",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Multibagger scoring scheduled: {schedule_time} IST (sunday)")


def _execute_forensic_scoring_job() -> None:
    """
    Weekly full-universe forensic risk scoring (M-09/M-10).

    score_forensic.py (systems/ml_signal_engine/inference/) was, until
    2026-07-04, operator-CLI only — never invoked by anything scheduled,
    so ml_forensic only ever had rows from manual runs. Same subprocess
    isolation as _execute_multibagger_scoring_job.

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013).
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("forensic_scoring: starting full-universe scoring run")
        result = subprocess.run(
            [sys.executable, "-m", "systems.ml_signal_engine.inference.score_forensic"],
            capture_output=False,
            timeout=3600 * 2,  # 2-hour cap
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"forensic_scoring: script exited with code {result.returncode}")
            _record_heartbeat(
                "forensic_scoring", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("forensic_scoring: completed successfully")
            _record_heartbeat(
                "forensic_scoring", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("forensic_scoring: exceeded 2-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "forensic_scoring", "failed", "timeout after 2h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"forensic_scoring job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "forensic_scoring", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_forensic_scoring(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekly forensic scoring job (FutureDevelopment.md #14).

    Fires at FORENSIC_SCORING_SCHEDULE_TIME (default 10:00 IST, Sunday) —
    30 minutes after multibagger_scoring, avoiding both hitting DuckDB
    write paths at the exact same moment.
    """
    if schedule_time is None:
        from config.settings import FORENSIC_SCORING_SCHEDULE_TIME
        schedule_time = FORENSIC_SCORING_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_forensic_scoring_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sun", timezone="Asia/Kolkata"),
        id="forensic_scoring",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Forensic scoring scheduled: {schedule_time} IST (sunday)")


def _execute_nse_xbrl_fundamentals_job() -> None:
    """
    Weekly full-universe scan for newly-published NSE Integrated Filing —
    IndAS regulatory disclosures (2026-07-08, per explicit operator
    instruction: "has to be a daily/weekly scanner to look for newly
    published data").

    Runs scripts/backfill_fundamentals_nse_xbrl.py, which is fully
    idempotent (COALESCE upsert keyed on (ticker, fiscal_year, quarter) —
    see that script's module docstring) — safe to re-run every week over
    the whole universe; only genuinely new/changed filings result in a
    write. Weekly rather than daily because company filings are quarterly
    events staggered across each quarter, not a daily-changing feed — a
    week is short enough that no newly-published filing sits unindexed
    for long, without re-scanning the full ~2,700-ticker universe daily
    for what is usually zero new filings on any given day.

    Same subprocess isolation as _execute_forensic_scoring_job.

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).

    Raises
    ------
    None — wrapped in try/except (SPEC-SCHED-013).
    """
    import subprocess
    import sys

    _t0 = _job_timer_start()
    try:
        logger.info("nse_xbrl_fundamentals: starting full-universe filing scan")
        result = subprocess.run(
            [sys.executable, "scripts/backfill_fundamentals_nse_xbrl.py"],
            capture_output=False,
            timeout=3600 * 4,  # 4-hour cap — full universe took ~2-3h in testing
        )
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        if result.returncode != 0:
            logger.error(f"nse_xbrl_fundamentals: script exited with code {result.returncode}")
            _record_heartbeat(
                "nse_xbrl_fundamentals", "failed", f"exit code {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
        else:
            logger.info("nse_xbrl_fundamentals: completed successfully")
            _record_heartbeat(
                "nse_xbrl_fundamentals", "success",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
    except subprocess.TimeoutExpired:
        logger.error("nse_xbrl_fundamentals: exceeded 4-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "nse_xbrl_fundamentals", "failed", "timeout after 4h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
    except Exception as exc:
        logger.error(f"nse_xbrl_fundamentals job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "nse_xbrl_fundamentals", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )


def schedule_nse_xbrl_fundamentals(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekly NSE Integrated Filing (IndAS) fundamentals scan.

    Fires at NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME (default 05:00 IST,
    Saturday) — [REVISED 2026-07-08] moved from an earlier Sunday-after-
    forensic-scoring slot per explicit operator instruction: this must run
    AHEAD OF forensic scoring, valuation modeling, and every other model
    that reads `fundamentals`, not after. A full-universe scan is a real
    ~2-3h run, so it needs a multi-hour head start, not a same-morning
    30-minute gap — 05:00 Saturday gives it until markets/other jobs need
    the data (weekend_feature_backfill 09:00, weekend_fundamentals 10:30,
    model_training 12:00, all Saturday; multibagger_scoring/
    forensic_scoring 09:30/10:00 the FOLLOWING Sunday) — a single early run
    covers the entire weekend batch rather than needing two slots.
    weekend_fundamentals (Screener/Trendlyne — the FALLBACK source per
    ingestion/scrapers/nse_xbrl_financials.py's module docstring) runs
    after this deliberately, so it only ever fills gaps this scan's
    primary source didn't cover.
    """
    if schedule_time is None:
        from config.settings import NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME
        schedule_time = NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_nse_xbrl_fundamentals_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="sat", timezone="Asia/Kolkata"),
        id="nse_xbrl_fundamentals",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"NSE XBRL fundamentals scan scheduled: {schedule_time} IST (saturday)")


def _execute_emergency_recompute_job(
    from_date: Optional[str] = None, ticker_batch_size: int = 150,
    start_batch_idx: int = 0, start_stage: str = "stage1",
) -> None:
    """
    One-off emergency feature-cache recompute + full model retrain chain.

    Reusable job for whenever a retroactive price-history correction (e.g.
    a corporate-action adjuster fix) invalidates cached features/models.
    Not on a recurring cadence — registered as a single DateTrigger job by
    schedule_emergency_recompute() and consumed once, same subprocess-
    isolation pattern as the other model-training jobs.

    Added 2026-07-04 after the price_adjuster.py SPLIT/BONUS corporate-
    action fix (129 defects across 246 tickers, later found to be 424
    tickers once missing-split backfill was included): historical adjusted
    OHLCV changed, so every price-derived feature and every model trained
    on those features needs recomputing/retraining.

    Stage 1 batching (added 2026-07-05): running --all-db-tickers in one
    process OOM-killed on a 14 GB machine — the upfront BackfillDataCache
    preload (fundamentals/shareholding/corp_actions for the full ~2487-
    ticker active universe) alone pushed memory pressure past the
    systemd-oomd threshold before Stage 1 compute even got going. Each
    batch now runs in its own subprocess so memory is fully released
    between batches; per-ticker staging parquets make this resumable if a
    batch itself fails (Stage 1's own "already staged" skip logic, which
    is NOT gated on --force — that flag only affects Stage 2/date-level
    recompute, so IMPORTANT: any stale pre-fix staging directory must be
    cleared/moved aside before a correctness-critical rerun, or Stage 1
    will silently reuse pre-correction cached features).

    Parameters
    ----------
    from_date : str, optional
        YYYY-MM-DD. Defaults to feature_backfill_hybrid.py's own default
        (2007-01-03, i.e. full history) if not given.
    ticker_batch_size : int
        Tickers per Stage 1 subprocess (default 150 — safe on a 14 GB
        machine with ~8 GB free; lower further on tighter memory).

    Top-level + picklable (APScheduler SQLAlchemyJobStore requirement).
    """
    import json
    import subprocess
    import sys
    from pathlib import Path

    from datastore.api.db import get_duckdb_connection
    from config.settings import DUCKDB_PATH

    MODEL_NAMES = (
        "signal_5d", "signal_21d", "signal_63d", "tft", "bilstm",
        "multibagger", "hmm_market", "pnd_detector",
    )
    progress_path = Path("datastore/logs/emergency_recompute_progress.json")
    progress_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_progress(**fields) -> None:
        """Persist scope-vs-completed status so it can be checked without log-parsing."""
        state = {}
        if progress_path.exists():
            try:
                state = json.loads(progress_path.read_text())
            except Exception:
                state = {}
        state.update(fields)
        state["updated_at"] = now_ist().isoformat()
        progress_path.write_text(json.dumps(state, indent=2))

    _t0 = _job_timer_start()
    try:
        if start_stage == "stage1":
            logger.info("emergency_recompute: Stage 1 — per-ticker staging recompute (batched)")
            with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
                n_active = conn.execute("""
                    SELECT count(DISTINCT ticker) FROM ohlcv_adjusted
                    WHERE date >= (SELECT CAST(MAX(date) - INTERVAL 30 DAYS AS DATE) FROM ohlcv_adjusted)
                """).fetchone()[0]
            n_batches = (n_active + ticker_batch_size - 1) // ticker_batch_size
            logger.info(f"emergency_recompute: {n_active} active tickers -> {n_batches} batches of {ticker_batch_size}")
            _write_progress(
                stage="stage1", stage1_batches_total=n_batches, stage1_batches_done=start_batch_idx,
                active_tickers=n_active, stage2_done=False,
                models_total=len(MODEL_NAMES), models_done=[],
            )

            for batch_idx in range(start_batch_idx, n_batches):
                stage1_cmd = [
                    sys.executable, "scripts/feature_backfill_hybrid.py",
                    "--all-db-tickers", "--active-only", "--force",
                    "--ticker-batch-size", str(ticker_batch_size),
                    "--ticker-batch-index", str(batch_idx),
                    "--workers", "3",
                    # REQUIRED with --all-db-tickers: main() sets ohlcv_by_ticker={}
                    # unconditionally whenever --all-db-tickers is passed (comment:
                    # "workers load from DuckDB directly, so skip the pre-load ...").
                    # The sequential (--workers 1, the default) path then does
                    # ohlcv_by_ticker.get(ticker, pd.DataFrame()) -> an EMPTY frame
                    # for every ticker, silently NaN-ing every price-derived
                    # feature. Only the >1-worker path (_stage1_ticker) loads OHLCV
                    # itself per ticker from DuckDB. Found 2026-07-05 after a full
                    # batched Stage 1 run completed "successfully" with rsi_14 etc.
                    # 100% null for every ticker's entire history.
                ]
                if from_date:
                    stage1_cmd += ["--from-date", from_date]
                logger.info(f"emergency_recompute: Stage 1 batch {batch_idx + 1}/{n_batches}")
                result = subprocess.run(stage1_cmd, capture_output=False, timeout=3600 * 2)
                if result.returncode != 0:
                    logger.error(
                        f"emergency_recompute: Stage 1 batch {batch_idx + 1}/{n_batches} exited {result.returncode}"
                    )
                    duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
                    _record_heartbeat(
                        "emergency_recompute", "failed",
                        f"stage1 batch {batch_idx + 1}/{n_batches} exit {result.returncode}",
                        duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
                    )
                    _write_progress(stage="stage1_failed", stage1_batches_done=batch_idx,
                                     error=f"batch {batch_idx + 1}/{n_batches} exit {result.returncode}")
                    return
                _write_progress(stage="stage1", stage1_batches_done=batch_idx + 1)

        logger.info("emergency_recompute: Stage 2 — rebuilding daily parquets from staging")
        _write_progress(stage="stage2")
        stage2_cmd = [
            sys.executable, "scripts/feature_backfill_hybrid.py",
            "--rebuild-daily", "--all-db-tickers", "--active-only", "--force",
            "--stage2-chunk-size", "150",
        ]
        if from_date:
            stage2_cmd += ["--from-date", from_date]
        result = subprocess.run(stage2_cmd, capture_output=False, timeout=3600 * 8)
        if result.returncode != 0:
            logger.error(f"emergency_recompute: Stage 2 exited {result.returncode}")
            duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
            _record_heartbeat(
                "emergency_recompute", "failed", f"stage2 exit {result.returncode}",
                duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
            )
            _write_progress(stage="stage2_failed", error=f"stage2 exit {result.returncode}")
            return
        _write_progress(stage="retrain", stage2_done=True)

        logger.info("emergency_recompute: feature cache done — retraining all price-derived models")
        models_done = []
        for model_name in MODEL_NAMES:
            _trigger_model_retrain(model_name)
            models_done.append(model_name)
            _write_progress(stage="retrain", models_done=list(models_done))

        logger.info("emergency_recompute: complete")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "emergency_recompute", "success",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
        _write_progress(stage="complete")
    except subprocess.TimeoutExpired:
        logger.error("emergency_recompute: feature backfill exceeded 8-hour timeout")
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "emergency_recompute", "failed", "timeout after 8h",
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
        _write_progress(stage="failed", error="timeout after 8h")
    except Exception as exc:
        logger.error(f"emergency_recompute job raised an unexpected exception: {exc}", exc_info=True)
        duration_seconds, peak_rss_mb = _job_timer_stats(_t0)
        _record_heartbeat(
            "emergency_recompute", "failed", str(exc),
            duration_seconds=duration_seconds, peak_rss_mb=peak_rss_mb,
        )
        _write_progress(stage="failed", error=str(exc))


def schedule_emergency_recompute(
    scheduler: BackgroundScheduler,
    run_at=None,
    from_date: Optional[str] = None,
    job_id: Optional[str] = None,
) -> str:
    """
    Register a one-off (non-recurring) emergency recompute+retrain job.

    Unlike every other schedule_* function here, this is not part of the
    regular cadence — it's a durable, reusable mechanism for the "we just
    corrected historical prices for N tickers, now the feature cache and
    every downstream model are stale" situation. Persisted to the same
    SQLAlchemyJobStore as the recurring jobs (so it survives a scheduler
    restart) but uses a DateTrigger, so it fires exactly once and is then
    removed automatically by APScheduler — it does not alter or replace any
    of the recurring jobs' registrations.

    Parameters
    ----------
    scheduler : BackgroundScheduler
    run_at : datetime, optional
        When to fire. Defaults to ~10 seconds from now (i.e. "run it now").
    from_date : str, optional
        Passed through to _execute_emergency_recompute_job.
    job_id : str, optional
        Defaults to f"emergency_recompute_{today's date}". Pass an explicit
        id to reinstate/re-trigger a previous emergency run under a new
        timestamp.

    Returns
    -------
    str
        The job id that was registered.
    """
    from datetime import timedelta

    from apscheduler.triggers.date import DateTrigger

    if run_at is None:
        run_at = now_ist() + timedelta(seconds=10)
    if job_id is None:
        job_id = f"emergency_recompute_{now_ist().strftime('%Y%m%d_%H%M%S')}"

    scheduler.add_job(
        _execute_emergency_recompute_job,
        DateTrigger(run_date=run_at, timezone="Asia/Kolkata"),
        args=[from_date],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=86400,
    )
    logger.info(f"Emergency recompute scheduled: job_id={job_id} run_at={run_at}")
    return job_id
