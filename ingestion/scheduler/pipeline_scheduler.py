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

import logging
from datetime import date as date_type
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from ingestion.scheduler.checkpoint import STEP_NAMES, STEPS, CheckpointManager
from ingestion.scheduler.gap_detector import detect_gaps, is_trading_day

logger = logging.getLogger(__name__)

# Pre-compute the depends_on lookup for fast access in run_steps_for_date.
# {step_name: [dep_name, ...]} — empty list means no hard prerequisites.
_STEP_DEPS: dict = {step["name"]: step.get("depends_on", []) for step in STEPS}

_INSERT_PIPELINE_RUN = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, ?, ?, ?, ?)
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
                run_date, step_name, status="skipped", error_message=reason
            )
            logger.warning(
                f"Skipping '{step_name}' for {run_date} — {reason}"
            )
            continue

        checkpoint_manager.save_checkpoint(run_date, step_name, status="running")
        try:
            step_runner(run_date, step_name)
        except Exception as exc:
            checkpoint_manager.save_checkpoint(
                run_date, step_name, status="failed", error_message=str(exc)
            )
            logger.error(f"Step '{step_name}' failed for {run_date}: {exc}")
            any_step_failed = True
            # Do NOT return immediately — continue evaluating later steps
            # whose dependencies may still be fully met (SPEC-SCHED-011).
        else:
            checkpoint_manager.save_checkpoint(run_date, step_name, status="success")
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


def _record_pipeline_run(
    run_date: date_type,
    success: bool,
    started_at: datetime,
    db_path: Optional[Path] = None,
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

    with get_sqlite_connection(db_path) as conn:
        conn.execute(
            _INSERT_PIPELINE_RUN,
            (
                run_date.isoformat(),
                started_at.isoformat(),
                now_ist().isoformat(),
                "success" if success else "failed",
                0,  # stocks_processed: not threaded through StepRunner's fire-and-forget contract yet
                None,
            ),
        )
        conn.commit()


def _record_heartbeat(
    job_id: str,
    status: str,
    error: Optional[str] = None,
    db_path: Optional[Path] = None,
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

    started_at = now_ist()
    ok = run_steps_for_date(today, step_runner, checkpoint_manager, is_backfill=False)
    _record_pipeline_run(today, ok, started_at, db_path)
    return ok


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
    try:
        ok = run_startup_sequence(step_runner, checkpoint_manager, today=now_ist().date())
        error = None if ok else "pipeline run returned False"
        _record_heartbeat(job_id, "success" if ok else "failed", error)
    except Exception as exc:
        logger.error(f"{job_id} job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat(job_id, "failed", str(exc))


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
            _record_heartbeat("backfill_catchup", "skipped", skip_reason)
            return

        to_date = now_ist().date()
        from_date = to_date - timedelta(days=365 * BACKFILL_YEARS)
        tickers = get_tickers()
        logger.info(f"Backfill catch-up starting: {len(tickers)} universe tickers, {from_date}..{to_date}")
        run_backfill(tickers, from_date.isoformat(), to_date.isoformat())
        _record_heartbeat("backfill_catchup", "success")
    except Exception as exc:
        logger.error(f"backfill_catchup job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat("backfill_catchup", "failed", str(exc))


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
    Register a second recurring trigger for the same catch-up-then-today
    logic as schedule_daily_pipeline, fired earlier in the day (2026-07,
    SPEC-SCHED-014 follow-up).

    Why a second trigger of the exact same function rather than a new one:
    _execute_daily_job's real value on any given firing is almost always
    the gap-backfill it runs first (run_startup_sequence walks every
    trading day since the last recorded success and re-attempts each one)
    — "today" itself will still 404 at 07:30 IST since NSE typically
    doesn't publish a trading day's bhavcopy until after that day's own
    market close. This exists so a step that failed on an earlier date
    (e.g. download_fno/download_macro/download_corporate_actions/
    download_large_deals hitting a transient network error) gets retried
    hours sooner than waiting for the 18:00 IST run, rather than sitting
    visibly "never run" on the Ops page until evening. NSE-sourced only,
    same as the main job — no FYERS dependency (contrast with the removed
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
        _execute_daily_job,
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
    APScheduler job target for the twice-monthly MF-holdings ingestion
    (SPEC-SCHED-009, P2.2 — pivoted to Groww as primary source). Module-
    level function, not a closure/lambda — SQLAlchemyJobStore must be
    able to pickle it (same constraint documented on _execute_daily_job).

    Registers every Groww-listed AMC (a real network call — AMC_REGISTRY
    starts empty for Groww until this is called, see SPEC-MFHOLD-001),
    imports sbi_mf_holdings (triggers its zero-cost auto-registration),
    determines which month Groww's live snapshot actually represents
    (never assumes — see _determine_groww_live_snapshot_month), then
    ingests that month for every registered AMC (Groww's 49 + SBI's
    direct Excel cross-check, which supports the same historical month
    since it has a real archive).

    Fires twice a month (config.settings.MF_HOLDINGS_SCHEDULE_DAYS) rather
    than once: AMC disclosure timing varies, and Groww's "current
    snapshot" can change mid-cycle — two checks per month make it very
    unlikely a given month's snapshot is missed entirely between visits.
    save_monthly_parquet's merge-not-overwrite behavior (P2.2 continued)
    makes re-ingesting the same month on the second visit safe — it just
    refreshes rows for schemes whose data has changed, never duplicates.
    """
    import ingestion.scrapers.sbi_mf_holdings  # noqa: F401 (import for its registration side effect)
    from ingestion.scrapers.amfi_holdings import run_monthly_ingestion
    from ingestion.scrapers.groww_mf_holdings import register_all_amcs

    try:
        register_all_amcs()
        year, month = _determine_groww_live_snapshot_month()
        run_monthly_ingestion(year, month)
        _record_heartbeat("mf_holdings_ingestion", "success")
    except RuntimeError as exc:
        # AMC_REGISTRY empty (no real source configured yet) — a known,
        # documented gap, not an unexpected failure. Recorded as
        # "skipped", not "failed".
        logger.warning(f"mf_holdings_ingestion skipped: {exc}")
        _record_heartbeat("mf_holdings_ingestion", "skipped", str(exc))
    except Exception as exc:
        logger.error(f"mf_holdings_ingestion job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat("mf_holdings_ingestion", "failed", str(exc))


def schedule_mf_holdings_ingestion(
    scheduler: BackgroundScheduler,
    days: Optional[str] = None,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the recurring twice-monthly MF-holdings ingestion job
    (SPEC-SCHED-009 — laptop-only APScheduler job store, not a separate
    Oracle/OS-level cron entry, same precedent as schedule_backfill_catchup).

    Parameters
    ----------
    scheduler : BackgroundScheduler
    days : str, optional
        Cron day-of-month field, e.g. "5,20" for twice a month. Defaults
        to config.settings.MF_HOLDINGS_SCHEDULE_DAYS.
    schedule_time : str, optional
        "HH:MM". Defaults to config.settings.AMFI_SCHEDULE_TIME (08:00 IST).

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
    if days is None:
        from config.settings import MF_HOLDINGS_SCHEDULE_DAYS

        days = MF_HOLDINGS_SCHEDULE_DAYS
    if schedule_time is None:
        from config.settings import AMFI_SCHEDULE_TIME

        schedule_time = AMFI_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))

    scheduler.add_job(
        _execute_mf_holdings_job,
        CronTrigger(day=days, hour=hour, minute=minute, timezone="Asia/Kolkata"),
        id="mf_holdings_ingestion",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"MF holdings ingestion scheduled: days={days}, time={schedule_time} IST")


# ---------------------------------------------------------------------------
# Model training job (SPEC-SCHED-007) — weekday, after daily pipeline
# ---------------------------------------------------------------------------

def _execute_model_training_job() -> None:
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

    from config.settings import MODELS_DIR, RETRAIN_OVERDUE_MULTIPLIER

    try:
        registry_path = Path(MODELS_DIR) / "registry.json"
        if not registry_path.exists():
            logger.info("model_training: registry.json not found — no trained models yet, skipping")
            _record_heartbeat("model_training", "skipped", "registry.json not found")
            return

        with registry_path.open() as f:
            registry = json.load(f)

        today = now_ist().date()
        overdue_models = []
        for model_name, meta in registry.items():
            last_train_str = meta.get("last_trained_date")
            interval_days = meta.get("training_interval_days", 30)
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
            logger.info("model_training: no models overdue — skipping")
            _record_heartbeat("model_training", "skipped", "no models overdue")
            return

        logger.info(f"model_training: {len(overdue_models)} model(s) overdue: {overdue_models}")
        for model_name, reason in overdue_models:
            logger.info(f"  Queuing retrain for '{model_name}' ({reason})")
            # Phase 3 retrain protocol (SPEC-MODEL-008): snapshot → train →
            # shadow-test → compare → promote.  The actual training scripts
            # (scripts/train_*.py) are invoked as subprocess calls here so
            # they run in their own process and don't hold DuckDB write locks
            # for the life of the scheduler process.
            _trigger_model_retrain(model_name)

        _record_heartbeat("model_training", "success")

    except Exception as exc:
        logger.error(f"model_training job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat("model_training", "failed", str(exc))


def _trigger_model_retrain(model_name: str) -> None:
    """
    Invoke the appropriate training script for model_name as a subprocess.

    Subprocess isolation (not a direct function call) ensures the training
    job doesn't hold DuckDB write locks, does not share memory with the
    scheduler process, and doesn't destabilize APScheduler if it crashes.

    Spec References
    ----------------
    SPEC-MODEL-008, SPEC-SCHED-007.
    """
    import subprocess
    import sys

    script_map = {
        "signal_5d": "scripts/run_phase1_backtest.py",
        "signal_21d": "scripts/run_phase1_backtest.py",
        "signal_63d": "scripts/run_phase2_backtest.py",
        "tft": "scripts/train_tft.py",
        "bilstm": "scripts/train_bilstm.py",
        "multibagger": "systems/ml_signal_engine/models/multibagger/multibagger_model.py",
    }
    script = script_map.get(model_name)
    if script is None:
        logger.warning(f"_trigger_model_retrain: no training script known for '{model_name}' — skipping")
        return

    try:
        result = subprocess.run(
            [sys.executable, script],
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


def schedule_model_training(
    scheduler: BackgroundScheduler,
    schedule_time: Optional[str] = None,
) -> None:
    """
    Register the weekday model-training check job (SPEC-SCHED-007).

    Fires at MODEL_TRAINING_SCHEDULE_TIME (default 20:00 IST, mon-fri) —
    after the 18:00 daily pipeline is expected to have completed. Checks
    registry.json for overdue models and triggers retraining if needed.
    Training runs as subprocesses within the 23-hour window.

    Spec References
    ----------------
    SPEC-SCHED-007, SPEC-MODEL-008.
    """
    if schedule_time is None:
        from config.settings import MODEL_TRAINING_SCHEDULE_TIME
        schedule_time = MODEL_TRAINING_SCHEDULE_TIME

    hour, minute = (int(part) for part in schedule_time.split(":"))
    scheduler.add_job(
        _execute_model_training_job,
        CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="model_training",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Model training check scheduled: {schedule_time} IST (mon-fri)")


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

    try:
        logger.info("weekend_feature_backfill: starting feature Parquet gap scan")
        result = subprocess.run(
            [sys.executable, "scripts/feature_backfill_hybrid.py",
             "--stage2-chunk-size", "400"],
            capture_output=False,
            timeout=3600 * 6,  # 6-hour cap (stage 2 is the slow part)
        )
        if result.returncode != 0:
            logger.error(f"weekend_feature_backfill: script exited with code {result.returncode}")
            _record_heartbeat("weekend_feature_backfill", "failed", f"exit code {result.returncode}")
        else:
            logger.info("weekend_feature_backfill: completed successfully")
            _record_heartbeat("weekend_feature_backfill", "success")
    except subprocess.TimeoutExpired:
        logger.error("weekend_feature_backfill: exceeded 6-hour timeout")
        _record_heartbeat("weekend_feature_backfill", "failed", "timeout after 6h")
    except Exception as exc:
        logger.error(f"weekend_feature_backfill job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat("weekend_feature_backfill", "failed", str(exc))


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

    try:
        logger.info("weekend_fundamentals: starting fundamentals backfill")
        result = subprocess.run(
            [sys.executable, "scripts/backfill_fundamentals_trendlyne.py"],
            capture_output=False,
            timeout=3600 * 4,  # 4-hour cap
        )
        if result.returncode != 0:
            logger.error(f"weekend_fundamentals: script exited with code {result.returncode}")
            _record_heartbeat("weekend_fundamentals", "failed", f"exit code {result.returncode}")
        else:
            logger.info("weekend_fundamentals: completed successfully")
            _record_heartbeat("weekend_fundamentals", "success")
    except subprocess.TimeoutExpired:
        logger.error("weekend_fundamentals: exceeded 4-hour timeout")
        _record_heartbeat("weekend_fundamentals", "failed", "timeout after 4h")
    except Exception as exc:
        logger.error(f"weekend_fundamentals job raised an unexpected exception: {exc}", exc_info=True)
        _record_heartbeat("weekend_fundamentals", "failed", str(exc))


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
