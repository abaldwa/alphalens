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

_INSERT_PIPELINE_RUN = """
    INSERT INTO pipeline_runs (date, started_at, completed_at, status, stocks_processed, error_message)
    VALUES (?, ?, ?, ?, ?, ?)
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
    Execute STEPS for one date with checkpoint-resume and backfill ML-skip.

    Resumes from CheckpointManager.get_resume_step(run_date) rather than
    always starting at step 0, so already-succeeded steps are never
    re-executed (SPEC-SCHED-002). Stops at the first failure for this date
    — the failed step's checkpoint is what the next run resumes from.

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
        True if every applicable step succeeded (or was skipped as
        non-backfillable); False if a step failed.

    Spec References
    ----------------
    SPEC-SCHED-002: checkpoint-resume on failure.
    SPEC-SCHED-006: no model inference during backfill.

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

    for index, step in enumerate(STEPS):
        if index < resume_index:
            continue  # SPEC-SCHED-002: already succeeded in a previous run

        step_name = step["name"]

        if is_backfill and not step["is_backfillable"]:
            logger.info(
                f"Skipping non-backfillable step '{step_name}' for {run_date} (backfill)"
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
            return False
        else:
            checkpoint_manager.save_checkpoint(run_date, step_name, status="success")

    return True


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


def _execute_daily_job(step_runner: StepRunner, checkpoint_manager: CheckpointManager) -> None:
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

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-001

    Raises
    ------
    None
    """
    run_startup_sequence(step_runner, checkpoint_manager, today=now_ist().date())


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
    SPEC-SCHED-012, SPEC-PIPE-001.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    from datetime import timedelta

    from config.settings import BACKFILL_YEARS
    from config.universe import get_tickers
    from ingestion.backfill_runner import run_backfill
    from ingestion.scrapers.fyers_backfill import FYERSBackfill

    fb = FYERSBackfill()
    cached_token = fb._load_cached_token()
    if not cached_token or not fb._validate_token(cached_token):
        logger.warning(
            "Backfill catch-up skipped: no valid (same-day) FYERS token cached. "
            "Run `python3 -m ingestion.scrapers.fyers_backfill login` / "
            "`... exchange <redirected URL>` first, then this job will pick "
            "up the cached token on its next scheduled run today."
        )
        return

    to_date = now_ist().date()
    from_date = to_date - timedelta(days=365 * BACKFILL_YEARS)
    tickers = get_tickers()
    logger.info(f"Backfill catch-up starting: {len(tickers)} universe tickers, {from_date}..{to_date}")
    run_backfill(tickers, from_date.isoformat(), to_date.isoformat())


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
        args=[step_runner, checkpoint_manager],
        id="daily_pipeline",
        replace_existing=True,
        misfire_grace_time=86400,
        coalesce=True,
    )
    logger.info(f"Daily pipeline scheduled: mode={mode}, time={schedule_time} IST")
