"""
datastore/api/routers/ops.py

Phase: 3.x (Job Autoruns / Ops Page)
Specs: SPEC-SCHED-014
Owner: Platform / DataStore
Consumers: dashboard/static/ops/index.html

Not part of the 27-screen prototype spec (alphalens_docs/screens/
SCREEN_INVENTORY.md) — this is an operational page the user asked for
directly: see every scheduled pipeline step / recurring job with its
last-run status, and force-start a step that hasn't run yet or failed.
All the underlying infrastructure already existed before this file —
ingestion/scheduler/checkpoint.py's STEPS/STEP_NAMES/CheckpointManager,
the pipeline_checkpoints and scheduler_heartbeats SQLite tables (both in
config.settings.PIPELINE_LOG_DB_PATH), and daily_pipeline.py's
step_runner/_STEP_DISPATCH. This router adds no new scheduling logic, only
API scaffolding over what's there, plus one small addition: a
single-step force-run guarded the same way
ingestion/scheduler/pipeline_scheduler.py's run_steps_for_date() already
guards its own step loop (never skip ahead of an unmet prerequisite).
"""

import asyncio
import logging
import re
import subprocess
from datetime import date as date_type
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH, PIPELINE_LOG_DB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection, get_sqlite_connection
from datastore.api.schemas import (
    OpsFailedStepInfo,
    OpsForceStepResponse,
    OpsForceStepResult,
    OpsFreshnessResponse,
    OpsFreshnessRow,
    OpsIntegrityFinding,
    OpsIntegrityFindingActionResponse,
    OpsIntegrityFindingsResponse,
    OpsMissedJobFinding,
    OpsMissedJobFindingActionResponse,
    OpsMissedJobFindingsResponse,
    OpsRunRow,
    OpsRunsResponse,
    OpsExceptionCatalogEntry,
    OpsExceptionCatalogResponse,
    OpsLockStatusEntry,
    OpsLockStatusResponse,
    OpsLiveResourceStatus,
    OpsSchedulerResourceStatus,
    OpsUnusedModelEntry,
    OpsUnusedModelsResponse,
    OpsStepRow,
    OpsStepsResponse,
    SchedulerJobHeartbeat,
)
from datastore.api.utils.scheduler_status import get_scheduler_heartbeats
from config.nse_holidays import ALL_NSE_HOLIDAYS
from ingestion.scheduler.checkpoint import STEPS, CheckpointManager
from ingestion.scheduler.gap_detector import is_trading_day
from ingestion.scheduler.pipeline_scheduler import pipeline_run_lock

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ops", tags=["Ops"])


@router.get("/trading-calendar/holidays")
async def get_trading_calendar_holidays() -> dict[str, list[str]]:
    """NSE trading holidays (ALL_NSE_HOLIDAYS, config/nse_holidays.py) as
    ISO date strings — used by dashboard/static/js/calendar_picker.js to
    flag non-trading dates on date inputs client-side. Reuses the single
    source of truth already used by ingestion/scheduler/gap_detector.py's
    is_trading_day; does not duplicate the holiday list."""
    return {"holidays": sorted(d.isoformat() for d in ALL_NSE_HOLIDAYS)}


@router.get("/heartbeats", response_model=list[SchedulerJobHeartbeat])
async def get_ops_heartbeats() -> list[SchedulerJobHeartbeat]:
    """Every recurring APScheduler job tracked in datastore/api/utils/
    scheduler_status.py's HEARTBEAT_STALE_AFTER — currently daily_pipeline,
    morning_catchup, mf_holdings_ingestion, model_training, and the two
    Saturday-only jobs (weekend_feature_backfill, weekend_fundamentals;
    #5 backlog item). Same data /health already surfaces, factored into
    scheduler_status.py so neither endpoint duplicates the staleness
    thresholds or next-run-time computation."""
    return get_scheduler_heartbeats()  # type: ignore[no-any-return]


# #4: DataStore API Console (freshness rollup) — table -> (db_path, date_col).
# ohlcv_adjusted/fundamentals/macro_indicators live in the normalised store
# (DUCKDB_PATH); ml_signals/ta_signals live in the signals store
# (SIGNALS_DUCKDB_PATH, a separate DuckDB file — see checkpoint.py's/
# signals.py's module docstrings for why these are split). mf_holdings is
# NOT a DuckDB table at all (ingestion/scrapers/amfi_holdings.py writes
# monthly Parquet files to config.settings.MF_HOLDINGS_DIR), so it's
# handled separately below rather than forced into this table-driven loop.
_FRESHNESS_DUCKDB_SOURCES = [
    ("ohlcv_adjusted", "DUCKDB_PATH", "date"),
    ("fundamentals", "DUCKDB_PATH", "announcement_date"),
    ("macro_indicators", "DUCKDB_PATH", "date"),
    ("ml_signals", "SIGNALS_DUCKDB_PATH", "date"),
    ("ta_signals", "SIGNALS_DUCKDB_PATH", "date"),
]


@router.get("/freshness", response_model=OpsFreshnessResponse)
async def get_ops_freshness() -> OpsFreshnessResponse:
    """
    #4: per-data-source freshness rollup — last-write timestamp + row count
    for each of the DataStore's main tables/stores, so a stale source (e.g.
    fundamentals not refreshed in weeks) is visible in one place instead of
    requiring a manual DuckDB query.

    None of these tables carry their own write-timestamp column, so
    last_write_at is the underlying DuckDB file's mtime (a reasonable proxy:
    each file is only ever touched by this pipeline's own writers). For
    mf_holdings (monthly Parquet files, not a DuckDB table — see
    ingestion/scrapers/amfi_holdings.py) this is the most recent file's mtime,
    and row_count/latest_data_date come from reading just that one file
    (not summing every historical month).
    """
    import os
    from datetime import datetime

    import config.settings as _settings

    rows: list[OpsFreshnessRow] = []

    for source, db_path_attr, date_col in _FRESHNESS_DUCKDB_SOURCES:
        db_path = getattr(_settings, db_path_attr)
        try:
            with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
                row_count, latest_date = conn.execute(
                    f"SELECT COUNT(*), MAX({date_col}) FROM {source}"
                ).fetchone()
            last_write_at = (
                datetime.fromtimestamp(os.path.getmtime(db_path)) if db_path.exists() else None
            )
            rows.append(OpsFreshnessRow(
                source=source, row_count=row_count,
                latest_data_date=str(latest_date) if latest_date is not None else None,
                last_write_at=last_write_at,
            ))
        except Exception as exc:
            logger.warning(f"ops.freshness: could not read '{source}' from {db_path}: {exc}")
            rows.append(OpsFreshnessRow(source=source, error=str(exc)))

    # mf_holdings: monthly Parquet files under MF_HOLDINGS_DIR, most recent by filename.
    try:
        mf_dir = _settings.MF_HOLDINGS_DIR
        mf_files = sorted(mf_dir.glob("*.parquet")) if mf_dir.exists() else []
        if not mf_files:
            rows.append(OpsFreshnessRow(source="mf_holdings", row_count=0, error="no Parquet files found"))
        else:
            import pandas as pd

            latest_file = mf_files[-1]
            latest_df = pd.read_parquet(latest_file)
            rows.append(OpsFreshnessRow(
                source="mf_holdings",
                row_count=len(latest_df),
                latest_data_date=latest_file.stem,  # "YYYY-MM"
                last_write_at=datetime.fromtimestamp(os.path.getmtime(latest_file)),
            ))
    except Exception as exc:
        logger.warning(f"ops.freshness: could not read mf_holdings: {exc}")
        rows.append(OpsFreshnessRow(source="mf_holdings", error=str(exc)))

    return OpsFreshnessResponse(sources=rows)


@router.get("/runs", response_model=OpsRunsResponse)
async def get_ops_runs(limit: int = Query(10, ge=1, le=100)) -> OpsRunsResponse:
    """
    Recent pipeline_runs rows (one per full daily-pipeline invocation).

    pipeline_runs itself never records which step failed or why (its own
    error_message column is always written as None — see
    pipeline_scheduler.py::_record_pipeline_run) — only whether the run's
    own date's cascade completed. For any row with status='failed', this
    looks up pipeline_checkpoints for that same date to attach the actual
    failed step(s) and their error messages, so a "failed" row is
    diagnosable without a separate query.
    """
    with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT run_id, date, status, stocks_processed, started_at, completed_at, error_message "
            "FROM pipeline_runs ORDER BY run_id DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()

        runs = []
        for r in rows:
            run_id, date, status, stocks_processed, started_at, completed_at, error_message = r
            failed_steps = []
            if status == "failed" and date:
                cursor.execute(
                    "SELECT step_name, error_message FROM pipeline_checkpoints "
                    "WHERE date = ? AND status = 'failed'",
                    (date,),
                )
                failed_steps = [
                    OpsFailedStepInfo(step_name=fs[0], error_message=fs[1]) for fs in cursor.fetchall()
                ]

            is_backfill = False
            if date:
                cursor.execute(
                    "SELECT 1 FROM pipeline_checkpoints WHERE date = ? AND is_backfill = 1 LIMIT 1",
                    (date,),
                )
                is_backfill = cursor.fetchone() is not None

            # AF-2 (#9): surfaced distinctly from the run's own `status` —
            # a run can be status='success' (every attempted step finished
            # without raising) while sanity_check itself failed and this
            # run never got past it, since sanity_check is hard-depended-on
            # by paper_trade (see checkpoint.py's STEPS).
            sanity_check_passed = None
            if date:
                cursor.execute(
                    "SELECT status FROM pipeline_checkpoints WHERE date = ? AND step_name = 'sanity_check'",
                    (date,),
                )
                sanity_row = cursor.fetchone()
                if sanity_row is not None:
                    sanity_check_passed = sanity_row[0] == "success"

            # Phase 1 (Pipeline & Monitoring Remediation): a 'running' row
            # whose started_at is older than PIPELINE_STALE_RUN_THRESHOLD_MINUTES
            # almost certainly means the process that started it crashed
            # (OOM-killed or otherwise) before ever recording a final
            # status — surface that distinctly rather than letting it read
            # as "still in progress" indefinitely.
            is_stale = False
            if status == "running" and started_at:
                from config.settings import PIPELINE_STALE_RUN_THRESHOLD_MINUTES

                try:
                    started_dt = datetime.fromisoformat(started_at)
                    age_minutes = (now_ist() - started_dt).total_seconds() / 60.0
                    is_stale = age_minutes > PIPELINE_STALE_RUN_THRESHOLD_MINUTES
                except ValueError:
                    pass

            runs.append(OpsRunRow(
                run_id=run_id, date=date, status=status, stocks_processed=stocks_processed,
                started_at=started_at, completed_at=completed_at, error_message=error_message,
                is_backfill=is_backfill, failed_steps=failed_steps, sanity_check_passed=sanity_check_passed,
                is_stale=is_stale,
            ))
    return OpsRunsResponse(runs=runs)


@router.get("/steps", response_model=OpsStepsResponse)
async def get_ops_steps(date: Optional[str] = Query(None, description="YYYY-MM-DD; default today IST")) -> OpsStepsResponse:
    """Every checkpoint.STEPS entry for one date, with its checkpoint status
    ('never_run' if no pipeline_checkpoints row exists yet for that step),
    plus that step's most recent success date (any date, not just this
    row's) and the next time a recurring job could (re)attempt it."""
    from datastore.api.utils.scheduler_status import get_earliest_pipeline_step_next_run

    resolved_date = date or str(now_ist().date())
    next_scheduled_run = get_earliest_pipeline_step_next_run()

    with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT step_name, status, started_at, completed_at, error_message, is_backfill "
            "FROM pipeline_checkpoints WHERE date = ?",
            (resolved_date,),
        )
        by_step = {r[0]: r for r in cursor.fetchall()}

    rows = []
    for index, step in enumerate(STEPS):
        name = step["name"]
        last_success_date = _last_step_success_date(name)
        last_success_str = last_success_date.isoformat() if last_success_date else None
        checkpoint_row = by_step.get(name)
        if checkpoint_row is None:
            rows.append(OpsStepRow(
                step_name=name, step_index=index, is_backfillable=step["is_backfillable"],
                last_success_date=last_success_str, next_scheduled_run=next_scheduled_run,
            ))
        else:
            _, status, started_at, completed_at, error_message, is_backfill = checkpoint_row
            rows.append(OpsStepRow(
                step_name=name, step_index=index, is_backfillable=step["is_backfillable"],
                status=status, started_at=started_at, completed_at=completed_at, error_message=error_message,
                is_backfill=bool(is_backfill),
                last_success_date=last_success_str, next_scheduled_run=next_scheduled_run,
            ))
    return OpsStepsResponse(date=resolved_date, steps=rows)


def _last_step_success_date(step_name: str) -> Optional[date_type]:
    """Most recent date this step has status='success' in pipeline_checkpoints."""
    with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM pipeline_checkpoints WHERE step_name = ? AND status = 'success'",
            (step_name,),
        ).fetchone()
    return date_type.fromisoformat(row[0]) if row and row[0] else None


@router.post("/steps/{step_name}/force", response_model=OpsForceStepResponse)
async def force_run_step(
    step_name: str,
    date: Optional[str] = Query(
        None,
        description=(
            "YYYY-MM-DD to force exactly that one date. Omit to auto-backfill: "
            "every NSE trading day between this step's last success and today "
            "(inclusive) that hasn't succeeded yet. Ignored (always just today) "
            "for the non-backfillable paper_trade step (2026-07-08: run_models/write_signals/"
            "sanity_check are now backfillable — only paper_trade stays today-only)."
        ),
    ),
    cascade: bool = Query(
        True,
        description=(
            "After this step succeeds for a date, keep running the next steps "
            "in STEPS order for that same date (e.g. download_bhavcopy -> "
            "download_fno -> ... -> compute_features) instead of stopping "
            "after just this one. Stops at the first failure, and never "
            "crosses into the non-backfillable paper_trade step for a "
            "date before today."
        ),
    ),
) -> OpsForceStepResponse:
    """
    Run a step — and by default its downstream steps — outside the regular
    scheduler.

    With an explicit `date`, the *starting* date is exactly that one date.
    Without one, this no longer defaults to "today" — a bare force-run used
    to always retry today's date even when NSE hadn't published it yet (e.g.
    mid-session), which is what previously turned a plain 404 into the only
    thing the button could ever do. Instead it walks every NSE trading day
    (config/nse_holidays.py via ingestion/scheduler/gap_detector.is_trading_day)
    since this step's last recorded success up to and including today, and
    force-runs each missing one in order. The one non-backfillable step
    (paper_trade — SPEC-SCHED-006 reserves it for today only, so a gap
    day's signals are never auto-traded retroactively; run_models/
    write_signals/sanity_check are backfillable as of 2026-07-08) never
    gets this treatment: its implicit date is always just today.

    cascade=True (default) means a successful download/feature step doesn't
    leave the rest of that date's pipeline (adjust_prices, compute_features,
    ...) for the user to force-run one button at a time — it continues
    through STEPS in order for that date until a step fails, cascade is
    turned off, or (for a backfilled date before today) the next step isn't
    backfillable, at which point it stops rather than running model
    inference / signal-writing / paper trading on a gap day.

    Guard (same ordering discipline ingestion/scheduler/pipeline_scheduler.py's
    run_steps_for_date() already enforces for its own step loop): every step
    with a lower step_index must have status='success' for a given date, or
    that date is rejected for the *originally requested* step — running it
    out of order (e.g. write_signals before run_models) corrupts data, it
    doesn't just skip work. That check only applies to the first step of the
    cascade; subsequent cascaded steps are always run immediately after their
    prerequisite just succeeded, so they can't have unmet prerequisites.

    Runs each step_runner() call in a thread (asyncio.to_thread) so a slow
    step (e.g. compute_features) doesn't block the event loop for other API
    requests while it runs.
    """
    step_names = [s["name"] for s in STEPS]
    if step_name not in step_names:
        raise HTTPException(status_code=404, detail=f"Unknown step '{step_name}'. Must be one of {step_names}")
    step_index = step_names.index(step_name)
    today = now_ist().date()

    if date:
        run_dates = [date_type.fromisoformat(date)]
    elif not STEPS[step_index]["is_backfillable"]:
        run_dates = [today]
    else:
        last_success = _last_step_success_date(step_name)
        if last_success is None:
            run_dates = [today]
        else:
            # Exclusive of today, same as ingestion/scheduler/gap_detector's
            # detect_gaps(): today is NSE archive data that (usually) isn't
            # published yet during market hours, so a bare (no `date`) force
            # trying it on every click just repeats the same 404 instead of
            # backfilling anything. Today is only attempted here if the
            # caller explicitly passes ?date=<today's date>.
            run_dates = []
            cursor = last_success + timedelta(days=1)
            while cursor < today:
                if is_trading_day(cursor):
                    run_dates.append(cursor)
                cursor += timedelta(days=1)

        if not run_dates:
            return OpsForceStepResponse(
                step_name=step_name,
                date=(last_success or today).isoformat(),
                status="success",
                error_message=None,
                results=[],
            )

    checkpoint_manager = CheckpointManager()
    results: list[OpsForceStepResult] = []

    # 2026-07-05: this endpoint runs in the API's own OS process, entirely
    # separate from the alphalens-scheduler.service process — without this
    # lock a force-run here can race the scheduler's own daily_pipeline/
    # morning_catchup jobs on the same date's pipeline_checkpoints rows
    # (see pipeline_scheduler.pipeline_run_lock's docstring for the
    # 2026-07-02/03 incident this was root-caused from).
    with pipeline_run_lock() as acquired:
        if not acquired:
            raise HTTPException(
                status_code=409,
                detail="Cannot force-run — the scheduler is currently mid-run for some date "
                "(cross-process lock held). Try again once its current run completes.",
            )
        return await _force_run_step_locked(
            step_name, run_dates, step_names, step_index, today, cascade,
            checkpoint_manager, results,
        )


async def _force_run_step_locked(
    step_name: str,
    run_dates: list[date_type],
    step_names: list[str],
    step_index: int,
    today: date_type,
    cascade: bool,
    checkpoint_manager: CheckpointManager,
    results: list[OpsForceStepResult],
) -> OpsForceStepResponse:
    # 2026-07-09 (A21): the actual STEPS-walking/dependency-respecting
    # logic now lives in ingestion/scheduler/force_run.py::force_run_date_sync
    # so A21's "force_run_daily_pipeline" catch-up action can reuse it
    # instead of reimplementing this endpoint's own loop. Run in a thread
    # so a slow step doesn't block the event loop for other API requests.
    from ingestion.scheduler.force_run import force_run_date_sync

    try:
        force_run_results = await asyncio.to_thread(
            force_run_date_sync, step_name, run_dates, today, cascade, checkpoint_manager
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    for r in force_run_results:
        if r.status == "failed":
            logger.error(f"ops.force_run_step: '{r.step_name}' failed for {r.date}: {r.error_message}")
        results.append(
            OpsForceStepResult(step_name=r.step_name, date=r.date, status=r.status, error_message=r.error_message)
        )

    # Backward-compat summary fields describe the originally requested step's
    # own outcome, not whatever it cascaded into.
    own_results = [r for r in results if r.step_name == step_name]
    last = own_results[-1] if own_results else results[-1]
    return OpsForceStepResponse(
        step_name=step_name,
        date=last.date,
        status=last.status,
        error_message=last.error_message,
        results=results,
    )


# 2026-07-05: daily_pipeline.py now runs as the alphalens-scheduler.service
# systemd --user unit (decoupled from VS Code/Claude Code — a Claude session
# ending or running out of tokens no longer stops the scheduler) plus a
# 30-min alphalens-scheduler-monitor.timer that logs mem/load to
# datastore/logs/scheduler_resource_monitor.log and throttles
# HMM_FEATURE_WORKERS/FEATURE_CACHE_PRELOAD_WORKERS under memory pressure —
# see scripts/monitor_scheduler_resources.py's module docstring. This
# endpoint surfaces both so the Ops page can show "is the always-on
# scheduler actually up" separately from "did today's pipeline run", and so
# a throttled/deferred state (which changes step timing, not correctness)
# is visible rather than silent.
_RESOURCE_MONITOR_LOG_PATH = Path("/home/amit/projects/AlphaLens/datastore/logs/scheduler_resource_monitor.log")
_SCHEDULER_SERVICE_NAME = "alphalens-scheduler.service"
_DEFAULT_HMM_WORKERS = 3
_DEFAULT_PRELOAD_WORKERS = 16

_MONITOR_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\S+ \S+) .*mem_available=(?P<mem>[\d.]+)% .*load1=(?P<load1>[\d.]+) .*"
    r"hmm_workers=(?P<hmm>\d+) preload_workers=(?P<preload>\d+)"
)
_MONITOR_DEFERRED_RE = re.compile(r"step '(?P<step>[^']+)' is in progress")


@router.get("/scheduler-resources", response_model=OpsSchedulerResourceStatus)
async def get_ops_scheduler_resources() -> OpsSchedulerResourceStatus:
    """Systemd service status + latest resource-monitor reading (see module docstring above)."""
    service_active: Optional[bool] = None
    service_state: Optional[str] = None
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", _SCHEDULER_SERVICE_NAME, "--property=ActiveState"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        service_state = result.stdout.strip().removeprefix("ActiveState=") or None
        service_active = service_state == "active"
    except Exception as exc:
        logger.warning(f"ops.scheduler_resources: could not query systemctl: {exc}")

    mem_pct = load1 = None
    hmm_workers = preload_workers = None
    last_monitor_run_at = None
    last_deferred_step = None
    error = None

    if _RESOURCE_MONITOR_LOG_PATH.exists():
        try:
            lines = _RESOURCE_MONITOR_LOG_PATH.read_text().splitlines()
            for line in reversed(lines[-200:]):
                m = _MONITOR_LOG_LINE_RE.match(line)
                if m:
                    last_monitor_run_at = m.group("ts")
                    mem_pct = float(m.group("mem"))
                    load1 = float(m.group("load1"))
                    hmm_workers = int(m.group("hmm"))
                    preload_workers = int(m.group("preload"))
                    break
            for line in reversed(lines[-50:]):
                dm = _MONITOR_DEFERRED_RE.search(line)
                if dm:
                    last_deferred_step = dm.group("step")
                    break
        except Exception as exc:
            logger.warning(f"ops.scheduler_resources: could not read monitor log: {exc}")
            error = str(exc)
    else:
        error = "monitor log not found — has the timer fired yet?"

    throttled = (
        hmm_workers is not None and preload_workers is not None
        and (hmm_workers < _DEFAULT_HMM_WORKERS or preload_workers < _DEFAULT_PRELOAD_WORKERS)
    )

    return OpsSchedulerResourceStatus(
        service_active=service_active,
        service_state=service_state,
        mem_available_pct=mem_pct,
        load1=load1,
        hmm_feature_workers=hmm_workers,
        feature_cache_preload_workers=preload_workers,
        throttled=throttled,
        last_monitor_run_at=last_monitor_run_at,
        last_deferred_step=last_deferred_step,
        error=error,
    )


@router.get("/live-resources", response_model=OpsLiveResourceStatus)
async def get_ops_live_resources() -> OpsLiveResourceStatus:
    """A48: near-real-time (uncached, on-demand psutil read) RSS/CPU for
    alphalens-scheduler.service's MainPID — a 10-30s-pollable complement to
    /scheduler-resources' 30-min monitor-log snapshot. Meant to be polled
    by the Ops dashboard only while a pipeline run is active."""
    from datetime import datetime

    from config.settings import PIPELINE_MEMORY_CEILING_MB
    from ingestion.scheduler.resource_guard import poll_process_resources

    pid: Optional[int] = None
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", _SCHEDULER_SERVICE_NAME, "--property=MainPID"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        raw_pid = result.stdout.strip().removeprefix("MainPID=")
        pid = int(raw_pid) if raw_pid.isdigit() and raw_pid != "0" else None
    except Exception as exc:
        logger.warning(f"ops.live_resources: could not query systemctl for MainPID: {exc}")
        return OpsLiveResourceStatus(error=str(exc))

    if pid is None:
        return OpsLiveResourceStatus(error="alphalens-scheduler.service has no live MainPID")

    reading = poll_process_resources(pid)
    if "error" in reading:
        return OpsLiveResourceStatus(pid=pid, error=reading["error"])

    rss_mb = reading["rss_mb"]
    return OpsLiveResourceStatus(
        pid=pid,
        rss_mb=round(rss_mb, 1),
        cpu_percent=round(reading["cpu_percent"], 1),
        memory_ceiling_mb=PIPELINE_MEMORY_CEILING_MB,
        high_pressure=rss_mb >= 0.8 * PIPELINE_MEMORY_CEILING_MB,
        polled_at=datetime.now().isoformat(timespec="seconds"),
    )


@router.get("/lock-status", response_model=OpsLockStatusResponse)
async def get_ops_lock_status() -> OpsLockStatusResponse:
    """Pipeline & Monitoring Remediation Phase 2: live status of both
    cross-process fcntl.flock locks (pipeline_run_lock, publish_run_lock)
    — see ingestion/scheduler/lock_monitor.py's module docstring."""
    from ingestion.scheduler.lock_monitor import all_lock_statuses

    statuses = all_lock_statuses()
    return OpsLockStatusResponse(
        locks=[
            OpsLockStatusEntry(
                name=s.name, path=s.path, exists=s.exists,
                locked=s.locked, last_modified_at=s.last_modified_at,
            )
            for s in statuses
        ]
    )


@router.get("/unused-models", response_model=OpsUnusedModelsResponse)
async def get_ops_unused_models() -> OpsUnusedModelsResponse:
    """Pipeline & Monitoring Remediation Phase 4/5 (A53): models trained
    (real last_trained_date in registry.json) but not read by any known
    consumer — see ingestion/scheduler/model_usage_audit.py."""
    from pathlib import Path

    from config.settings import MODELS_DIR
    from ingestion.scheduler.model_usage_audit import find_trained_but_unused_models

    findings = find_trained_but_unused_models(Path(MODELS_DIR) / "registry.json")
    return OpsUnusedModelsResponse(
        unused=[
            OpsUnusedModelEntry(model_name=f.model_name, last_trained_date=f.last_trained_date)
            for f in findings
        ]
    )


@router.get("/exception-catalog", response_model=OpsExceptionCatalogResponse)
async def get_ops_exception_catalog() -> OpsExceptionCatalogResponse:
    """Pipeline & Monitoring Remediation Phase 0/5: every intentionally-
    swallowed exception in the daily pipeline, with impact + remediation
    — see ingestion/scheduler/exception_catalog.py."""
    from ingestion.scheduler.exception_catalog import all_entries

    return OpsExceptionCatalogResponse(
        entries=[
            OpsExceptionCatalogEntry(
                step_name=e.step_name, location=e.location, caught=e.caught,
                impact=e.impact, remediation=e.remediation, severity=e.severity,
            )
            for e in all_entries()
        ]
    )


@router.get("/integrity-findings", response_model=OpsIntegrityFindingsResponse)
async def get_integrity_findings(
    status: Optional[str] = Query(None, description="Filter by status: pending|approved|rejected|applied"),
    check_name: Optional[str] = Query(None, description="Filter by check: corporate_actions|null_sweep|holiday_leakage|spot_check"),
) -> OpsIntegrityFindingsResponse:
    """A20: list data_integrity_findings rows, most recent first."""
    from datastore.integrity.findings import list_findings

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        df = list_findings(conn, status=status, check_name=check_name)

    findings = [
        OpsIntegrityFinding(
            id=int(row.id),
            check_name=str(row.check_name),
            ticker=str(row.ticker) if row.ticker is not None else None,
            finding_date=str(row.finding_date),
            severity=str(row.severity),
            description=str(row.description),
            evidence_json=str(row.evidence_json) if row.evidence_json is not None else None,
            proposed_fix_sql=str(row.proposed_fix_sql) if row.proposed_fix_sql is not None else None,
            status=str(row.status),
            reviewed_by=str(row.reviewed_by) if row.reviewed_by is not None else None,
            reviewed_at=str(row.reviewed_at) if row.reviewed_at is not None else None,
            created_at=str(row.created_at) if row.created_at is not None else None,
        )
        for row in df.itertuples()
    ]
    return OpsIntegrityFindingsResponse(findings=findings)


@router.post("/integrity-findings/{finding_id}/approve", response_model=OpsIntegrityFindingActionResponse)
async def approve_integrity_finding(
    finding_id: int,
    reviewed_by: str = Query("operator", description="Who approved this finding"),
) -> OpsIntegrityFindingActionResponse:
    """
    A20: approve a pending finding. If it carries a proposed_fix_sql, that
    fix is executed against production data now — this is the ONLY code
    path that writes production data on A20's behalf (never automatic),
    per this project's "flag, don't silently write" discipline.
    """
    from datastore.integrity.findings import approve_finding

    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        try:
            approve_finding(conn, finding_id, reviewed_by)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        row = conn.execute(
            "SELECT status FROM data_integrity_findings WHERE id = ?", [finding_id]
        ).fetchone()
    return OpsIntegrityFindingActionResponse(id=finding_id, status=row[0], reviewed_by=reviewed_by)


@router.post("/integrity-findings/{finding_id}/reject", response_model=OpsIntegrityFindingActionResponse)
async def reject_integrity_finding(
    finding_id: int,
    reviewed_by: str = Query("operator", description="Who rejected this finding"),
) -> OpsIntegrityFindingActionResponse:
    """A20: reject a pending finding. No production data is touched."""
    from datastore.integrity.findings import reject_finding

    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        try:
            reject_finding(conn, finding_id, reviewed_by)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return OpsIntegrityFindingActionResponse(id=finding_id, status="rejected", reviewed_by=reviewed_by)


@router.get("/missed-jobs", response_model=OpsMissedJobFindingsResponse)
async def get_missed_job_findings(
    status: Optional[str] = Query(None, description="Filter by status: pending|approved|rejected|applied"),
    job_id: Optional[str] = Query(None, description="Filter by job_id, e.g. weekend_feature_backfill"),
) -> OpsMissedJobFindingsResponse:
    """A21: list missed_job_findings rows, most recent first."""
    from datastore.health.findings import list_findings

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        df = list_findings(conn, status=status, job_id=job_id)

    findings = [
        OpsMissedJobFinding(
            id=int(row.id),
            job_id=str(row.job_id),
            missed_date=str(row.missed_date),
            severity=str(row.severity),
            description=str(row.description),
            proposed_catchup_action=str(row.proposed_catchup_action) if row.proposed_catchup_action is not None else None,
            proposed_catchup_params_json=str(row.proposed_catchup_params_json) if row.proposed_catchup_params_json is not None else None,
            status=str(row.status),
            reviewed_by=str(row.reviewed_by) if row.reviewed_by is not None else None,
            reviewed_at=str(row.reviewed_at) if row.reviewed_at is not None else None,
            created_at=str(row.created_at) if row.created_at is not None else None,
        )
        for row in df.itertuples()
    ]
    return OpsMissedJobFindingsResponse(findings=findings)


@router.post("/missed-jobs/{finding_id}/approve", response_model=OpsMissedJobFindingActionResponse)
async def approve_missed_job_finding(
    finding_id: int,
    reviewed_by: str = Query("operator", description="Who approved this finding"),
) -> OpsMissedJobFindingActionResponse:
    """
    A21: approve a pending missed-job finding. If it carries a
    proposed_catchup_action, that catch-up (force-run the daily pipeline
    for the missed date(s), re-run a weekend script, or re-run
    mf_holdings ingestion) is triggered now — this is the ONLY code path
    that runs a catch-up job on A21's behalf (never automatic), per this
    project's "flag, don't silently write" discipline.

    Two-phase, deliberately NOT one `with get_duckdb_connection(...)`
    block spanning the whole approval: a catch-up can run for hours (e.g.
    re-running scripts/feature_backfill_hybrid.py), and holding a single
    DuckDB write connection open that whole time would lock the entire
    database for every other reader/writer (the scheduler, the rest of
    this API) until it finished. Instead: read+validate (short-lived
    connection) -> run the catch-up with NO DuckDB connection held ->
    write the final status (short-lived connection).
    """
    from datastore.health.catchup import run_catchup
    from datastore.health.findings import begin_approve, complete_approve

    def _begin() -> tuple[str, str, str | None, dict[str, Any]]:
        with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
            return begin_approve(conn, finding_id)  # type: ignore[no-any-return]

    try:
        job_id, missed_date_str, action, params = await asyncio.to_thread(_begin)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if action:
        missed_date_obj = date_type.fromisoformat(missed_date_str)
        await asyncio.to_thread(run_catchup, action, job_id, missed_date_obj, params)
        new_status = "applied"
    else:
        new_status = "approved"

    def _complete() -> None:
        with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
            complete_approve(conn, finding_id, new_status, reviewed_by)

    await asyncio.to_thread(_complete)
    return OpsMissedJobFindingActionResponse(id=finding_id, status=new_status, reviewed_by=reviewed_by)


@router.post("/missed-jobs/{finding_id}/reject", response_model=OpsMissedJobFindingActionResponse)
async def reject_missed_job_finding(
    finding_id: int,
    reviewed_by: str = Query("operator", description="Who rejected this finding"),
) -> OpsMissedJobFindingActionResponse:
    """A21: reject a pending missed-job finding. No catch-up run is triggered."""
    from datastore.health.findings import reject_finding

    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        try:
            reject_finding(conn, finding_id, reviewed_by)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return OpsMissedJobFindingActionResponse(id=finding_id, status="rejected", reviewed_by=reviewed_by)
