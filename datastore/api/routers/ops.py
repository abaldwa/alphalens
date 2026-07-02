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
from datetime import date as date_type
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import PIPELINE_LOG_DB_PATH
from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from datastore.api.schemas import (
    OpsFailedStepInfo,
    OpsForceStepResponse,
    OpsForceStepResult,
    OpsRunRow,
    OpsRunsResponse,
    OpsStepRow,
    OpsStepsResponse,
    SchedulerJobHeartbeat,
)
from datastore.api.utils.scheduler_status import get_scheduler_heartbeats
from config.nse_holidays import ALL_NSE_HOLIDAYS
from ingestion.scheduler.checkpoint import STEPS, CheckpointManager
from ingestion.scheduler.daily_pipeline import step_runner
from ingestion.scheduler.gap_detector import is_trading_day

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ops", tags=["Ops"])


@router.get("/trading-calendar/holidays")
async def get_trading_calendar_holidays() -> dict:
    """NSE trading holidays (ALL_NSE_HOLIDAYS, config/nse_holidays.py) as
    ISO date strings — used by dashboard/static/js/calendar_picker.js to
    flag non-trading dates on date inputs client-side. Reuses the single
    source of truth already used by ingestion/scheduler/gap_detector.py's
    is_trading_day; does not duplicate the holiday list."""
    return {"holidays": sorted(d.isoformat() for d in ALL_NSE_HOLIDAYS)}


@router.get("/heartbeats", response_model=list[SchedulerJobHeartbeat])
async def get_ops_heartbeats() -> list:
    """The 3 recurring APScheduler jobs (daily_pipeline, backfill_catchup,
    mf_holdings_ingestion) — same data /health already surfaces, factored
    into datastore/api/utils/scheduler_status.py so neither endpoint
    duplicates the staleness thresholds."""
    return get_scheduler_heartbeats()


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
            runs.append(OpsRunRow(
                run_id=run_id, date=date, status=status, stocks_processed=stocks_processed,
                started_at=started_at, completed_at=completed_at, error_message=error_message,
                failed_steps=failed_steps,
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
            "SELECT step_name, status, started_at, completed_at, error_message "
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
            _, status, started_at, completed_at, error_message = checkpoint_row
            rows.append(OpsStepRow(
                step_name=name, step_index=index, is_backfillable=step["is_backfillable"],
                status=status, started_at=started_at, completed_at=completed_at, error_message=error_message,
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
            "for non-backfillable steps (run_models/write_signals/paper_trade)."
        ),
    ),
    cascade: bool = Query(
        True,
        description=(
            "After this step succeeds for a date, keep running the next steps "
            "in STEPS order for that same date (e.g. download_bhavcopy -> "
            "download_fno -> ... -> compute_features) instead of stopping "
            "after just this one. Stops at the first failure, and never "
            "crosses into a non-backfillable step (run_models onward) for a "
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
    force-runs each missing one in order. Non-backfillable steps (run_models,
    write_signals, paper_trade — SPEC-SCHED-006 reserves these for today only)
    never get this treatment: their implicit date is always just today.

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

    for run_date in run_dates:
        resolved_date = run_date.isoformat()

        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT step_name FROM pipeline_checkpoints WHERE date = ? AND status = 'success'",
                (resolved_date,),
            )
            succeeded = {r[0] for r in cursor.fetchall()}

        unmet = [name for name in step_names[:step_index] if name not in succeeded]
        if unmet:
            if len(run_dates) > 1:
                logger.warning(
                    f"ops.force_run_step: skipping {resolved_date} for '{step_name}' "
                    f"— prerequisite step(s) not yet successful: {unmet}"
                )
                continue
            raise HTTPException(
                status_code=409,
                detail=f"Cannot force-run '{step_name}' — prerequisite step(s) not yet successful for {resolved_date}: {unmet}",
            )

        current_index = step_index
        while current_index < len(STEPS):
            current_meta = STEPS[current_index]
            current_name = current_meta["name"]

            if run_date < today and not current_meta["is_backfillable"]:
                break  # never run inference/signal/paper-trade steps on a gap day

            checkpoint_manager.save_checkpoint(run_date, current_name, status="running")
            try:
                await asyncio.to_thread(step_runner, run_date, current_name)
            except Exception as exc:
                checkpoint_manager.save_checkpoint(run_date, current_name, status="failed", error_message=str(exc))
                logger.error(f"ops.force_run_step: '{current_name}' failed for {resolved_date}: {exc}")
                results.append(OpsForceStepResult(step_name=current_name, date=resolved_date, status="failed", error_message=str(exc)))
                break  # don't cascade past a failure

            checkpoint_manager.save_checkpoint(run_date, current_name, status="success")
            results.append(OpsForceStepResult(step_name=current_name, date=resolved_date, status="success"))
            succeeded.add(current_name)

            if not cascade:
                break
            current_index += 1

    if not results:
        # every candidate date was skipped for unmet prerequisites
        last_date = run_dates[-1].isoformat()
        raise HTTPException(
            status_code=409,
            detail=f"Cannot force-run '{step_name}' — no candidate date had its prerequisite steps successful "
            f"(checked {run_dates[0].isoformat()} to {last_date})",
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
