"""
ingestion/scheduler/pipeline_startup.py

On-startup and morning-catchup sequences: run_startup_sequence and
run_morning_catchup_sequence. Extracted from pipeline_scheduler.py
(A46 — per-concern module split).

Consumers: scheduler_jobs.py (daily/morning job wrappers), tests
           (re-exported via pipeline_scheduler.py for backward compat)
"""

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Optional

from config.timezone import now_ist
from ingestion.scheduler.checkpoint import CheckpointManager
from ingestion.scheduler.gap_detector import detect_gaps, is_trading_day
from ingestion.scheduler.pipeline_steps import StepRunner, run_backfill, run_steps_for_date
from ingestion.scheduler.run_recording import _record_pipeline_run, _record_pipeline_run_started

logger = logging.getLogger(__name__)


def run_startup_sequence(
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    today: Optional[date_type] = None,
    db_path: Optional[Path] = None,
) -> bool:
    """
    On-startup sequence: detect and backfill gaps, then run today's pipeline.

    Used both for the one-off catch-up call at process start and as the
    target of the recurring scheduled job. Returns True if today's own run
    succeeded or was skipped (NSE holiday); False if it failed.
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

    Returns True if there were no gaps, or every gap date backfilled
    successfully.
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