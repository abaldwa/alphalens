"""
datastore/api/utils/scheduler_status.py

Phase: 3.x (Job Autoruns / Ops API)
Specs: SPEC-SCHED-013, SPEC-SCHED-014
Owner: Platform / DataStore
Consumers: datastore/api/routers/system.py, datastore/api/routers/ops.py

Recurring-job heartbeat logic, factored out of system.py's `/health`
endpoint (SPEC-SCHED-013) so the new Ops page's `/api/v1/ops/heartbeats`
doesn't duplicate the `_HEARTBEAT_STALE_AFTER` staleness thresholds and
`scheduler_heartbeats` query — both endpoints now call this one function.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from config.settings import PIPELINE_LOG_DB_PATH
from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from datastore.api.schemas import SchedulerJobHeartbeat

logger = logging.getLogger(__name__)

# SPEC-SCHED-013: per-job "how long without an attempt before we call it
# stale" — generous enough to absorb a normal weekend for the Mon-Fri
# daily pipeline (last attempt Friday, checked Monday morning) without a
# false positive, while still catching a genuinely silent multi-day stall.
# backfill_catchup deliberately excluded (2026-07): it's no longer
# registered by daily_pipeline.py's main() (FYERS-only, NSE covers
# everything this pipeline needs), so it would show a permanently false
# STALE badge here forever if kept.
HEARTBEAT_STALE_AFTER = {
    "daily_pipeline": timedelta(days=4),
    # 2026-07: earlier second trigger of the same catch-up logic (see
    # schedule_morning_catchup) — same mon-fri cadence as daily_pipeline.
    "morning_catchup": timedelta(days=4),
    # [AS BUILT, P2.2] monthly job (5th of each month) — 33 days absorbs a
    # run landing a few days late without a false-positive staleness flag
    # right before the next month's scheduled fire.
    "mf_holdings_ingestion": timedelta(days=33),
    # 2026-07-02: new overnight training check, mon-fri at 20:00 IST.
    # 4 days absorbs the weekend gap (last fired Fri, checked Mon morning).
    "model_training": timedelta(days=4),
    # Saturday-only jobs — 8 days absorbs a single missed Saturday without
    # a permanent false-positive stale flag.
    "weekend_feature_backfill": timedelta(days=8),
    "weekend_fundamentals": timedelta(days=8),
}


def get_next_run_times() -> Dict[str, Optional[datetime]]:
    """
    Next scheduled fire time for each known recurring job, computed purely
    from the same cron parameters ingestion/scheduler/pipeline_scheduler.py
    registers them with — NOT read from the persisted APScheduler job store.

    Why not just read the job store directly (e.g. SQLAlchemyJobStore.
    get_all_jobs()): each job's pickled state references its callable by
    module path, and daily_pipeline.py is normally launched via
    `python -m ingestion.scheduler.daily_pipeline`, which makes functions
    defined directly in that file pickle under `__main__` rather than their
    real module path. A *different* process (like this API server) trying
    to unpickle that job fails with an AttributeError -- and
    SQLAlchemyJobStore's failure handling silently DELETES the
    unreconstructable job from the persisted store as a side effect. This
    isn't hypothetical: reading the live store this way from a throwaway
    diagnostic script deleted the real "daily_pipeline" job during
    development of this feature (2026-07-02), requiring a scheduler
    restart to recover. Recomputing next-fire-time analytically from the
    known schedule avoids ever touching that store from this process.
    """
    from apscheduler.triggers.cron import CronTrigger

    from config.settings import (
        AMFI_SCHEDULE_TIME,
        DAILY_PIPELINE_SCHEDULE_TIME,
        MF_HOLDINGS_SCHEDULE_DAYS,
        MODEL_TRAINING_SCHEDULE_TIME,
        MORNING_CATCHUP_SCHEDULE_TIME,
        WEEKEND_FEATURE_BACKFILL_TIME,
        WEEKEND_FUNDAMENTALS_TIME,
    )

    now = now_ist()
    daily_hour, daily_minute = (int(p) for p in DAILY_PIPELINE_SCHEDULE_TIME.split(":"))
    morning_hour, morning_minute = (int(p) for p in MORNING_CATCHUP_SCHEDULE_TIME.split(":"))
    mf_hour, mf_minute = (int(p) for p in AMFI_SCHEDULE_TIME.split(":"))
    train_hour, train_minute = (int(p) for p in MODEL_TRAINING_SCHEDULE_TIME.split(":"))
    wfb_hour, wfb_minute = (int(p) for p in WEEKEND_FEATURE_BACKFILL_TIME.split(":"))
    wfu_hour, wfu_minute = (int(p) for p in WEEKEND_FUNDAMENTALS_TIME.split(":"))

    triggers = {
        "daily_pipeline": CronTrigger(
            hour=daily_hour, minute=daily_minute, day_of_week="mon-fri", timezone="Asia/Kolkata"
        ),
        "morning_catchup": CronTrigger(
            hour=morning_hour, minute=morning_minute, day_of_week="mon-fri", timezone="Asia/Kolkata"
        ),
        "mf_holdings_ingestion": CronTrigger(
            day=MF_HOLDINGS_SCHEDULE_DAYS, hour=mf_hour, minute=mf_minute, timezone="Asia/Kolkata"
        ),
        "model_training": CronTrigger(
            hour=train_hour, minute=train_minute, day_of_week="mon-fri", timezone="Asia/Kolkata"
        ),
        "weekend_feature_backfill": CronTrigger(
            hour=wfb_hour, minute=wfb_minute, day_of_week="sat", timezone="Asia/Kolkata"
        ),
        "weekend_fundamentals": CronTrigger(
            hour=wfu_hour, minute=wfu_minute, day_of_week="sat", timezone="Asia/Kolkata"
        ),
    }
    return {job_id: trigger.get_next_fire_time(None, now) for job_id, trigger in triggers.items()}


def get_earliest_pipeline_step_next_run() -> Optional[datetime]:
    """
    Earliest next-fire-time across every recurring job capable of running
    STEPS (daily_pipeline and morning_catchup both can — mf_holdings_
    ingestion runs an unrelated job, not the STEPS cascade). Used by the
    Ops page's per-step "Next Scheduled Run" column: a step could be
    (re)attempted by whichever of the two fires first.
    """
    next_runs = get_next_run_times()
    candidates = [
        next_runs.get(job_id) for job_id in ("daily_pipeline", "morning_catchup")
        if next_runs.get(job_id) is not None
    ]
    return min(candidates) if candidates else None


def get_scheduler_heartbeats() -> List[SchedulerJobHeartbeat]:
    """One row per known recurring job, from scheduler_heartbeats, with staleness and next-run-time computed here."""
    now = now_ist()
    rows_by_job = {}
    try:
        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT job_id, last_attempt_at, last_status, last_error, last_success_at FROM scheduler_heartbeats"
            )
            for job_id, last_attempt_at, last_status, last_error, last_success_at in cursor.fetchall():
                rows_by_job[job_id] = (last_attempt_at, last_status, last_error, last_success_at)
    except Exception as exc:
        logger.warning(f"scheduler_status: could not fetch scheduler heartbeats: {exc}")

    try:
        next_run_times = get_next_run_times()
    except Exception as exc:
        logger.warning(f"scheduler_status: could not compute next run times: {exc}")
        next_run_times = {}

    results: List[SchedulerJobHeartbeat] = []
    for job_id, stale_after in HEARTBEAT_STALE_AFTER.items():
        row = rows_by_job.get(job_id)
        next_run_time = next_run_times.get(job_id)
        if row is None:
            results.append(SchedulerJobHeartbeat(job_id=job_id, is_stale=True, next_run_time=next_run_time))
            continue
        last_attempt_at, last_status, last_error, last_success_at = row
        is_stale = True
        if last_attempt_at:
            is_stale = (now - datetime.fromisoformat(last_attempt_at)) > stale_after
        results.append(
            SchedulerJobHeartbeat(
                job_id=job_id, last_attempt_at=last_attempt_at, last_status=last_status,
                last_error=last_error, last_success_at=last_success_at, is_stale=is_stale,
                next_run_time=next_run_time,
            )
        )
    return results
