"""
datastore/health/job_registry.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler
Consumers: datastore/health/checks.py

Static cadence table for every recurring scheduled job this project runs
(ingestion/scheduler/pipeline_scheduler.py's schedule_* functions), plus
expected_dates() to compute which calendar dates a job should have fired
on within a window.

`model_training` is deliberately NOT registered here — it's demand-driven
(ingestion/scheduler/pipeline_scheduler.py::_execute_model_training_job
skips cleanly when no model is currently overdue for retrain), so a
'skipped' heartbeat is a normal, compliant outcome for it, not a missed
job. Flagging it on a calendar cadence would just be noise.
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import timedelta
from typing import Dict, List

# Weekday numbers per datetime.date.weekday(): 0=Mon ... 6=Sun.
_MON_FRI = {0, 1, 2, 3, 4}
_DAILY = {0, 1, 2, 3, 4, 5, 6}
_SAT = {5}
_SUN = {6}

# job_id -> {"weekdays": set of expected weekday() values, "catchup_action": str, "catchup_params": dict}
JOB_REGISTRY: Dict[str, Dict] = {
    "daily_pipeline": {
        "weekdays": _MON_FRI,
        "catchup_action": "force_run_daily_pipeline",
        "catchup_params": {},
    },
    "weekend_feature_backfill": {
        "weekdays": _SAT,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "scripts/feature_backfill_hybrid.py", "args": ["--stage2-chunk-size", "400"]},
    },
    "weekend_fundamentals": {
        "weekdays": _SAT,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "scripts/backfill_fundamentals_trendlyne.py", "args": []},
    },
    "nse_xbrl_fundamentals": {
        "weekdays": _SAT,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "scripts/backfill_fundamentals_nse_xbrl.py", "args": []},
    },
    "mf_holdings_ingestion": {
        "weekdays": _SAT,
        "catchup_action": "rerun_mf_holdings",
        "catchup_params": {},
    },
    "multibagger_scoring": {
        "weekdays": _SUN,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "systems/ml_signal_engine/inference/score_multibagger.py", "args": []},
    },
    "forensic_scoring": {
        "weekdays": _SUN,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "features/deep_forensic.py", "args": []},
    },
    "daily_backup": {
        "weekdays": _DAILY,
        "catchup_action": "rerun_script",
        "catchup_params": {"script": "scripts/backup_to_b2.py", "args": []},
    },
}


def expected_dates(job_id: str, window_start: date_type, window_end: date_type) -> List[date_type]:
    """
    Every calendar date in [window_start, window_end] (inclusive) on which
    `job_id` was expected to fire, per its registered cadence.

    Raises
    ------
    KeyError
        If job_id is not in JOB_REGISTRY (e.g. 'model_training', which is
        deliberately excluded — see module docstring).
    """
    weekdays = JOB_REGISTRY[job_id]["weekdays"]
    dates = []
    cursor = window_start
    while cursor <= window_end:
        if cursor.weekday() in weekdays:
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates
