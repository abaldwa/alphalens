"""
ingestion/scheduler/pipeline_scheduler.py — facade / re-export shim.

A46 (2026-08-15): the original 3,375-line monolith was split into five
per-concern modules. This file re-exports their public surface so every
existing `from ingestion.scheduler.pipeline_scheduler import ...` keeps
working unchanged.

    pipeline_run_lock.py  — cross-process advisory lock
    run_recording.py      — pipeline_runs INSERT/UPDATE, heartbeats, job timing
    pipeline_steps.py     — StepRunner, _STEP_DEPS, run_steps_for_date, run_backfill
    pipeline_startup.py   — run_startup_sequence, run_morning_catchup_sequence
    scheduler_jobs.py     — create_jobstore/scheduler, all _execute_*_job /
                            schedule_* functions, model-training maps

NOTE (monkeypatch): the submodules hold their own `from ... import ...`
bindings. `monkeypatch.setattr(<this module>, "now_ist", ...)` patches only
this facade's reference — it does NOT reach the `now_ist` binding inside
`scheduler_jobs`. Tests that must patch a symbol a submodule actually uses
must patch the submodule that owns it too (see tests/unit/test_scheduler.py,
test_pipeline_scheduler_utils.py, test_daily_pipeline.py).

TODO: delete after ML40-2.3 once all consumers import the submodules
directly and the facade is no longer load-bearing.
"""

# ruff: noqa: F401  # re-export shim: every import here is public surface
from ingestion.scheduler.pipeline_run_lock import pipeline_run_lock
from ingestion.scheduler.run_recording import (
    _job_timer_stats,
    _job_timer_start,
    _record_heartbeat,
    _record_pipeline_run,
    _record_pipeline_run_started,
)
from ingestion.scheduler.pipeline_steps import (
    _STEP_DEPS,
    StepRunner,
    run_backfill,
    run_steps_for_date,
)
from ingestion.scheduler.pipeline_startup import (
    run_morning_catchup_sequence,
    run_startup_sequence,
)
from ingestion.scheduler.scheduler_jobs import (
    _MODEL_TRAINING_GROUPS,
    _MODEL_TRAINING_SCRIPT_MAP,
    _VALID_MODES,
    _determine_groww_live_snapshot_month,
    _execute_backfill_catchup,
    _execute_balance_sheet_backfill_job,
    _execute_daily_backup_job,
    _execute_daily_job,
    _execute_emergency_recompute_job,
    _execute_fno_late_catchup_job,
    _execute_forensic_scoring_job,
    _execute_fyers_login_job,
    _execute_job_health_check_job,
    _execute_mf_holdings_job,
    _execute_model_training_job,
    _execute_model_training_job_for_group,
    _execute_morning_catchup_job,
    _execute_multibagger_scoring_job,
    _execute_nse_xbrl_fundamentals_job,
    _execute_promoter_pledge_backfill_job,
    _execute_queued_feature_backfill_job,
    _execute_weekend_feature_backfill_job,
    _execute_weekend_fundamentals_job,
    create_jobstore,
    create_scheduler,
    schedule_backfill_catchup,
    schedule_balance_sheet_backfill,
    schedule_daily_backup,
    schedule_daily_pipeline,
    schedule_emergency_recompute,
    schedule_feature_backfill_once,
    schedule_fno_late_catchup,
    schedule_forensic_scoring,
    schedule_fyers_login,
    schedule_job_health_check,
    schedule_mf_holdings_ingestion,
    schedule_model_training,
    schedule_model_training_nightly,
    schedule_morning_catchup,
    schedule_multibagger_scoring,
    schedule_nse_xbrl_fundamentals,
    schedule_promoter_pledge_backfill,
    schedule_weekend_feature_backfill,
    schedule_weekend_fundamentals,
    _trigger_model_retrain,
    trigger_stacking_ensemble_retrain,
)

__all__ = [
    "pipeline_run_lock",
    "_job_timer_stats",
    "_job_timer_start",
    "_record_heartbeat",
    "_record_pipeline_run",
    "_record_pipeline_run_started",
    "_STEP_DEPS",
    "StepRunner",
    "run_backfill",
    "run_steps_for_date",
    "run_morning_catchup_sequence",
    "run_startup_sequence",
    "_MODEL_TRAINING_GROUPS",
    "_MODEL_TRAINING_SCRIPT_MAP",
    "_VALID_MODES",
    "_determine_groww_live_snapshot_month",
    "_execute_backfill_catchup",
    "_execute_balance_sheet_backfill_job",
    "_execute_daily_backup_job",
    "_execute_daily_job",
    "_execute_emergency_recompute_job",
    "_execute_fno_late_catchup_job",
    "_execute_forensic_scoring_job",
    "_execute_fyers_login_job",
    "_execute_job_health_check_job",
    "_execute_mf_holdings_job",
    "_execute_model_training_job",
    "_execute_model_training_job_for_group",
    "_execute_morning_catchup_job",
    "_execute_multibagger_scoring_job",
    "_execute_nse_xbrl_fundamentals_job",
    "_execute_promoter_pledge_backfill_job",
    "_execute_queued_feature_backfill_job",
    "_execute_weekend_feature_backfill_job",
    "_execute_weekend_fundamentals_job",
    "create_jobstore",
    "create_scheduler",
    "schedule_backfill_catchup",
    "schedule_balance_sheet_backfill",
    "schedule_daily_backup",
    "schedule_daily_pipeline",
    "schedule_emergency_recompute",
    "schedule_feature_backfill_once",
    "schedule_fno_late_catchup",
    "schedule_forensic_scoring",
    "schedule_fyers_login",
    "schedule_job_health_check",
    "schedule_mf_holdings_ingestion",
    "schedule_model_training",
    "schedule_model_training_nightly",
    "schedule_morning_catchup",
    "schedule_multibagger_scoring",
    "schedule_nse_xbrl_fundamentals",
    "schedule_promoter_pledge_backfill",
    "schedule_weekend_feature_backfill",
    "schedule_weekend_fundamentals",
    "_trigger_model_retrain",
    "trigger_stacking_ensemble_retrain",
]
