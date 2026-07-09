"""
datastore/health

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler

Weekly job-completeness audit: confirms every recurring scheduled job
(daily_pipeline, and the weekly/weekend jobs — weekend_feature_backfill,
weekend_fundamentals, mf_holdings_ingestion, daily_backup,
multibagger_scoring, forensic_scoring, nse_xbrl_fundamentals) actually
recorded a success in the trailing lookback window, using
job_run_log (ingestion/scheduler/pipeline_scheduler.py::_record_heartbeat)
as its source of per-invocation history. Findings are always proposed,
never auto-applied — see datastore/health/findings.py's module docstring.
"""
