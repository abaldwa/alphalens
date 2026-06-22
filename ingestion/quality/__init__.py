"""
ingestion.quality package.

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-SYS-003, SPEC-OBS-001 through SPEC-OBS-005,
    SPEC-QUALITY-001, SPEC-QUALITY-002, SPEC-QUALITY-003
Owner: Platform / Ingestion
Consumers: ingestion/scheduler

Data quality validation and drift detection.
- validator.py: bhavcopy completeness gate + anomaly detection (SPEC-SYS-003, SPEC-PIPE-005).
- drift_monitor.py: PSIMonitor — Population Stability Index drift detection (SPEC-PIPE-005).
- baseline_runner.py: operator script computing the PSI baseline from feature history.
- structured_logger.py: per-pipeline-step JSON-line logging (SPEC-OBS-003, SPEC-SEC-001).
"""
