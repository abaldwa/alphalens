"""
systems.ml_signal_engine package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-MODEL-001, SPEC-MODEL-002, SPEC-MODEL-003, SPEC-PIPE-002, SPEC-PIPE-004,
       SPEC-SYS-002, SPEC-SOLID-001
Owner: Platform / Signals / ML
Consumers: backtest, datastore/api, dashboard, ingestion/scheduler

ML-based signal generation: classification, survival, regime detection.
Models implement IClassificationModel, IExplainableModel, ISurvivalModel interfaces.
Integrated with SHAP for explainability (SPEC-MODEL-002).
"""
