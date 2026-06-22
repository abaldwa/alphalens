"""
systems package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-MODEL-001, SPEC-MODEL-002, SPEC-MODEL-003, SPEC-PIPE-002,
       SPEC-PIPE-004, SPEC-DS-005, SPEC-SOLID-001, SPEC-SOLID-002
Owner: Platform / Signals
Consumers: backtest, datastore/api, ingestion/scheduler

Signal generation systems: ML models, regime detection, fundamental analysis.
Subpackages: ml_signal_engine (classification/survival), fundamental_analysis,
technical_analysis, damodaran_valuation.
SOLID: Each system implements IModel interface for pluggability.
"""
