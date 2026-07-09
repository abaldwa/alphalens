"""
datastore/integrity

Phase: A20 (Data Integrity Checker)
Specs: FeatureBacklog.md A20
Owner: Data Layer / Ops / Scheduler

Recurring, standalone audit of already-published production data
(ohlcv_adjusted, fundamentals, corporate_actions, feature Parquet).
Findings are always proposed, never auto-applied — see
datastore/integrity/findings.py's module docstring.
"""
