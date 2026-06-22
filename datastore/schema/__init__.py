"""
datastore/schema/__init__.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001 through SPEC-DS-007
Owner: Platform / DataStore
Consumers: datastore/api, ingestion/*, systems/*, backtest

Schema creation scripts for DataStore's normalised (DuckDB) and signals
(SQLite + DuckDB) stores. Run once at deployment time or whenever a fresh
database needs to be bootstrapped; all CREATE TABLE statements are
idempotent (IF NOT EXISTS).
"""
