"""
datastore/schema/create_backtest.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
(BacktestUmbrellaPlan.md at the repo root)
Owner: Platform / Backtest
Consumers: backtest/core/feature_log.py, backtest/core/engine.py (once
refactored), Phase 3's backtest_runs API

Store 6 (Backtest, DuckDB) — see config/settings.py's BACKTEST_DUCKDB_PATH
docstring for why this is its own file rather than reusing signals.duckdb.

Two tables, both written by every channel/mode (backtest, walk_forward,
paper) via the shared core/engine.py orchestrator, never per-channel:

- backtest_runs: one row per BacktestRun (backtest/core/run_context.py),
  the run-record schema. Phase 3 will expose this via
  /api/v1/backtest/*; created here first so Phase 1's engine refactor has
  somewhere to write to.
- backtest_feature_log: one row per (run_id, ticker, as_of_date) — the
  full feature vector considered for EVERY candidate signal, not just the
  ones ultimately picked (Standard Backtesting Algorithm step 3a), so the
  feature-reengineering/model-finetuning feedback loop can query "what
  did the model/rule see for stocks it passed on."

Same idempotent CREATE TABLE IF NOT EXISTS + ALTER TABLE ADD COLUMN IF NOT
EXISTS pattern as create_signals.py / create_normalised.py.
"""

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

_CREATE_BACKTEST_RUNS = """
    CREATE TABLE IF NOT EXISTS backtest_runs (
        run_id VARCHAR PRIMARY KEY,
        parent_run_id VARCHAR,
        channel VARCHAR NOT NULL,
        strategy_id VARCHAR NOT NULL,
        horizon_bucket VARCHAR NOT NULL,
        mode VARCHAR NOT NULL,
        universe_spec VARCHAR NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        capital_mode VARCHAR NOT NULL,
        initial_capital DOUBLE NOT NULL,
        sip_amount DOUBLE,
        sip_cadence_days INTEGER,
        random_seed INTEGER NOT NULL,
        config_hash VARCHAR NOT NULL,
        config_json VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL,
        -- Populated once the run completes; NULL while a background Phase 6
        -- agent run is still executing (BacktestUmbrellaPlan.md Phase 6
        -- requirement #7, auditability).
        metrics_json VARCHAR,
        data_gaps_json VARCHAR,
        integrity_passed BOOLEAN,
        integrity_detail_json VARCHAR,
        -- Phase 6 hard boundary (BacktestUmbrellaPlan.md Phase 6 requirement
        -- #6): only a human-approved action may ever set this true. No code
        -- path in the fine-tuning loop is permitted to write TRUE here —
        -- enforced structurally at the API layer (Phase 3), not just by
        -- convention; default FALSE.
        live_eligible BOOLEAN NOT NULL DEFAULT FALSE
    )
"""

_CREATE_BACKTEST_FEATURE_LOG = """
    CREATE TABLE IF NOT EXISTS backtest_feature_log (
        run_id VARCHAR NOT NULL,
        ticker VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        horizon_bucket VARCHAR NOT NULL,
        -- Wide feature vector kept as JSON rather than one column per
        -- feature: the feature set differs per channel/adapter (Technical
        -- indicators vs. Fundamental ratios vs. ML model inputs vs.
        -- Momentum rank factors) and grows over time as adapters evolve —
        -- a fixed-column schema would need a migration every time any
        -- channel added a feature. Queried via DuckDB's native JSON
        -- functions (json_extract), not deserialized in Python for
        -- aggregate queries.
        feature_vector_json VARCHAR NOT NULL,
        signal_output VARCHAR,
        decision_taken VARCHAR NOT NULL,
        PRIMARY KEY (run_id, ticker, as_of_date)
    )
"""

_BACKTEST_TABLES = {
    "backtest_runs": _CREATE_BACKTEST_RUNS,
    "backtest_feature_log": _CREATE_BACKTEST_FEATURE_LOG,
}


def create_backtest_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create Store 6 (Backtest) DuckDB tables: backtest_runs, backtest_feature_log.

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.BACKTEST_DUCKDB_PATH.
        in_memory: If True, create the schema in an in-memory DuckDB
            (db_path is ignored). Used by tests/unit/test_schema_backtest.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import BACKTEST_DUCKDB_PATH

        db_path = BACKTEST_DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(db_path) as conn:
        for table_name, ddl in _BACKTEST_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")

    logger.info(f"Backtest schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> dict:
    """Return {engine: [table names]} created by this module."""
    return {"duckdb": list(_BACKTEST_TABLES.keys())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_backtest_schema()
