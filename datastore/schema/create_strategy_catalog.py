"""
datastore/schema/create_strategy_catalog.py

Owner: Platform / Backtest
Consumers: backtest/run_orchestrator_backtest.py (upserts a row after every
run), backtest/run_iterative_backtest.py (upserts a row after every ML
holdout run), operator queries ("show me all Momentum strategies tested
with 15 stocks and 6-month lookback").

One new DuckDB table, strategy_catalog: one row per distinct strategy
CONFIGURATION (not per run) — channel + descriptor + params define the
identity; re-running the same config updates last_run_at/n_runs/
latest_run_id on the existing row instead of inserting a duplicate.

Same idempotent CREATE TABLE IF NOT EXISTS pattern as create_backtest.py.
Lives in the same BACKTEST_DUCKDB_PATH database as backtest_runs, since
latest_run_id is a value-level FK into that table.
"""

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

_CREATE_STRATEGY_CATALOG = """
    CREATE TABLE IF NOT EXISTS strategy_catalog (
        -- Deterministic identity for a strategy CONFIGURATION, not a run:
        -- sha1(channel + descriptor + sorted params_json), stable across
        -- re-runs of the identical config so this table stays one row
        -- per distinct strategy rather than growing per-run.
        strategy_key VARCHAR PRIMARY KEY,
        channel VARCHAR NOT NULL,             -- technical | fundamental | momentum | ml
        descriptor VARCHAR NOT NULL,          -- template name / preset name / "momentum" / "ml"
        params_json VARCHAR NOT NULL,         -- exit_variant, top_n, lookback_months, universe_tier, preset, ...
        latest_run_id VARCHAR NOT NULL,       -- value-level FK -> backtest_runs.run_id
        first_run_at TIMESTAMP NOT NULL,
        last_run_at TIMESTAMP NOT NULL,
        n_runs INTEGER NOT NULL DEFAULT 1
    )
"""

_STRATEGY_CATALOG_TABLES = {"strategy_catalog": _CREATE_STRATEGY_CATALOG}


def create_strategy_catalog_schema(
    db_path: Optional[Path] = None, in_memory: bool = False,
    retry_attempts: Optional[int] = None, retry_base_delay_s: Optional[float] = None,
    retry_max_delay_s: Optional[float] = None,
) -> None:
    """
    Create the strategy_catalog table. Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.BACKTEST_DUCKDB_PATH.
        in_memory: If True, create the schema in an in-memory DuckDB
            (db_path is ignored). Used by tests.
        retry_attempts, retry_base_delay_s: passed through to
            get_duckdb_connection's lock-retry override (2026-07-26 fix —
            this is called at the END of a backtest job, after potentially
            hours of compute, so it needs the same wider write-lock retry
            budget as the job's main write connection; see
            run_orchestrator_backtest.py).
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import BACKTEST_DUCKDB_PATH

        db_path = BACKTEST_DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(
        db_path, retry_attempts=retry_attempts, retry_base_delay_s=retry_base_delay_s,
        retry_max_delay_s=retry_max_delay_s,
    ) as conn:
        for table_name, ddl in _STRATEGY_CATALOG_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")

    logger.info(f"Strategy catalog schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> dict:
    """Return {engine: [table names]} created by this module."""
    return {"duckdb": list(_STRATEGY_CATALOG_TABLES.keys())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_strategy_catalog_schema()
