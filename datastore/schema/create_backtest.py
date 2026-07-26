"""
datastore/schema/create_backtest.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
(BacktestUmbrellaPlan.md at the repo root)
Owner: Platform / Backtest
Consumers: backtest/core/feature_log.py, backtest/core/engine.py (once
refactored), Phase 3's backtest_runs API

Store 6 (Backtest, DuckDB) — see config/settings.py's BACKTEST_DUCKDB_PATH
docstring for why this is its own file rather than reusing signals.duckdb.

Three tables:

- backtest_runs: one row per BacktestRun (backtest/core/run_context.py),
  the run-record schema. Phase 3 will expose this via
  /api/v1/backtest/*; created here first so Phase 1's engine refactor has
  somewhere to write to. Written by every channel/mode (backtest,
  walk_forward, paper) via the shared core/engine.py orchestrator.
- backtest_feature_log: one row per (run_id, ticker, as_of_date) — the
  full feature vector considered for EVERY candidate signal, not just the
  ones ultimately picked (Standard Backtesting Algorithm step 3a), so the
  feature-reengineering/model-finetuning feedback loop can query "what
  did the model/rule see for stocks it passed on." Written by every
  channel/mode via the shared core/engine.py orchestrator.
- technical_screener_cache: one row per (template_name, as_of_date, ticker)
  — Technical channel only, see its own docstring below.

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
        -- Per-Bull/Bear/Sideways-segment performance breakdown (backtest/
        -- core/regime_breakdown.py), NULL when the run wasn't given a
        -- regime_conn — see create_backtest_schema()'s ALTER TABLE below,
        -- since this column was added after backtest_runs already existed
        -- in real deployments (CREATE TABLE IF NOT EXISTS alone would not
        -- reach an already-created table).
        regime_breakdown_json VARCHAR,
        -- Experiment-comparison convenience columns (added after
        -- backtest_runs already existed in real deployments — see the
        -- ALTER TABLE ADD COLUMN IF NOT EXISTS calls below, same pattern
        -- as regime_breakdown_json above):
        --   exit_policy_variant: which of EXIT_POLICY_VARIANTS
        --     (backtest/core/engine.py) this run used, NULL if the caller
        --     built exit_model directly rather than via the variant factory.
        --   regime_label: single dominant-regime summary derived from
        --     regime_breakdown_json (see engine.py::_finalize), NULL when
        --     no one regime holds a strict majority of the run's days.
        --   trade_log_path: filesystem path to this run's
        --     trade_log_{run_id}.csv (backtest/core/engine.py::
        --     _write_trade_log), NULL only if trade-log writing failed.
        exit_policy_variant VARCHAR,
        regime_label VARCHAR,
        trade_log_path VARCHAR,
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

_CREATE_TECHNICAL_SCREENER_CACHE = """
    CREATE TABLE IF NOT EXISTS technical_screener_cache (
        template_name VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        -- ScreenerResult fields (systems/technical_analysis/screener/engine.py)
        -- verbatim — this table caches ScreenerEngine.screen()'s raw, full-
        -- universe, exit-policy-agnostic output so every exit-variant job for
        -- the same (template, date) reuses one computation instead of each
        -- independently re-reading/re-scoring the daily feature Parquet
        -- (backtest/adapters/technical_adapter.py::TechnicalAdapter,
        -- 2026-07-25 fix — see FeatureBacklog.md). Only score==1.0 (full
        -- matches) are ever cached, matching screen()'s own "Return only
        -- full matches" behavior — never truncated to any one job's top_n,
        -- so it's safe to share across jobs configured with different top_n.
        matched_conditions INTEGER NOT NULL,
        total_conditions INTEGER NOT NULL,
        score DOUBLE NOT NULL,
        -- Per-ticker technical indicator snapshot at match time (sma_200_ratio,
        -- rsi_14, etc.) — TechnicalAdapter.feature_vector() reads this back
        -- for backtest_feature_log; dropping it would silently degrade
        -- downstream ML feature-vector consumers with no error.
        key_values_json VARCHAR NOT NULL,
        PRIMARY KEY (template_name, as_of_date, ticker)
    )
"""

_BACKTEST_TABLES = {
    "backtest_runs": _CREATE_BACKTEST_RUNS,
    "backtest_feature_log": _CREATE_BACKTEST_FEATURE_LOG,
    "technical_screener_cache": _CREATE_TECHNICAL_SCREENER_CACHE,
}


def create_backtest_schema(
    db_path: Optional[Path] = None, in_memory: bool = False,
    retry_attempts: Optional[int] = None, retry_base_delay_s: Optional[float] = None,
    retry_max_delay_s: Optional[float] = None,
) -> None:
    """
    Create Store 6 (Backtest) DuckDB tables: backtest_runs, backtest_feature_log.

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.BACKTEST_DUCKDB_PATH.
        in_memory: If True, create the schema in an in-memory DuckDB
            (db_path is ignored). Used by tests/unit/test_schema_backtest.py.
        retry_attempts, retry_base_delay_s: passed through to
            get_duckdb_connection's lock-retry override (2026-07-26 fix —
            a caller opening this on BACKTEST_DUCKDB_PATH alongside a
            long-running backtest job should use the same wider budget as
            the job's own write connection; see run_orchestrator_backtest.py).
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
        for table_name, ddl in _BACKTEST_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS regime_breakdown_json VARCHAR")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS exit_policy_variant VARCHAR")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS regime_label VARCHAR")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS trade_log_path VARCHAR")
        # 2026-07-26 (REV6 wiring, model-review-corrected design): queue_id
        # identifies which sweep (backtest/run_strategy_queue.py's
        # --report-suffix) a run belongs to — the multiple-comparisons
        # "universe" deflated_sharpe_ratio's n_trials must count against.
        # dsr/dsr_n_trials are written EVENT-DRIVEN, one row at a time, as
        # each job in a queue completes (run_strategy_queue.py), using
        # n_trials = count of jobs completed so far in that queue at THAT
        # moment — matching backtest/iterative_retrain.py::RetrainLoop's
        # sequential n_trials_so_far convention, not a single batch pass
        # after the whole queue finishes (reviewers confirmed a full-queue
        # batch pass cannot function as a gate, since nothing can be
        # rejected once every row is already published). dsr_computed_post_hoc
        # is TRUE only for the one-time backfill of runs that completed
        # BEFORE this wiring existed (see backtest/backfill_dsr.py) — every
        # row computed by the live wiring going forward has it FALSE.
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS queue_id VARCHAR")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS dsr DOUBLE")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS dsr_n_trials INTEGER")
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS dsr_computed_post_hoc BOOLEAN")

    logger.info(f"Backtest schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> dict:
    """Return {engine: [table names]} created by this module."""
    return {"duckdb": list(_BACKTEST_TABLES.keys())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_backtest_schema()
