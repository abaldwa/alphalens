"""
datastore/schema/create_strategy_registry.py

Owner: Platform / Architecture
Consumers: strategies/registry.py (read/write API), backtest adapters (read
definitions instead of importing TEMPLATES / build_category_presets /
STRATEGY_CATALOG), datastore/api/routers/strategies.py, the technical
screener and alert checker, daily inference.

Four tables that together make strategy definitions, filters and generated
signals declarative and auditable instead of scattered across Python
modules in four incompatible shapes:

    strategy_registry          A92 - one row per (strategy, version)
    filter_registry            A93 - one row per (filter, version)
    strategy_signals           A94 - the ledger: (strategy, date, ticker, action)
    signal_generation_blocked  the refusals: when a generator would not run

See AGENTS.md "Architectural invariants" for the rules these enforce, and
FeatureBacklog.md's "A92-A95 - Registry-driven architecture (spec)" section
for the full rationale.

Two design points that are load-bearing rather than stylistic:

* strategy_registry and filter_registry are APPEND-ONLY and point-in-time
  versioned. A row is never mutated; a change writes a new version and
  closes the previous one's valid_to. Runs record the version they
  executed, so editing a definition cannot silently invalidate historical
  results. This is the same discipline already applied to features.

* strategy_signals stores only EMITTED signals, never every ticker
  evaluated. 63 templates x ~800 tickers x ~4,300 sessions is order 1e8
  rows if written naively; the emitted-only rule keeps it in the millions.

Lives in BACKTEST_DUCKDB_PATH alongside backtest_runs and strategy_catalog,
because strategy_signals.run_id is a value-level FK into backtest_runs and
the registries are read by the same jobs.

Same idempotent CREATE TABLE IF NOT EXISTS pattern as create_backtest.py.
"""

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# A92 - strategy_registry
# ---------------------------------------------------------------------------
# strategy_key is the canonical cross-application identity: "{channel}:{name}",
# e.g. "technical:A1_pullback_in_uptrend" or "momentum:b1_lb6mo_monthly_top15".
# It is the same string the frontend uses as a URL parameter and the same one
# the engines must emit (A89), which is why it is a readable composite rather
# than the sha1 strategy_catalog uses -- a hash cannot be put in a URL and read
# back by a human debugging a report.
_CREATE_STRATEGY_REGISTRY = """
    CREATE TABLE IF NOT EXISTS strategy_registry (
        strategy_key VARCHAR NOT NULL,          -- "{channel}:{name}"
        version INTEGER NOT NULL,               -- 1-based; append-only
        channel VARCHAR NOT NULL,               -- momentum|technical|fundamental|ml
        name VARCHAR NOT NULL,                  -- unique within channel
        display_label VARCHAR NOT NULL,         -- ONE label for every screen
        description VARCHAR,
        category VARCHAR,                       -- channel-specific grouping

        -- Channel-specific setup block (the StrategySetup union the frontend
        -- renders): lookback/rebalance/top_n for momentum, template+horizon
        -- for technical, preset/score_function for fundamental, model+
        -- threshold for ml. JSON because the four genuinely differ; the
        -- COMMON fields (universe, capital, benchmark) are named columns
        -- below so they stay queryable.
        definition_json VARCHAR NOT NULL,

        -- Ordered structured predicates, one grammar for every channel:
        --   [{"feature": <col>, "op": "lt|gt|lte|gte|eq|between|
        --                              top_pct|bottom_pct|in|not_in",
        --     "value": <scalar|[lo,hi]|[...]>}, ...]
        entry_criterion_json VARCHAR NOT NULL,

        -- {"variant": <EXIT_POLICY_VARIANTS member>, "stop_pct": ...,
        --  "target_pct": ..., "max_hold_days": ..., "trailing_pct": ...,
        --  "conditions": [<predicate>, ...]}
        exit_criterion_json VARCHAR NOT NULL,

        filter_ids VARCHAR[] NOT NULL,          -- FKs -> filter_registry.filter_id

        -- Common setup fields, promoted out of definition_json so they can be
        -- filtered on without JSON extraction.
        universe_spec VARCHAR,                  -- e.g. "pit_adtv_top_800"
        benchmark_index_name VARCHAR,           -- A98: distinct from the regime index
        regime_index_name VARCHAR,

        status VARCHAR NOT NULL DEFAULT 'draft',  -- draft|active|retired
        valid_from DATE NOT NULL,
        valid_to DATE,                          -- NULL = currently in force
        source_ref VARCHAR,                     -- module/commit this was migrated from
        created_at TIMESTAMP NOT NULL,
        created_by VARCHAR,
        PRIMARY KEY (strategy_key, version)
    )
"""

# ---------------------------------------------------------------------------
# A93 - filter_registry
# ---------------------------------------------------------------------------
# One row per filter CONCEPT. The seven concepts currently declared three times
# each (momentum kwargs / technical job fields / fundamental adapter constants)
# collapse to seven rows, each pointing at exactly one implementation.
_CREATE_FILTER_REGISTRY = """
    CREATE TABLE IF NOT EXISTS filter_registry (
        filter_id VARCHAR NOT NULL,             -- adtv_floor, downtrend, circuit_lock, ...
        version INTEGER NOT NULL,
        name VARCHAR NOT NULL,
        description VARCHAR,
        filter_type VARCHAR NOT NULL,           -- universe|entry|exit|sizing
        params_schema_json VARCHAR NOT NULL,    -- {param: {type, default, min, max, required}}
        default_params_json VARCHAR NOT NULL,
        applies_to_channels VARCHAR[] NOT NULL,
        implementation_ref VARCHAR NOT NULL,    -- dotted path to the ONE implementation
        status VARCHAR NOT NULL DEFAULT 'active',
        valid_from DATE NOT NULL,
        valid_to DATE,
        source_ref VARCHAR,
        created_at TIMESTAMP NOT NULL,
        created_by VARCHAR,
        PRIMARY KEY (filter_id, version)
    )
"""

# ---------------------------------------------------------------------------
# A94 - strategy_signals
# ---------------------------------------------------------------------------
# run_id is part of the PK and cannot be NULL in DuckDB PKs, so live/paper rows
# use the sentinel '' rather than NULL. Storing NULL would silently drop every
# live signal from the index and let duplicates through.
_CREATE_STRATEGY_SIGNALS = """
    CREATE TABLE IF NOT EXISTS strategy_signals (
        strategy_key VARCHAR NOT NULL,
        strategy_version INTEGER NOT NULL,
        signal_date DATE NOT NULL,              -- as-of date the signal is FOR
        ticker VARCHAR NOT NULL,
        action VARCHAR NOT NULL,                -- buy|sell|hold|forced_close
        conviction DOUBLE,
        rank INTEGER,
        size_multiplier DOUBLE,

        -- sector, adtv_cr, which entry predicates fired, which filters passed
        -- or rejected. This is what lets a report explain WHY a strategy acted,
        -- and what makes a live trade traceable to its signal.
        context_json VARCHAR,

        source VARCHAR NOT NULL,                -- backtest|paper|live
        run_id VARCHAR NOT NULL DEFAULT '',     -- '' for live/paper; FK -> backtest_runs
        generated_at TIMESTAMP NOT NULL,
        PRIMARY KEY (strategy_key, strategy_version, signal_date, ticker, source, run_id)
    )
"""

# Signal reads are overwhelmingly "this strategy over this window" (report
# rendering, A87 signal reuse) and "what fired on this date" (live alerts).
_CREATE_SIGNAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_strategy_signals_date ON strategy_signals (signal_date)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_signals_key_date "
    "ON strategy_signals (strategy_key, signal_date)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_signals_run ON strategy_signals (run_id)",
]

# ---------------------------------------------------------------------------
# signal_generation_blocked
# ---------------------------------------------------------------------------
# The counterpart to strategy_signals: strategy_signals records what a
# generator DID, this records when it REFUSED to. Without it, "no signals for
# this strategy today" is ambiguous between "the strategy found nothing" and
# "the strategy was never allowed to look because six of its indicators were
# null" -- and those demand opposite responses from an operator.
#
# missing_json is the serialised list of backtest/core/readiness.py's
# MissingInput ([{kind, detail, expected}, ...]), so the row says WHICH input
# was absent, not merely that something was. A row that only said "blocked"
# would send whoever reads it back to re-derive the cause by hand, on an
# evening when the data has since been backfilled and the cause is gone.
#
# The PK is (strategy, version, as_of_date) with no checked_at: the scheduler
# retries the same day several times while a backfill catches up, and the
# LATEST attempt's missing list is the one worth keeping. Including checked_at
# in the key would accumulate one row per retry and bury it.
_CREATE_SIGNAL_GENERATION_BLOCKED = """
    CREATE TABLE IF NOT EXISTS signal_generation_blocked (
        strategy_key VARCHAR NOT NULL,
        strategy_version INTEGER NOT NULL,
        channel VARCHAR NOT NULL,               -- momentum|technical|fundamental|ml
        as_of_date DATE NOT NULL,               -- the date signals were WANTED for
        missing_json VARCHAR NOT NULL,          -- [{kind, detail, expected}, ...]
        checked_at TIMESTAMP NOT NULL,
        PRIMARY KEY (strategy_key, strategy_version, as_of_date)
    )
"""

# "what was blocked today, across every strategy" is the operator's question
# every evening; "was this strategy ever blocked" is the debugging one.
_CREATE_BLOCKED_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_signal_blocked_date "
    "ON signal_generation_blocked (as_of_date)",
    "CREATE INDEX IF NOT EXISTS idx_signal_blocked_key "
    "ON signal_generation_blocked (strategy_key, as_of_date)",
]

_REGISTRY_TABLES = {
    "strategy_registry": _CREATE_STRATEGY_REGISTRY,
    "filter_registry": _CREATE_FILTER_REGISTRY,
    "strategy_signals": _CREATE_STRATEGY_SIGNALS,
    "signal_generation_blocked": _CREATE_SIGNAL_GENERATION_BLOCKED,
}


def create_strategy_registry_schema(
    db_path: Optional[Path] = None,
    in_memory: bool = False,
    retry_attempts: Optional[int] = None,
    retry_base_delay_s: Optional[float] = None,
    retry_max_delay_s: Optional[float] = None,
) -> None:
    """
    Create the three registry tables. Idempotent - safe to call repeatedly.

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.BACKTEST_DUCKDB_PATH.
        in_memory: If True, create in an in-memory DuckDB (db_path ignored).
            Used by tests.
        retry_attempts, retry_base_delay_s, retry_max_delay_s: passed through
            to get_duckdb_connection's lock-retry override, same rationale as
            create_strategy_catalog_schema - this can be called at the end of
            a long job that has been holding the write lock.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import BACKTEST_DUCKDB_PATH

        db_path = BACKTEST_DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(
        db_path,
        retry_attempts=retry_attempts,
        retry_base_delay_s=retry_base_delay_s,
        retry_max_delay_s=retry_max_delay_s,
    ) as conn:
        for table_name, ddl in _REGISTRY_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        for ddl in _CREATE_SIGNAL_INDEXES + _CREATE_BLOCKED_INDEXES:
            conn.execute(ddl)

    logger.info(
        f"Strategy registry schema ready at {db_path if db_path else ':memory:'}"
    )


def list_tables() -> dict:
    """Return {engine: [table names]} created by this module."""
    return {"duckdb": list(_REGISTRY_TABLES.keys())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_strategy_registry_schema()
