"""
datastore/schema/create_strategy_deployments.py

A91: channel-agnostic strategy deployments.

`momentum_strategy_configs` is momentum-shaped down to its column names
(band_id, lookback_months, grace_period), so a Technical, Fundamental or ML
strategy cannot be deployed at all -- the Deploy control in the backtest
report has to render disabled for three channels out of four. That is not a UI
limitation; there is nowhere for the row to go.

This table deploys a strategy by its REGISTRY KEY plus the parameters that are
genuinely deployment decisions rather than strategy attributes. Everything
that defines the strategy itself -- entry rules, exit rules, filters -- stays
in strategy_registry and is referenced, never copied. That is what makes
AGENTS.md invariant 6 true: a strategy's backtested definition and its
deployed definition are the same row.

`strategy_version` is recorded and NOT NULL on purpose. Deploying "the current
version" is how a live position ends up running rules that were revised after
it was opened; pinning the version means a definition change produces a
visible upgrade decision rather than a silent behavioural drift.

Same idempotent CREATE TABLE IF NOT EXISTS pattern as create_backtest.py.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

_SEQUENCE = """
CREATE SEQUENCE IF NOT EXISTS strategy_deployments_id_seq START 1
"""

_TABLES = {
    "strategy_deployments": """
    CREATE TABLE IF NOT EXISTS strategy_deployments (
        deployment_id BIGINT PRIMARY KEY
            DEFAULT nextval('strategy_deployments_id_seq'),

        -- Identity: which registry row is being deployed, at which version.
        strategy_key VARCHAR NOT NULL,
        strategy_version INTEGER NOT NULL,
        channel VARCHAR NOT NULL,

        -- Deployment decisions. None of these are strategy attributes: a
        -- backtest cannot know how much capital you are putting in or when
        -- you start, which is why the report hands them over blank and the
        -- form requires them.
        initial_capital DOUBLE NOT NULL DEFAULT 0,
        sip_amount DOUBLE NOT NULL DEFAULT 0,
        start_date DATE NOT NULL,
        -- 0 means "not assigned to a portfolio". A sentinel rather than NULL
        -- because NULLs compare as distinct in a UNIQUE constraint, so a
        -- nullable column here would silently permit the duplicate
        -- deployments the application check below is meant to prevent.
        portfolio_id INTEGER NOT NULL DEFAULT 0,

        -- Execution schedule. rebalance_frequency is shared across channels;
        -- rebalance_day_of_month applies only where the frequency is monthly.
        rebalance_frequency VARCHAR,
        rebalance_day_of_month INTEGER,

        -- Per-deployment overrides of the strategy's declared filter params,
        -- as {filter_id: {param: value}}. Overrides only -- a deployment that
        -- needs DIFFERENT RULES needs a new registry version, not a config
        -- field, or the deployed strategy stops being the backtested one.
        filter_overrides_json JSON NOT NULL DEFAULT '{}',

        -- Which index this deployment is judged against (A98). Separate from
        -- the regime index, and recorded here so a live report cannot be
        -- silently re-benchmarked after the fact.
        benchmark_index_name VARCHAR,

        -- Capital treatment: lump | sip | annual_reset (A88's income mode).
        capital_mode VARCHAR NOT NULL DEFAULT 'lump',

        -- Provenance: the backtest run this deployment was approved from, so
        -- a live position can always be traced to the evidence for it.
        source_run_id VARCHAR,

        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
        updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
        created_by VARCHAR,

        -- NOTE: "one ACTIVE deployment per (strategy, version, portfolio)"
        -- is deliberately NOT a UNIQUE constraint here. Expressing it needs a
        -- partial unique index (WHERE is_active), which DuckDB does not
        -- support; including is_active in a plain UNIQUE would also forbid a
        -- second RETIRED deployment of the same strategy and so destroy the
        -- history the table exists to keep. It is enforced in the API, which
        -- is the honest place for a rule the schema cannot hold.
        CHECK (initial_capital >= 0),
        CHECK (sip_amount >= 0)
    )
    """,
}

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_strategy_deployments_key "
    "ON strategy_deployments (strategy_key)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_deployments_active "
    "ON strategy_deployments (is_active)",
]


def create_strategy_deployments_schema(
    db_path: Optional[Path] = None,
    in_memory: bool = False,
    conn=None,
) -> None:
    """Create the deployments table. Idempotent.

    `conn` lets a caller reuse an open connection (tests use an in-memory one,
    per the project rule that no test row is ever written to a real database).
    """
    if conn is not None:
        _apply(conn)
        return

    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import DUCKDB_PATH

        db_path = DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(db_path) as c:
        _apply(c)
    logger.info(
        "strategy_deployments schema ready at %s", db_path if db_path else ":memory:"
    )


def _apply(conn) -> None:
    conn.execute(_SEQUENCE)
    for name, ddl in _TABLES.items():
        conn.execute(ddl)
        logger.info("Ensured table exists: %s", name)
    for ddl in _INDEXES:
        conn.execute(ddl)


def list_tables() -> dict:
    return {"duckdb": list(_TABLES.keys())}
