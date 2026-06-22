"""
datastore/schema/create_normalised.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001, SPEC-DS-003, SPEC-DS-007, SPEC-PIPE-003
Owner: Platform / DataStore
Consumers: ingestion/*, datastore/api, features/*, backtest

Creates Store 2 (Normalised) DuckDB tables: ohlcv_adjusted, corporate_actions,
fundamentals, shareholding, macro_indicators, stock_master.

PIT enforcement (SPEC-PIPE-003, SPEC-DS-003) is applied at the API layer
(datastore/api/pit.py), not via schema constraints, because point-in-time
correctness depends on the caller-supplied as_of parameter at query time.
This module enforces the schema-level precondition for PIT correctness instead:
announcement_date and filing_date are NOT NULL, since a row with no known
disclosure date can never be safely filtered by datastore/api/pit.py.
"""

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

# SPEC-DS-001: normalised, corporate-action-adjusted OHLCV
_CREATE_OHLCV_ADJUSTED = """
    CREATE TABLE IF NOT EXISTS ohlcv_adjusted (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume BIGINT NOT NULL,
        delivery_qty BIGINT,
        delivery_pct DOUBLE,
        adj_factor DOUBLE NOT NULL DEFAULT 1.0,
        PRIMARY KEY (date, ticker)
    )
"""

# SPEC-PIPE-002: corporate action log driving idempotent price adjustment
_CREATE_CORPORATE_ACTIONS = """
    CREATE TABLE IF NOT EXISTS corporate_actions (
        ticker VARCHAR NOT NULL,
        ex_date DATE NOT NULL,
        action_type VARCHAR NOT NULL,
        ratio DOUBLE NOT NULL,
        announcement_date DATE,
        record_date DATE,
        PRIMARY KEY (ticker, ex_date, action_type)
    )
"""

# SPEC-PIPE-003 (CRITICAL): announcement_date is the PIT key, never quarter_end_date
_CREATE_FUNDAMENTALS = """
    CREATE TABLE IF NOT EXISTS fundamentals (
        ticker VARCHAR NOT NULL,
        fiscal_year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        quarter_end_date DATE NOT NULL,
        announcement_date DATE NOT NULL,
        revenue DOUBLE,
        ebitda DOUBLE,
        pat DOUBLE,
        eps DOUBLE,
        operating_margin DOUBLE,
        ebitda_margin DOUBLE,
        net_margin DOUBLE,
        roe DOUBLE,
        roce DOUBLE,
        debt_to_equity DOUBLE,
        interest_coverage DOUBLE,
        fcf DOUBLE,
        asset_turnover DOUBLE,
        inventory_days DOUBLE,
        receivable_days DOUBLE,
        payable_days DOUBLE,
        book_value_per_share DOUBLE,
        shares_outstanding BIGINT,
        PRIMARY KEY (ticker, fiscal_year, quarter)
    )
"""

# SPEC-PIPE-003 (CRITICAL): filing_date is the PIT key, never quarter_end_date
_CREATE_SHAREHOLDING = """
    CREATE TABLE IF NOT EXISTS shareholding (
        ticker VARCHAR NOT NULL,
        quarter_end_date DATE NOT NULL,
        filing_date DATE NOT NULL,
        promoter_pct DOUBLE,
        promoter_pledge DOUBLE,
        fii_pct DOUBLE,
        dii_pct DOUBLE,
        mf_pct DOUBLE,
        retail_pct DOUBLE,
        PRIMARY KEY (ticker, quarter_end_date)
    )
"""

_CREATE_MACRO_INDICATORS = """
    CREATE TABLE IF NOT EXISTS macro_indicators (
        date DATE NOT NULL,
        indicator VARCHAR NOT NULL,
        value DOUBLE NOT NULL,
        PRIMARY KEY (date, indicator)
    )
"""

# SPEC-SYS-001, SPEC-SYS-011: universe membership and tiering reference table
_CREATE_STOCK_MASTER = """
    CREATE TABLE IF NOT EXISTS stock_master (
        ticker VARCHAR NOT NULL PRIMARY KEY,
        company_name VARCHAR NOT NULL,
        sector VARCHAR,
        industry VARCHAR,
        nse_series VARCHAR NOT NULL,
        listing_date DATE,
        market_cap_cr DOUBLE,
        adtv_cr DOUBLE,
        current_tier INTEGER,
        is_fno_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        is_nifty500 BOOLEAN NOT NULL DEFAULT FALSE
    )
"""

_ALL_TABLES = {
    "ohlcv_adjusted": _CREATE_OHLCV_ADJUSTED,
    "corporate_actions": _CREATE_CORPORATE_ACTIONS,
    "fundamentals": _CREATE_FUNDAMENTALS,
    "shareholding": _CREATE_SHAREHOLDING,
    "macro_indicators": _CREATE_MACRO_INDICATORS,
    "stock_master": _CREATE_STOCK_MASTER,
}


def create_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create all Store 2 (Normalised) DuckDB tables.

    Idempotent — safe to call multiple times (CREATE TABLE IF NOT EXISTS).

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.DUCKDB_PATH.
        in_memory: If True, create the schema in an in-memory DuckDB
            (db_path is ignored). Used by tests/unit/test_schema.py.

    Raises:
        ImportError: If duckdb is not installed
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import DUCKDB_PATH

        db_path = DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(db_path) as conn:
        for table_name, ddl in _ALL_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")

    logger.info(f"Normalised schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> list:
    """Return the names of all tables created by this module."""
    return list(_ALL_TABLES.keys())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_schema()
