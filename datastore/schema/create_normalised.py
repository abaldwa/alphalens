"""
datastore/schema/create_normalised.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001, SPEC-DS-003, SPEC-DS-007, SPEC-PIPE-003
Owner: Platform / DataStore
Consumers: ingestion/*, datastore/api, features/*, backtest

Creates Store 2 (Normalised) DuckDB tables: ohlcv_adjusted, corporate_actions,
fundamentals, shareholding, fno_data, macro_indicators, stock_master.

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

# SPEC-DS-001 / SPEC-PIPE-002: normalised OHLCV — always holds the
# corporate-action-adjusted values (backward-adjusted to today's basis).
#
# adj_factor     : cumulative price adj factor; adjusted = raw × adj_factor
# vol_adj_factor : cumulative volume adj factor; adj_vol = raw_vol × vol_adj_factor
#
# Original NSE-reported values are NOT stored here.  Only rows that the
# price adjuster has modified appear in the companion ohlcv_ca_audit table
# with their exact original values.  Unmodified rows (adj_factor=1.0) have
# raw == adjusted — no audit entry is created for them.
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
        vol_adj_factor DOUBLE NOT NULL DEFAULT 1.0,
        PRIMARY KEY (date, ticker)
    )
"""

# SPEC-PIPE-002: audit / restore table for the price adjuster.
#
# Populated by price_adjuster.adjust_for_corporate_actions() just before
# it modifies any row in ohlcv_adjusted.  Only rows that have been touched
# by the adjuster appear here; stocks with no corporate actions have no rows.
#
# raw_* columns:  exact NSE-reported values at the time of first adjustment —
#                 NEVER overwritten (ON CONFLICT preserves them).
# adj_factor / vol_adj_factor: cumulative factors last applied; updated on
#   every adjuster run so the audit table is self-contained (knowing what
#   factor was applied to which original value).
#
# Restore a single row:
#   UPDATE ohlcv_adjusted o
#   SET open=a.raw_open, high=a.raw_high, low=a.raw_low, close=a.raw_close,
#       volume=a.raw_volume, delivery_qty=a.raw_delivery_qty,
#       adj_factor=1.0, vol_adj_factor=1.0
#   FROM ohlcv_ca_audit a
#   WHERE o.date=a.date AND o.ticker=a.ticker
#   AND a.ticker='RELIANCE' AND a.date='2019-06-17';
_CREATE_OHLCV_CA_AUDIT = """
    CREATE TABLE IF NOT EXISTS ohlcv_ca_audit (
        date             DATE NOT NULL,
        ticker           VARCHAR NOT NULL,
        raw_open         DOUBLE NOT NULL,
        raw_high         DOUBLE NOT NULL,
        raw_low          DOUBLE NOT NULL,
        raw_close        DOUBLE NOT NULL,
        raw_volume       BIGINT NOT NULL,
        raw_delivery_qty BIGINT,
        adj_factor       DOUBLE NOT NULL,
        vol_adj_factor   DOUBLE NOT NULL,
        PRIMARY KEY (date, ticker)
    )
"""

# SPEC-PIPE-002: corporate action log driving idempotent price adjustment.
# action_type values: SPLIT, BONUS, DIVIDEND, RIGHTS, BUYBACK, QIP, AGM, OTHER.
# ratio semantics depend on action_type:
#   SPLIT:    new shares per old share (e.g. ratio=5 for a 10→2 FV split)
#   BONUS:    bonus shares per held share (e.g. ratio=1 for a 1:1 bonus)
#   DIVIDEND: amount per share in INR (e.g. ratio=10.0 for Rs.10/share)
#   RIGHTS:   rights shares per held (e.g. ratio=0.2 for 1:5 rights)
#   Others:   0.0 (no price-adjustment relevance)
# details: raw purpose string from NSE for auditability / re-parsing.
_CREATE_CORPORATE_ACTIONS = """
    CREATE TABLE IF NOT EXISTS corporate_actions (
        ticker VARCHAR NOT NULL,
        ex_date DATE NOT NULL,
        action_type VARCHAR NOT NULL,
        ratio DOUBLE NOT NULL,
        announcement_date DATE,
        record_date DATE,
        details VARCHAR,
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
        gross_profit DOUBLE,
        capex DOUBLE,
        current_assets DOUBLE,
        current_liabilities DOUBLE,
        total_debt DOUBLE,
        cash_and_equivalents DOUBLE,
        depreciation DOUBLE,
        -- [AS BUILT] ebit/net_debt/debt_to_ebitda are computed by
        -- features/financial_ratios.py from already-scraped raw fields
        -- (ebitda, depreciation, total_debt, cash_and_equivalents) rather
        -- than scraped from a website ratio box — see that module's
        -- docstring and BuildLog.md "Real data sourcing — Financial ratio
        -- derivation" for why roe/roce/debt_to_equity above stay sparse
        -- (they need shareholder equity, which neither free scraper
        -- reliably captures) while these three are ~99%+ computable today.
        ebit DOUBLE,
        net_debt DOUBLE,
        debt_to_ebitda DOUBLE,
        fcf_margin DOUBLE,
        capex_intensity DOUBLE,
        -- [AS BUILT, P3.11] Direct shareholder equity (Equity Capital +
        -- Reserves, INR Cr) read per fiscal year from Screener.in's
        -- #balance-sheet table, which renders ALL historical FY columns
        -- (Mar 2015..Mar 2026) on one page — unlike book_value_per_share
        -- (still a current-snapshot-only header stat, ~9% populated),
        -- this is read across every column, not just the rightmost one.
        -- Patched onto every quarter row of the matching fiscal_year,
        -- same one-value-per-FY pattern as Trendlyne's ROE_A/DEBT_CE_A
        -- annual fields (see scripts/backfill_fundamentals_trendlyne.py).
        -- features/financial_ratios.py prefers this over the
        -- book_value_per_share*shares_outstanding back-derivation when
        -- present. See BuildLog.md "P3.11".
        total_equity DOUBLE,
        -- [AS BUILT, P2.6] Tijori Finance Pro sector-specific operational
        -- metrics (ARPU for telecom, NPA for banking, ANDA approvals for
        -- pharma, etc. — see ingestion/scrapers/tijori.py's _SECTOR_METRICS
        -- map for the full sector->metric-name dictionary). Generic
        -- numbered columns, not one column per metric type, because the
        -- metric *meaning* varies by sector — sector_specific_metric_1's
        -- label for a given row is looked up from tijori.py's map by
        -- stock_master.sector, not fixed at the schema level.
        sector_specific_metric_1 DOUBLE,
        sector_specific_metric_2 DOUBLE,
        sector_specific_metric_3 DOUBLE,
        sector_specific_metric_4 DOUBLE,
        sector_specific_metric_5 DOUBLE,
        sector_specific_metric_6 DOUBLE,
        PRIMARY KEY (ticker, fiscal_year, quarter)
    )
"""

# SPEC-PIPE-003 (CRITICAL): filing_date is the PIT key, never quarter_end_date
#
# [AS BUILT, P2.6] `shareholding` IS this project's "governance" store —
# 12_platform_architecture.md line 320 labels it literally:
# "/governance/  # Shareholding patterns (PIT via filing_date)". The P2.6
# build prompt's "Writes to governance table (superstar_flag,
# superstar_change columns)" therefore resolves to THIS table, not a new
# standalone one — same "the doc's own data-store naming governs over a
# build prompt that assumes a table exists under a different literal name"
# resolution as P2.5's `depreciation` column landing on the existing
# `fundamentals` table rather than a new one.
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
        superstar_flag BOOLEAN,
        superstar_change DOUBLE,
        PRIMARY KEY (ticker, quarter_end_date)
    )
"""

# SPEC-PIPE-001, P2.3: NSE F&O bhavcopy (futures + options), persisted
# per ingestion/scrapers/fno.py's UDiFF column set. No PRIMARY KEY:
# strike/option_type are NULL for futures rows, and the natural write
# pattern (one full day's bhavcopy file arrives atomically) is delete-
# then-insert per trade_date (ingestion/scheduler/daily_pipeline.py's
# step_download_fno), not row-level upsert — same reasoning corporate_actions
# would use if its source weren't already de-duplicated by ex_date.
_CREATE_FNO_DATA = """
    CREATE TABLE IF NOT EXISTS fno_data (
        trade_date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        instrument VARCHAR NOT NULL,
        expiry DATE NOT NULL,
        strike DOUBLE,
        option_type VARCHAR,
        oi BIGINT,
        oi_change BIGINT,
        volume BIGINT,
        settle_price DOUBLE,
        close_price DOUBLE,
        underlying_price DOUBLE
    )
"""

# Large deals: bulk deals (≥0.5% of shares in a single trade) and block
# deals (≥5 lakh shares or ≥Rs.10 crore in the block-deal window) from
# NSE and BSE. No PRIMARY KEY: the same client can have multiple bulk deals
# for the same stock on the same day — delete-then-insert per
# (trade_date, exchange, deal_type) mirrors fno_data's write pattern.
_CREATE_LARGE_DEALS = """
    CREATE TABLE IF NOT EXISTS large_deals (
        trade_date DATE NOT NULL,
        exchange VARCHAR NOT NULL,
        deal_type VARCHAR NOT NULL,
        ticker VARCHAR NOT NULL,
        client_name VARCHAR,
        transaction_type VARCHAR,
        quantity BIGINT,
        price DOUBLE,
        remarks VARCHAR
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
    "ohlcv_ca_audit": _CREATE_OHLCV_CA_AUDIT,
    "corporate_actions": _CREATE_CORPORATE_ACTIONS,
    "fundamentals": _CREATE_FUNDAMENTALS,
    "shareholding": _CREATE_SHAREHOLDING,
    "fno_data": _CREATE_FNO_DATA,
    "large_deals": _CREATE_LARGE_DEALS,
    "macro_indicators": _CREATE_MACRO_INDICATORS,
    "stock_master": _CREATE_STOCK_MASTER,
}

# [AS BUILT, P2.1] This project has no formal migration system — `CREATE
# TABLE IF NOT EXISTS` is a no-op against a table that already exists, so
# extending an EXISTING table's columns (as P2.1 did for `fundamentals`:
# gross_profit, capex, current_assets, current_liabilities, total_debt,
# cash_and_equivalents) silently does NOT reach a real, already-created
# database file — caught live: the real `datastore/normalised/
# alphalens.duckdb` (created back in P0.2, 0 rows, but the table already
# existed) rejected the first real screener.py write with
# `BinderException: ... does not have a column with name "gross_profit"`.
# `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` is DuckDB's idempotent
# equivalent for this case — applied here so any existing DB (this
# project's real one, or anyone else's) self-heals to the current schema
# the next time create_schema() runs, with zero manual migration step.
_MIGRATE_ADDED_COLUMNS = {
    "ohlcv_adjusted": [
        # vol_adj_factor: cumulative share-count adjustment factor for SPLIT/BONUS.
        # raw_ columns (raw_open…raw_delivery_qty) were added and then removed in P3.5;
        # original NSE values now live in ohlcv_ca_audit instead.
        "ALTER TABLE ohlcv_adjusted ADD COLUMN IF NOT EXISTS vol_adj_factor DOUBLE DEFAULT 1.0",
    ],
    "corporate_actions": [
        "ALTER TABLE corporate_actions ADD COLUMN IF NOT EXISTS details VARCHAR",
    ],
    "fundamentals": [
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS gross_profit DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS capex DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS current_assets DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS current_liabilities DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS total_debt DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS cash_and_equivalents DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS depreciation DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS ebit DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS net_debt DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS debt_to_ebitda DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS fcf_margin DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS capex_intensity DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS total_equity DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_1 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_2 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_3 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_4 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_5 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_6 DOUBLE",
    ],
    "shareholding": [
        "ALTER TABLE shareholding ADD COLUMN IF NOT EXISTS superstar_flag BOOLEAN",
        "ALTER TABLE shareholding ADD COLUMN IF NOT EXISTS superstar_change DOUBLE",
    ],
}


def _migrate_added_columns(conn) -> None:
    """Idempotently ALTER any table whose schema has grown since it may have first been created."""
    for table_name, statements in _MIGRATE_ADDED_COLUMNS.items():
        for ddl in statements:
            conn.execute(ddl)
        logger.info(f"Ensured added columns present: {table_name}")


# raw_ columns were added to ohlcv_adjusted in P3.5 then removed in the same
# phase when the design switched to the ohlcv_ca_audit table. Any DB that ran
# the P3.5 intermediate migration will have these orphan columns.
# DuckDB does not support `DROP COLUMN IF EXISTS`, so we check information_schema
# first and skip silently if the columns are already gone.
_DROP_ORPHAN_COLUMNS = {
    "ohlcv_adjusted": [
        "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "raw_delivery_qty",
    ],
}


def _migrate_dropped_columns(conn) -> None:
    """Drop any columns that were removed from the schema in a later phase."""
    for table_name, cols in _DROP_ORPHAN_COLUMNS.items():
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND column_name = ANY(?)",
                [table_name, cols],
            ).fetchall()
        }
        for col in cols:
            if col in existing:
                try:
                    conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {col}")
                    logger.info(f"Dropped orphan column {table_name}.{col}")
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Could not drop {table_name}.{col}: {exc}")


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
        _migrate_added_columns(conn)
        _migrate_dropped_columns(conn)

    logger.info(f"Normalised schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> list:
    """Return the names of all tables created by this module."""
    return list(_ALL_TABLES.keys())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_schema()
