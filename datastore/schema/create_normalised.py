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

# CA4 (2026-07-05, scripts/validate_corporate_actions_fyers.py): tracks
# per-row Fyers cross-validation of corporate_actions so a bad ratio/missing
# action (as CA2 found for KANSAINER/AJOONI) surfaces automatically instead
# of silently corrupting adj_close. Keyed the same way as corporate_actions
# (ticker, ex_date, action_type) so each action has exactly one validation
# row. validation_status: 'unchecked' (default, not yet run) | 'confirmed' |
# 'mismatch' | 'insufficient_window' | 'no_fyers_data' | 'error'.
# needs_retrain: True once a 'mismatch' is confirmed — signals any
# ML feature/model built on this ticker's price history may need a
# recompute once the underlying corporate_actions row is fixed.
# Added to this rebuild-from-scratch schema 2026-07-11 (CA4 follow-up) —
# previously this table only existed in the live DB (see BuildLog.md
# 2026-07-05/08 for the original build).
_CREATE_CORPORATE_ACTIONS_VALIDATION = """
    CREATE TABLE IF NOT EXISTS corporate_actions_validation (
        ticker VARCHAR NOT NULL,
        ex_date DATE NOT NULL,
        action_type VARCHAR NOT NULL,
        ratio DOUBLE,
        expected_price_factor DOUBLE,
        observed_price_factor DOUBLE,
        pct_diff DOUBLE,
        validation_status VARCHAR DEFAULT 'unchecked',
        fyers_validated_at TIMESTAMP,
        needs_retrain BOOLEAN DEFAULT FALSE,
        notes VARCHAR,
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
        -- [AS BUILT, deep-forensic altman_z fix 2026-07-07] "Reserves"
        -- (INR Cr) is a real, separately-labeled row in the same
        -- Screener.in #balance-sheet table total_equity above already
        -- reads (`_parse_balance_sheet_history`, previously it only kept
        -- the equity_capital+reserves SUM and discarded the reserves
        -- component). Reserves & Surplus is the standard accounting
        -- analog of "retained earnings" (accumulated profits not paid
        -- out as equity capital) used in the classic Altman Z-Score X2
        -- term — this is a real, separately-sourced field, not a
        -- fabricated split of total_equity. Feeds features/deep_forensic.py's
        -- altman_z, which was previously always NaN (its retained_earnings
        -- input had no backing column at all).
        retained_earnings DOUBLE,
        -- [AS BUILT, deep-forensic 20-field gap fix] Total Assets and CWIP
        -- (Capital Work in Progress) are REAL labeled rows in Screener.in's
        -- free-tier #balance-sheet table (verified live against TCS's real
        -- consolidated page 2026-07-07 — "CWIP" and "Total Assets" both
        -- render as distinct rows: CWIP 1,564/1,546/2,665 Cr for FY24-26,
        -- Total Assets 145,472/158,649/181,167 Cr, identical to Total
        -- Liabilities per the balance-sheet identity). Unlike goodwill,
        -- intangibles, contingent liabilities, subsidiary_count, and
        -- related-party loans (grepped for on the same real page — zero
        -- matches; Screener's free tier genuinely does not expose these as
        -- distinct line items, only the "Related Party Transactions" modal
        -- which requires Premium login AND still renders "xxx" placeholder
        -- cells for most historical years — see features/deep_forensic.py's
        -- module docstring for the full documented gap), total_assets/cwip
        -- were previously omitted from this schema even though the raw
        -- source data was sitting on every already-scraped page. Enables
        -- real (non-fabricated) cwip_ratio and asset_inflation_flag in
        -- features/deep_forensic.py; goodwill_ratio, contingent_liability_
        -- ratio, loans_to_related, subsidiary_count, capex_to_assets,
        -- intangibles_growth, off_balance_sheet_proxy, noncash_assets_ratio
        -- remain NaN — no free structured source found for their numerators.
        total_assets DOUBLE,
        cwip DOUBLE,
        -- [AS BUILT, 2026-07-07, NSE XBRL pipeline] The above comment's "no
        -- free structured source found" is now WRONG for goodwill and
        -- inventory/receivable/payable days — corrected same day. NSE's own
        -- SEBI-mandated "Integrated Filing — IndAS" regulatory disclosure
        -- (api/integrated-filing-results -> real iXBRL HTML per quarter,
        -- live-verified against RELIANCE's real 2026-03-31 filing) contains
        -- a complete, standardized "Statement of Asset and Liabilities" with
        -- goodwill, inventories, trade receivables/payables (current AND
        -- non-current), total liabilities, and a real audit-qualification
        -- declaration — none of which Screener's/Trendlyne's free tiers
        -- expose. See ingestion/scrapers/nse_xbrl_financials.py for the
        -- parser and scripts/backfill_fundamentals_nse_xbrl.py for the
        -- backfill. Per explicit operator instruction: NSE XBRL is now the
        -- PREFERRED/primary source for these fields (more authoritative —
        -- the regulatory filing itself, not a third-party's rendering of
        -- it); Screener/Trendlyne remain the fallback where NSE's
        -- Integrated Filing regime doesn't yet cover a company/quarter
        -- (the regime only fully phased in from FY2023-24 on).
        -- contingent_liability_ratio/subsidiary_count/loans_to_related/
        -- capex_to_assets/intangibles_growth/off_balance_sheet_proxy
        -- remain genuine gaps: verified live that "Disclosure of notes on
        -- assets and liabilities" is freeform "Textual Information", not a
        -- structured numeric field NSE's iXBRL exposes.
        goodwill DOUBLE,
        inventories DOUBLE,
        trade_receivables_current DOUBLE,
        trade_payables_current DOUBLE,
        total_liabilities DOUBLE,
        audit_qualified_flag BOOLEAN,
        -- 2026-07-07 (same-day follow-up, per explicit operator instruction
        -- "add additional columns as necessary, do not skip any datapoints,
        -- it might be required for some calculations"): the rest of the
        -- real, distinct line items NSE's Statement of Asset and
        -- Liabilities exposes, beyond the initial 6 above. Same source/
        -- authority as goodwill/inventories/etc. — see this table's comment
        -- above them. Not fabricated estimates; every field here is a
        -- separately-labeled real row live-verified against RELIANCE's
        -- 2026-03-31 and 2025-09-30 filings.
        property_plant_equipment DOUBLE,
        intangible_assets DOUBLE,
        non_current_investments DOUBLE,
        non_current_trade_receivables DOUBLE,
        deferred_tax_assets DOUBLE,
        current_investments DOUBLE,
        current_tax_assets DOUBLE,
        borrowings_current DOUBLE,
        borrowings_noncurrent DOUBLE,
        deferred_tax_liabilities DOUBLE,
        provisions_current DOUBLE,
        provisions_noncurrent DOUBLE,
        equity_share_capital DOUBLE,
        other_equity DOUBLE,
        non_controlling_interest DOUBLE,
        non_current_liabilities DOUBLE,
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
        -- [AS BUILT, backlog #12/AF-5] Populated by
        -- features/fundamental_quality_gate.py's validate_and_annotate(),
        -- called from scripts/backfill_fundamentals_trendlyne.py and
        -- scripts/backfill_fundamentals_nse_xbrl.py before every write. Flags
        -- (never rejects) rows with a ratio field outside its plausible
        -- range (e.g. a margin stored as 0-100 instead of 0-1) so a units
        -- bug like the operating_margin/net_margin one (BuildLog.md
        -- "Fundamental Dashboard OpMargin/NetMargin Wrong") is caught at
        -- write time instead of by hand months later. quality_flag_reason
        -- holds a human-readable detail per flagged field; NULL/false
        -- means the row passed every check (or was revenue-exempt).
        quality_flag BOOLEAN,
        quality_flag_reason VARCHAR,
        -- [AS BUILT, A36 fix 2026-07-09] Row-level provenance: which of the
        -- 3 writers (trendlyne/nse_xbrl/screener; kaggle removed A53, see
        -- features/fundamental_source_priority.py) most recently won a
        -- real (non-NULL-filling) conflict on this row. Row-level, not
        -- per-field, matching this table's existing granularity (no other
        -- column tracks per-field provenance either) — good enough to
        -- resolve "which source should win" without a much larger
        -- per-field-provenance migration. NULL for rows written before
        -- this fix (self-heals: any subsequent write from a covered
        -- writer sets both fields going forward, per
        -- build_priority_update_clause's NULL-existing-priority handling).
        fundamentals_source VARCHAR,
        fundamentals_source_priority INTEGER,
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

# Big Investor Activity, Phase B (plan: gentle-wobbling-swing.md) —
# related-party/investor-family seed mapping. entity_name is the
# normalized (upper-case, whitespace-collapsed) raw client_name as it
# appears in large_deals, so the join against large_deals.client_name is a
# simple exact match rather than fuzzy matching at query time.
_CREATE_INVESTOR_FAMILY = """
    CREATE TABLE IF NOT EXISTS investor_family (
        entity_name VARCHAR NOT NULL PRIMARY KEY,
        family_id VARCHAR NOT NULL,
        family_display_name VARCHAR NOT NULL,
        match_type VARCHAR,
        source VARCHAR,
        confidence DOUBLE,
        added_date DATE NOT NULL,
        notes VARCHAR
    )
"""

# Big Investor Activity, Phase B — derived, rebuildable from large_deals +
# investor_family. Never a second source of truth: the daily attribution
# step (and any full rebuild) can always regenerate this from those two
# tables plus the intraday-netting logic. family_id is either a real
# investor_family.family_id, or 'unmapped:<normalized_client_name>' for
# clients with no known family mapping yet.
_CREATE_BULK_DEAL_POSITIONS = """
    CREATE TABLE IF NOT EXISTS bulk_deal_positions (
        family_id VARCHAR NOT NULL,
        ticker VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        deal_type VARCHAR NOT NULL,
        net_transaction_type VARCHAR,
        net_quantity BIGINT,
        avg_price DOUBLE,
        exchange VARCHAR,
        cumulative_position_est BIGINT,
        is_new_entry BOOLEAN NOT NULL DEFAULT FALSE,
        is_full_exit BOOLEAN NOT NULL DEFAULT FALSE,
        source_correction_id BIGINT,
        PRIMARY KEY (family_id, ticker, trade_date, deal_type)
    )
"""

# Big Investor Activity, Phase C (plan: gentle-wobbling-swing.md) — promotes
# the existing datastore/normalised/mf_holdings/YYYY-MM.parquet snapshots
# (written by ingestion/scrapers/amfi_holdings.py) into a queryable DuckDB
# table, so the API can compute month-over-month movers without loading
# parquet per request. The parquet files remain the raw/audit artifact;
# this table is synced from them (see amfi_holdings.sync_duckdb_table).
# month is stored as the first-of-month DATE to match the parquet
# partition; availability_date is the PIT gate (SPEC-PIPE-003 — same
# discipline as shareholding.filing_date), never `month` itself.
_CREATE_MF_HOLDINGS = """
    CREATE TABLE IF NOT EXISTS mf_holdings (
        ticker VARCHAR NOT NULL,
        month DATE NOT NULL,
        scheme_name VARCHAR NOT NULL,
        isin VARCHAR,
        quantity BIGINT,
        value_inr DOUBLE,
        availability_date DATE NOT NULL,
        PRIMARY KEY (ticker, month, scheme_name)
    )
"""

# Big Investor Activity, Phase D (plan: gentle-wobbling-swing.md) — named
# public-shareholder disclosures (>1% holders), sourced from Trendlyne's
# superstar-investor pages (ingestion/scrapers/trendlyne.py, all ~62
# investors on Trendlyne's index — extended to also persist per-investor
# stake_pct here rather than only the aggregated shareholding.superstar_*
# columns). family_id links to investor_family where the holder_name
# matches a seeded entity; NULL if unmatched. One row per
# (ticker, holder_name, quarter_end_date) — a holder can appear across
# multiple quarters as their stake changes.
#
# reported_shares is a REAL absolute share count ("Qty Held" on
# Trendlyne's page, confirmed via a real authenticated fetch 2026-07-05)
# — when present, ingestion/scrapers/bulk_deal_reconciliation.py uses this
# directly instead of deriving shares outstanding from market_cap_cr /
# close price (its fallback for quarters where Trendlyne shows "-" /
# "Filing Awaited" for Qty Held).
_CREATE_PUBLIC_SHAREHOLDERS = """
    CREATE TABLE IF NOT EXISTS public_shareholders (
        ticker VARCHAR NOT NULL,
        holder_name VARCHAR NOT NULL,
        quarter_end_date DATE NOT NULL,
        filing_date DATE NOT NULL,
        family_id VARCHAR,
        stake_pct DOUBLE,
        qoq_change_pct DOUBLE,
        reported_shares BIGINT,
        source VARCHAR NOT NULL,
        fetched_at TIMESTAMP NOT NULL,
        PRIMARY KEY (ticker, holder_name, quarter_end_date)
    )
"""

# Big Investor Activity, Phase D — audit trail for reconciling
# bulk_deal_positions' estimated family position against public_shareholders'
# reported stake for the same family+ticker+quarter. See
# ingestion/scrapers/bulk_deal_reconciliation.py.
_CREATE_BULK_DEAL_RECONCILIATION_LOG = """
    CREATE TABLE IF NOT EXISTS bulk_deal_reconciliation_log (
        id BIGINT NOT NULL,
        family_id VARCHAR NOT NULL,
        ticker VARCHAR NOT NULL,
        quarter_end_date DATE NOT NULL,
        filing_date DATE NOT NULL,
        estimated_position_pre_correction BIGINT,
        reported_shares_est BIGINT,
        correction_applied BOOLEAN NOT NULL DEFAULT FALSE,
        correction_delta BIGINT,
        discrepancy_pct DOUBLE,
        status VARCHAR NOT NULL,
        reviewed_by VARCHAR,
        reviewed_at TIMESTAMP,
        notes VARCHAR,
        PRIMARY KEY (id)
    )
"""

_CREATE_INDEX_OHLCV = """
    CREATE TABLE IF NOT EXISTS index_ohlcv (
        date DATE NOT NULL,
        index_name VARCHAR NOT NULL,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        PRIMARY KEY (date, index_name)
    )
"""

# [AS BUILT, 2026-07-07] Real NSE Corporate Announcements feed
# (nseindia.com/api/corporate-announcements — live-verified real JSON,
# 18,036 rows over a 5-week window, `desc` field is one of ~90 real NSE
# taxonomy categories). Filtered at ingestion time to "material event"
# categories only (Buyback/QIP/Board changes/Investigations/Credit
# Rating/Auditor changes/M&A — see ingestion/scrapers/
# nse_corporate_announcements.py's _MATERIAL_CATEGORIES for the exact
# list); routine noise (Board Meeting outcomes, Dividend/Rights/Split/
# Bonus notices, generic Press Release/Updates) is dropped, not stored —
# an explicit user decision (recommended-scope option), not a technical
# constraint on the source, which has all of it.
_CREATE_CORPORATE_ANNOUNCEMENTS = """
    CREATE TABLE IF NOT EXISTS corporate_announcements (
        seq_id VARCHAR NOT NULL,
        ticker VARCHAR NOT NULL,
        company_name VARCHAR,
        category VARCHAR NOT NULL,
        subject VARCHAR,
        announcement_text VARCHAR,
        announced_at TIMESTAMP NOT NULL,
        exchange_disseminated_at TIMESTAMP,
        attachment_url VARCHAR,
        PRIMARY KEY (seq_id)
    )
"""

# A20 (Data Integrity Checker): RCA + fix-proposal output for the four
# integrity checks (datastore/integrity/checks.py). Findings always land
# as status='pending' — approve_finding()/reject_finding()
# (datastore/integrity/findings.py) are the only path to 'applied'/
# 'rejected', matching this project's "flag, don't silently write"
# discipline (A12, A25's staging.rejected_rows).
_CREATE_DATA_INTEGRITY_FINDINGS = """
    CREATE SEQUENCE IF NOT EXISTS data_integrity_findings_id_seq;
    CREATE TABLE IF NOT EXISTS data_integrity_findings (
        id BIGINT PRIMARY KEY DEFAULT nextval('data_integrity_findings_id_seq'),
        check_name VARCHAR NOT NULL,
        ticker VARCHAR,
        finding_date DATE NOT NULL,
        severity VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        evidence_json VARCHAR,
        proposed_fix_sql VARCHAR,
        proposed_fix_params_json VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'pending',
        reviewed_by VARCHAR,
        reviewed_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
    )
"""

# A21 (Pipeline Health Checker): append-only per-invocation history for
# every recurring scheduled job. scheduler_heartbeats (SQLite,
# PIPELINE_LOG_DB_PATH) only ever upserts the LATEST attempt per job_id —
# there is no way to answer "did weekend_feature_backfill actually
# succeed 7 days ago" from it. This table is written alongside that
# upsert by ingestion/scheduler/pipeline_scheduler.py::_record_heartbeat
# (no call-site changes needed) so every job gets real per-invocation
# history going forward. Never updated/deleted — a log, not a state
# table. History only starts accumulating once this ships (same "needs
# real weeks of data" caveat as A23's benchmark history).
# A23 (benchmark history): duration_seconds/peak_rss_mb added alongside
# the original A21 columns, written by the same _record_heartbeat call —
# no new storage system, just wider rows on what's already there. Both
# nullable: rows written before this shipped, and any call site that
# can't measure timing for some reason, simply leave them NULL rather
# than needing a schema migration/backfill. peak_rss_mb is a best-effort
# approximation (see pipeline_scheduler.py::_job_timing docstring for the
# ru_maxrss high-water-mark caveat), not an exact per-run figure — still
# useful for the relative weekday/weekend trend comparison A23 is for.
_CREATE_JOB_RUN_LOG = """
    CREATE SEQUENCE IF NOT EXISTS job_run_log_id_seq;
    CREATE TABLE IF NOT EXISTS job_run_log (
        id BIGINT PRIMARY KEY DEFAULT nextval('job_run_log_id_seq'),
        job_id VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        error VARCHAR,
        duration_seconds DOUBLE,
        peak_rss_mb DOUBLE,
        recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp
    )
"""

# A21 (Pipeline Health Checker): missed-job findings, same "flag, don't
# silently write" shape as A20's data_integrity_findings — see
# datastore/health/findings.py. proposed_catchup_action/params describe
# a catch-up ACTION (force-run a pipeline date, re-run a weekend script,
# re-run mf_holdings ingestion) rather than a SQL fix, since a missed job
# isn't a bad row to correct, it's work that never happened.
_CREATE_MISSED_JOB_FINDINGS = """
    CREATE SEQUENCE IF NOT EXISTS missed_job_findings_id_seq;
    CREATE TABLE IF NOT EXISTS missed_job_findings (
        id BIGINT PRIMARY KEY DEFAULT nextval('missed_job_findings_id_seq'),
        job_id VARCHAR NOT NULL,
        missed_date DATE NOT NULL,
        severity VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        proposed_catchup_action VARCHAR,
        proposed_catchup_params_json VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'pending',
        reviewed_by VARCHAR,
        reviewed_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
    )
"""

# CA6 (2026-07-10): NSE's real `api/corporate-further-issues-qip` endpoint
# — live-verified against IDFCFIRSTB/ZOMATO, fully structured JSON (no XBRL
# parsing needed, unlike BRSR below). appId is NSE's own per-filing ID —
# used as the natural conflict key since a company can do multiple QIPs.
_CREATE_QIP_DETAILS = """
    CREATE TABLE IF NOT EXISTS qip_details (
        ticker VARCHAR NOT NULL,
        app_id VARCHAR NOT NULL,
        board_resolution_date DATE,
        allotment_date DATE,
        listing_date DATE,
        issue_price DOUBLE,
        min_issue_price DOUBLE,
        final_issue_size DOUBLE,
        no_of_allottees INTEGER,
        no_of_shares_allotted BIGINT,
        no_of_equity_shares_listed BIGINT,
        dilution_pct DOUBLE,
        PRIMARY KEY (ticker, app_id)
    )
"""

# CA6 (2026-07-10): NSE's real `api/corporate-bussiness-sustainabilitiy`
# endpoint — live-verified against RELIANCE. Scope deliberately limited to
# the filing INDEX (submission date, XBRL file URL) rather than parsing
# every BRSR ESG metric out of the linked XBRL XML — that's a much larger,
# separately-scoped effort (hundreds of BRSR-specific tags), not attempted
# here. fy_to as part of the key since a company files at most once per FY.
_CREATE_BRSR_FILINGS = """
    CREATE TABLE IF NOT EXISTS brsr_filings (
        ticker VARCHAR NOT NULL,
        fy_from INTEGER,
        fy_to INTEGER NOT NULL,
        submission_date DATE,
        xbrl_file_url VARCHAR,
        attachment_file_url VARCHAR,
        PRIMARY KEY (ticker, fy_to)
    )
"""


# ML30 (2026-07-13): MyHoldings moves off browser localStorage
# (dashboard/static/ml/holdings.html's prior client-only storage) into a
# real DuckDB table — CRUD via datastore/api/routers/holdings.py.
# `id` is a DuckDB SEQUENCE-backed surrogate key (not (ticker,
# purchase_date), since a real investor can legitimately buy the same
# ticker on the same date in two separate lots/orders — no natural unique
# key exists here, unlike most of this schema's other tables). sale_date/
# sell_price/sell_rationale are NULL for a still-open position; NULL is
# the correct "not yet sold" state, never a fabricated 0/empty string.
# Per CLAUDE.md's DuckDB-migration convention (this project has no formal
# migration system), this DDL only runs via `CREATE TABLE IF NOT EXISTS`
# — safe/idempotent whenever it's eventually applied to the real
# datastore/normalised/alphalens.duckdb file (deliberately NOT run against
# production this session — see ML30's FeatureBacklog.md note: ML31/A26
# jobs may hold the production DB's write lock).
_CREATE_MY_HOLDINGS = """
    CREATE SEQUENCE IF NOT EXISTS my_holdings_id_seq START 1;
    CREATE TABLE IF NOT EXISTS my_holdings (
        id BIGINT PRIMARY KEY DEFAULT nextval('my_holdings_id_seq'),
        ticker VARCHAR NOT NULL,
        purchase_date DATE NOT NULL,
        qty DOUBLE NOT NULL,
        purchase_price DOUBLE,
        sale_date DATE,
        sell_price DOUBLE,
        purchase_rationale VARCHAR,
        sell_rationale VARCHAR,
        journal_entry VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
        updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
    )
"""

_ALL_TABLES = {
    "ohlcv_adjusted": _CREATE_OHLCV_ADJUSTED,
    "index_ohlcv": _CREATE_INDEX_OHLCV,
    "ohlcv_ca_audit": _CREATE_OHLCV_CA_AUDIT,
    "corporate_actions": _CREATE_CORPORATE_ACTIONS,
    "corporate_actions_validation": _CREATE_CORPORATE_ACTIONS_VALIDATION,
    "qip_details": _CREATE_QIP_DETAILS,
    "brsr_filings": _CREATE_BRSR_FILINGS,
    "fundamentals": _CREATE_FUNDAMENTALS,
    "shareholding": _CREATE_SHAREHOLDING,
    # A50 (2026-07-10): fno_data deliberately NOT in this dict — it lives in
    # its own file (config.settings.FNO_DATA_DB_PATH) for a real DB, created
    # separately in create_schema() below via the ATTACHed fno_db alias. For
    # in_memory=True (tests), it's created inline here like every other
    # table — the file-split only matters for the live multi-process lock
    # contention scenario, not test isolation.
    "large_deals": _CREATE_LARGE_DEALS,
    "macro_indicators": _CREATE_MACRO_INDICATORS,
    "stock_master": _CREATE_STOCK_MASTER,
    "investor_family": _CREATE_INVESTOR_FAMILY,
    "bulk_deal_positions": _CREATE_BULK_DEAL_POSITIONS,
    "mf_holdings": _CREATE_MF_HOLDINGS,
    "public_shareholders": _CREATE_PUBLIC_SHAREHOLDERS,
    "bulk_deal_reconciliation_log": _CREATE_BULK_DEAL_RECONCILIATION_LOG,
    "corporate_announcements": _CREATE_CORPORATE_ANNOUNCEMENTS,
    "data_integrity_findings": _CREATE_DATA_INTEGRITY_FINDINGS,
    "job_run_log": _CREATE_JOB_RUN_LOG,
    "missed_job_findings": _CREATE_MISSED_JOB_FINDINGS,
    "my_holdings": _CREATE_MY_HOLDINGS,
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
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS retained_earnings DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_1 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_2 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_3 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_4 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_5 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS sector_specific_metric_6 DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS quality_flag BOOLEAN",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS quality_flag_reason VARCHAR",
        # [AS BUILT, deep-forensic 20-field gap fix] see _CREATE_FUNDAMENTALS
        # comment above total_assets/cwip for sourcing rationale.
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS total_assets DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS cwip DOUBLE",
        # [AS BUILT, 2026-07-07, NSE XBRL pipeline] see _CREATE_FUNDAMENTALS
        # comment above these columns for sourcing rationale.
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS goodwill DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS inventories DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS trade_receivables_current DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS trade_payables_current DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS total_liabilities DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS audit_qualified_flag BOOLEAN",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS property_plant_equipment DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS intangible_assets DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS non_current_investments DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS non_current_trade_receivables DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS deferred_tax_assets DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS current_investments DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS current_tax_assets DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS borrowings_current DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS borrowings_noncurrent DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS deferred_tax_liabilities DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS provisions_current DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS provisions_noncurrent DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS equity_share_capital DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS other_equity DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS non_controlling_interest DOUBLE",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS non_current_liabilities DOUBLE",
        # [AS BUILT, A36 fix 2026-07-09] see _CREATE_FUNDAMENTALS comment
        # above these two columns for rationale.
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS fundamentals_source VARCHAR",
        "ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS fundamentals_source_priority INTEGER",
    ],
    "shareholding": [
        "ALTER TABLE shareholding ADD COLUMN IF NOT EXISTS superstar_flag BOOLEAN",
        "ALTER TABLE shareholding ADD COLUMN IF NOT EXISTS superstar_change DOUBLE",
    ],
    "public_shareholders": [
        # reported_shares: real "Qty Held" from Trendlyne (added after the
        # table's initial Phase D creation, once a live authenticated
        # fetch confirmed Trendlyne reports this directly — see
        # ingestion/scrapers/trendlyne.py's _parse_holdings_table).
        "ALTER TABLE public_shareholders ADD COLUMN IF NOT EXISTS reported_shares BIGINT",
    ],
    "job_run_log": [
        # A23: benchmark history columns, added after job_run_log's initial
        # A21 creation — see _CREATE_JOB_RUN_LOG's comment for rationale.
        "ALTER TABLE job_run_log ADD COLUMN IF NOT EXISTS duration_seconds DOUBLE",
        "ALTER TABLE job_run_log ADD COLUMN IF NOT EXISTS peak_rss_mb DOUBLE",
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

    # persist=False (SPEC-SCHED-013): this can run at startup of either the
    # long-lived scheduler process or the long-lived API process, both of
    # which share this same DuckDB file — DuckDB allows only one read-write
    # connection at a time, so a persistent (cached, held-open) connection
    # here would deadlock against the other process's own persistent
    # connection on every restart. Release the write lock immediately after
    # ensuring tables exist.
    with get_duckdb_connection(db_path, persist=False) as conn:
        for table_name, ddl in _ALL_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        # A50 (2026-07-10): fno_data lives in its own file for a real DB
        # (get_duckdb_connection already ATTACHed it as `fno_db` for any
        # connection to the real DUCKDB_PATH — see datastore/api/db.py)
        # — qualify the CREATE explicitly since unqualified CREATE TABLE
        # targets the current/default schema, not search_path resolution
        # (unlike SELECT/INSERT/DELETE, which DO follow search_path).
        # in_memory=True keeps fno_data inline like every other table.
        fno_ddl = _CREATE_FNO_DATA if in_memory else _CREATE_FNO_DATA.replace(
            "CREATE TABLE IF NOT EXISTS fno_data", "CREATE TABLE IF NOT EXISTS fno_db.fno_data"
        )
        conn.execute(fno_ddl)
        logger.info("Ensured table exists: fno_data")
        _migrate_added_columns(conn)
        _migrate_dropped_columns(conn)

    logger.info(f"Normalised schema ready at {db_path if db_path else ':memory:'}")


def list_tables() -> list:
    """Return the names of all tables created by this module."""
    return list(_ALL_TABLES.keys())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_schema()
