"""
tests/unit/test_schema.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001 through SPEC-DS-007, SPEC-PIPE-003, SPEC-QUALITY-001
Owner: Platform / DataStore
Consumers: CI, pytest

Verifies datastore/schema/create_normalised.py and create_signals.py:
all tables are created with the documented columns, and OHLCV queries
respect the as_of point-in-time rule (SPEC-PIPE-003: never return rows
dated after the reference date).
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection, get_sqlite_connection
from datastore.schema import create_normalised, create_signals

# Expected columns per alphalens_docs/12_platform_architecture.md "Six Stores"
NORMALISED_TABLE_COLUMNS = {
    "ohlcv_adjusted": {
        "date", "ticker", "open", "high", "low", "close", "volume",
        "delivery_qty", "delivery_pct", "adj_factor",
        # [AS BUILT] vol_adj_factor added for volume-adjusted corporate action tracking
        "vol_adj_factor",
    },
    "corporate_actions": {
        "ticker", "ex_date", "action_type", "ratio",
        "announcement_date", "record_date",
        # [AS BUILT] details column for human-readable action description
        "details",
    },
    "fundamentals": {
        "ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
        "revenue", "ebitda", "pat", "eps", "operating_margin", "ebitda_margin",
        "net_margin", "roe", "roce", "debt_to_equity", "interest_coverage", "fcf",
        "asset_turnover", "inventory_days", "receivable_days", "payable_days",
        "book_value_per_share", "shares_outstanding",
        # [AS BUILT, P2.1] added for features/fundamental.py's gross_margin,
        # capex_intensity, current_ratio, net_debt_to_ebitda, roic
        "gross_profit", "capex", "current_assets", "current_liabilities",
        "total_debt", "cash_and_equivalents",
        # [AS BUILT, P2.5] already parsed by screener.py but never persisted until
        # now — exposed for classical_scores.py's Beneish DEPI / Ohlson FFO inputs.
        "depreciation",
        # [AS BUILT, P2.6] Tijori Finance Pro sector-specific operational metrics
        # (ARPU/NPA/ANDA/etc. — see ingestion/scrapers/tijori.py's _SECTOR_METRICS).
        "sector_specific_metric_1", "sector_specific_metric_2", "sector_specific_metric_3",
        "sector_specific_metric_4", "sector_specific_metric_5", "sector_specific_metric_6",
        # [AS BUILT] additional derived metrics for Damodaran valuation and deep forensic
        "debt_to_ebitda", "capex_intensity", "fcf_margin", "total_equity", "ebit", "net_debt",
        # [AS BUILT, deep-forensic altman_z fix 2026-07-07] Reserves alone
        # (retained-earnings analog), kept separate from total_equity —
        # see create_normalised.py's column comment.
        "retained_earnings",
        # [AS BUILT, backlog #12/AF-5] fundamentals range/sanity gate — see
        # features/fundamental_quality_gate.py.
        "quality_flag", "quality_flag_reason",
        # [AS BUILT, deep-forensic 20-field gap fix] real Screener.in
        # free-tier #balance-sheet rows, previously never captured — see
        # datastore/schema/create_normalised.py's column comment.
        "total_assets", "cwip",
        # [AS BUILT, 2026-07-07, NSE XBRL pipeline] real, standardized fields
        # from NSE's SEBI-mandated Integrated Filing — IndAS regulatory
        # disclosure — see create_normalised.py's column comment.
        "goodwill", "inventories", "trade_receivables_current", "trade_payables_current",
        "total_liabilities", "audit_qualified_flag",
        "property_plant_equipment", "intangible_assets", "non_current_investments",
        "non_current_trade_receivables", "deferred_tax_assets", "current_investments",
        "current_tax_assets", "borrowings_current", "borrowings_noncurrent",
        "deferred_tax_liabilities", "provisions_current", "provisions_noncurrent",
        "equity_share_capital", "other_equity", "non_controlling_interest", "non_current_liabilities",
    },
    "shareholding": {
        "ticker", "quarter_end_date", "filing_date", "promoter_pct",
        "promoter_pledge", "fii_pct", "dii_pct", "mf_pct", "retail_pct",
        # [AS BUILT, P2.6] Trendlyne StratQ superstar-investor tracking — `shareholding`
        # IS this project's "governance" store (12_platform_architecture.md line 320).
        "superstar_flag", "superstar_change",
    },
    "macro_indicators": {"date", "indicator", "value"},
    "stock_master": {
        "ticker", "company_name", "sector", "industry", "nse_series",
        "listing_date", "market_cap_cr", "adtv_cr", "current_tier",
        "is_fno_eligible", "is_nifty500",
    },
}

SIGNAL_DUCKDB_TABLE_COLUMNS = {
    "ml_signals": {
        "date", "ticker", "model_name", "model_version", "signal_direction",
        "buy_prob", "hold_prob", "sell_prob", "q10_return", "q50_return", "q90_return",
        "meta_label", "meta_prob", "conformal_lower", "conformal_upper",
        "pnd_score", "pnd_phase", "pnd_block", "hmm_regime", "hmm_regime_prob",
        "hmm_stability", "exit_urgency", "exit_type",
        "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",  # [AS BUILT, P1.7]
        "shap_top5_json",
    },
    "ml_multibagger": {
        "date", "ticker", "mb_probability", "mb_tier", "mb_archetype",
        "survival_6m", "survival_12m", "survival_24m", "survival_36m",
        # [AS BUILT, P2.6] MultibaggerModel.predict_full() emits all 5
        # SURVIVAL_HORIZONS_MONTHS = (6, 12, 18, 24, 36) — this Phase 0.2
        # DDL was missing 18m.
        "survival_18m",
        "shap_top5_json", "analogues_json",
    },
    "ml_forensic": {
        "date", "ticker", "beneish_m", "altman_z", "piotroski_f", "ohlson_o",
        "dechow_f", "sloan_accrual", "benford_mad", "forensic_composite",
        "forensic_flag", "forensic_ml_prob", "shap_top5_json", "pattern_match",
        # [AS BUILT, P2.6] forensic_ml.py's actual 5-level flag taxonomy
        # (green/yellow/orange/red/black) — forensic_flag (BOOLEAN) stays
        # "blocked" semantics only.
        "forensic_flag_label",
    },
}

PIPELINE_RUNS_COLUMNS = {
    "run_id", "date", "started_at", "completed_at",
    "status", "stocks_processed", "error_message",
}


def _duckdb_columns(conn, table_name: str) -> set:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table_name],
    ).fetchall()
    return {r[0] for r in rows}


def _sqlite_columns(conn, table_name: str) -> set:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


# ===== create_normalised.py =====
class TestCreateNormalisedSchema:
    """SPEC-DS-007 Store 2: normalised DuckDB tables."""

    def test_all_tables_created(self):
        """SPEC-DS-001, SPEC-DS-007: all 6 normalised tables must exist."""
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            existing = {
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchall()
            }

        for table_name in NORMALISED_TABLE_COLUMNS:
            assert table_name in existing, f"{table_name} was not created"

    @pytest.mark.parametrize("table_name", sorted(NORMALISED_TABLE_COLUMNS))
    def test_table_columns_match_architecture_doc(self, table_name):
        """SPEC-DS-007: each table's columns must match 12_platform_architecture.md."""
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            actual = _duckdb_columns(conn, table_name)

        assert actual == NORMALISED_TABLE_COLUMNS[table_name]

    def test_fundamentals_announcement_date_not_null(self):
        """
        SPEC-PIPE-003: announcement_date is the mandatory PIT key for
        fundamentals (never quarter_end_date) — schema enforces NOT NULL
        so a row with no known disclosure date can never be inserted.
        """
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            with pytest.raises(Exception):
                conn.execute(
                    "INSERT INTO fundamentals "
                    "(ticker, fiscal_year, quarter, quarter_end_date, announcement_date) "
                    "VALUES ('RELIANCE', 2025, 1, '2025-03-31', NULL)"
                )

    def test_shareholding_filing_date_not_null(self):
        """SPEC-PIPE-003: filing_date is the mandatory PIT key for shareholding."""
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            with pytest.raises(Exception):
                conn.execute(
                    "INSERT INTO shareholding (ticker, quarter_end_date, filing_date) "
                    "VALUES ('RELIANCE', '2025-03-31', NULL)"
                )


# ===== create_signals.py =====
class TestCreateSignalsSchema:
    """SPEC-DS-007 Store 4 (signals, DuckDB) + transactional pipeline log (SQLite)."""

    def test_duckdb_signal_tables_created(self):
        """SPEC-DS-004, SPEC-DS-007: ml_signals, ml_multibagger, ml_forensic must exist."""
        create_signals.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            existing = {
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchall()
            }

        for table_name in SIGNAL_DUCKDB_TABLE_COLUMNS:
            assert table_name in existing, f"{table_name} was not created"

    @pytest.mark.parametrize("table_name", sorted(SIGNAL_DUCKDB_TABLE_COLUMNS))
    def test_duckdb_table_columns_match_architecture_doc(self, table_name):
        """SPEC-DS-004: each signal table's columns must match the architecture doc."""
        create_signals.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            actual = _duckdb_columns(conn, table_name)

        assert actual == SIGNAL_DUCKDB_TABLE_COLUMNS[table_name]

    def test_pipeline_runs_created_in_sqlite_not_duckdb(self):
        """
        SPEC-SCHED-002, SPEC-DS-007: pipeline_runs is transactional and
        must live in SQLite, not alongside the analytical signal tables
        in DuckDB.
        """
        create_signals.create_schema(in_memory=True)

        with get_sqlite_connection(None) as conn:
            sqlite_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "pipeline_runs" in sqlite_tables

        with get_duckdb_connection(None) as conn:
            duckdb_tables = {
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchall()
            }
        assert "pipeline_runs" not in duckdb_tables

    def test_pipeline_runs_columns(self):
        """SPEC-SCHED-005: pipeline_runs columns must match the documented schema."""
        create_signals.create_schema(in_memory=True)

        with get_sqlite_connection(None) as conn:
            actual = _sqlite_columns(conn, "pipeline_runs")

        assert actual == PIPELINE_RUNS_COLUMNS


# ===== PIT rule: OHLCV as_of filtering =====
class TestOHLCVPointInTime:
    """SPEC-PIPE-003: no query may return data dated after its as_of reference."""

    def test_as_of_excludes_future_rows(self):
        """
        SPEC-PIPE-003: an OHLCV query bounded by as_of must return only
        rows where date <= as_of, even when later rows exist in the table.
        """
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            rows = [
                ("2025-01-02", "RELIANCE", 100.0, 101.0, 99.0, 100.5, 1_000_000, 1.0),
                ("2025-01-03", "RELIANCE", 100.5, 102.0, 100.0, 101.5, 1_100_000, 1.0),
                ("2025-01-06", "RELIANCE", 101.5, 103.0, 101.0, 102.5, 1_200_000, 1.0),
                ("2025-01-07", "RELIANCE", 102.5, 104.0, 102.0, 103.5, 1_300_000, 1.0),
            ]
            conn.executemany(
                "INSERT INTO ohlcv_adjusted "
                "(date, ticker, open, high, low, close, volume, adj_factor) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

            as_of = date(2025, 1, 3)
            result = conn.execute(
                "SELECT date FROM ohlcv_adjusted "
                "WHERE ticker = ? AND date <= ? ORDER BY date",
                ["RELIANCE", as_of],
            ).fetchall()

        result_dates = [r[0] for r in result]

        # Rule: every returned row must satisfy date <= as_of.
        assert all(d <= as_of for d in result_dates)
        # And the two rows after as_of (Jan 6, Jan 7) must be excluded.
        assert result_dates == [date(2025, 1, 2), date(2025, 1, 3)]

    def test_as_of_in_the_past_returns_no_rows(self):
        """SPEC-PIPE-003: as_of before all data must return an empty result, not an error."""
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            conn.execute(
                "INSERT INTO ohlcv_adjusted "
                "(date, ticker, open, high, low, close, volume, adj_factor) "
                "VALUES ('2025-01-02', 'RELIANCE', 100.0, 101.0, 99.0, 100.5, 1000000, 1.0)"
            )

            result = conn.execute(
                "SELECT date FROM ohlcv_adjusted WHERE ticker = ? AND date <= ?",
                ["RELIANCE", date(2024, 12, 1)],
            ).fetchall()

        assert result == []
