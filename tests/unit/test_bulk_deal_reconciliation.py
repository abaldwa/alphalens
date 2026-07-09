"""
tests/unit/test_bulk_deal_reconciliation.py

Phase D verification (Big Investor Activity — plan: gentle-wobbling-swing.md
"Verification" section: unit test the correction/propagation logic against a
synthetic case where a bulk-deal estimate diverges from a reported quarterly
holding), added 2026-07-05.

Uses an isolated in-memory DuckDB — never touches the real project database.
"""

from datetime import date, datetime

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.scrapers.bulk_deal_reconciliation import (
    reconcile_family_ticker_quarter,
    reconcile_quarter,
)


@pytest.fixture
def conn():
    create_normalised.create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM public_shareholders")
        c.execute("DELETE FROM bulk_deal_positions")
        c.execute("DELETE FROM bulk_deal_reconciliation_log")
        c.execute("DELETE FROM stock_master")
        c.execute("DELETE FROM ohlcv_adjusted")


def _insert_position(conn, family_id, ticker, trade_date, deal_type, cumulative_position_est):
    conn.execute(
        """
        INSERT INTO bulk_deal_positions (family_id, ticker, trade_date, deal_type, cumulative_position_est)
        VALUES (?, ?, ?, ?, ?)
        """,
        [family_id, ticker, trade_date.isoformat(), deal_type, cumulative_position_est],
    )


def _insert_holder(conn, ticker, holder_name, quarter_end_date, family_id=None,
                    stake_pct=None, reported_shares=None, filing_date=None):
    conn.execute(
        """
        INSERT INTO public_shareholders (ticker, holder_name, quarter_end_date, filing_date,
                                          family_id, stake_pct, reported_shares, source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'trendlyne', ?)
        """,
        [ticker, holder_name, quarter_end_date.isoformat(), (filing_date or quarter_end_date).isoformat(),
         family_id, stake_pct, reported_shares, datetime.utcnow()],
    )


class TestReconcileFamilyTickerQuarter:
    def test_no_public_shareholder_data_returns_no_data(self, conn):
        result = reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", date(2026, 6, 30))
        assert result["status"] == "no_data"

    def test_within_tolerance_resolves_without_correction(self, conn):
        q = date(2026, 6, 30)
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 6, 1), "bulk", 100_000)
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", reported_shares=105_000)
        result = reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", q)
        assert result["status"] == "resolved"
        row = conn.execute(
            "SELECT cumulative_position_est FROM bulk_deal_positions WHERE ticker='TITAN' AND deal_type='bulk'"
        ).fetchone()
        assert row[0] == 100_000  # untouched

    def test_large_discrepancy_inserts_reconciliation_anchor_and_flags(self, conn):
        q = date(2026, 6, 30)
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 6, 1), "bulk", 100_000)
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", reported_shares=200_000)
        result = reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", q)
        assert result["status"] == "flagged_for_review"
        assert result["reported_shares_est"] == 200_000
        anchor = conn.execute(
            "SELECT cumulative_position_est FROM bulk_deal_positions WHERE ticker='TITAN' AND deal_type='reconciliation' AND trade_date=?",
            [q.isoformat()],
        ).fetchone()
        assert anchor is not None
        assert anchor[0] == 200_000
        log_row = conn.execute(
            "SELECT status, correction_applied, correction_delta FROM bulk_deal_reconciliation_log WHERE ticker='TITAN'"
        ).fetchone()
        assert log_row[0] == "flagged_for_review"
        assert log_row[1] is True
        assert log_row[2] == 100_000

    def test_correction_propagates_forward_to_rows_after_quarter_end(self, conn):
        q = date(2026, 6, 30)
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 6, 1), "bulk", 100_000)
        # A trade after quarter-end already reflects real activity since —
        # the correction should offset it, not overwrite it wholesale.
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 7, 5), "bulk", 110_000)
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", reported_shares=200_000)
        reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", q)
        row = conn.execute(
            "SELECT cumulative_position_est FROM bulk_deal_positions WHERE ticker='TITAN' AND trade_date=? AND deal_type='bulk'",
            [date(2026, 7, 5).isoformat()],
        ).fetchone()
        # delta = 200_000 - 100_000 = +100_000 applied on top of the existing 110_000
        assert row[0] == 210_000

    def test_no_prior_trades_since_last_position_still_gets_anchor(self, conn):
        # The bug this guards against: an earlier version only UPDATEd
        # existing rows, which did nothing when the last real trade
        # predated quarter-end (no row exists AT quarter_end_date itself).
        q = date(2026, 6, 30)
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 1, 15), "bulk", 50_000)
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", reported_shares=90_000)
        result = reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", q)
        assert result["status"] == "flagged_for_review"
        anchor = conn.execute(
            "SELECT cumulative_position_est FROM bulk_deal_positions WHERE ticker='TITAN' AND deal_type='reconciliation'"
        ).fetchone()
        assert anchor[0] == 90_000

    def test_falls_back_to_market_cap_estimate_when_reported_shares_missing(self, conn):
        q = date(2026, 6, 30)
        conn.execute(
            "INSERT INTO stock_master (ticker, company_name, sector, nse_series, market_cap_cr) "
            "VALUES ('TITAN', 'Titan Company', 'Consumer Durables', 'EQ', 30000.0)"
        )
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor) "
            "VALUES (?, 'TITAN', 3000, 3050, 2950, 3000, 100000, 1.0)",
            [date(2026, 6, 28).isoformat()],
        )
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 6, 1), "bulk", 100_000)
        # stake_pct only, no reported_shares — Trendlyne showed "Filing Awaited"
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", stake_pct=1.0)
        result = reconcile_family_ticker_quarter(conn, "jhunjhunwala", "TITAN", q)
        # shares_outstanding = 30000*1e7/3000 = 100,000,000; 1% = 1,000,000
        assert result["reported_shares_est"] == 1_000_000
        assert result["status"] == "flagged_for_review"


class TestReconcileQuarter:
    def test_reconciles_all_family_ticker_pairs_for_quarter(self, conn):
        q = date(2026, 6, 30)
        _insert_position(conn, "jhunjhunwala", "TITAN", date(2026, 6, 1), "bulk", 100_000)
        _insert_position(conn, "damani", "VST", date(2026, 6, 1), "bulk", 5_000)
        _insert_holder(conn, "TITAN", "Rakesh Jhunjhunwala", q, family_id="jhunjhunwala", reported_shares=100_000)
        _insert_holder(conn, "VST", "Radhakishan Damani", q, family_id="damani", reported_shares=5_000)
        results = reconcile_quarter(conn, q)
        assert len(results) == 2
        assert {r["status"] for r in results} == {"resolved"}

    def test_unmatched_holder_names_skipped(self, conn):
        q = date(2026, 6, 30)
        _insert_holder(conn, "TITAN", "Unknown Holder", q, family_id=None, reported_shares=1000)
        results = reconcile_quarter(conn, q)
        assert results == []
