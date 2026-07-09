"""
tests/unit/test_bulk_deal_attribution.py

Phase B verification (Big Investor Activity — plan: gentle-wobbling-swing.md
"Verification" section: unit tests for wash trades, HUF-suffix variants, and
unmapped clients), added 2026-07-05.

Uses an isolated in-memory DuckDB (create_normalised.create_schema(in_memory=True))
per config/settings.py's no-synthetic-writes-to-the-real-DB discipline — never
touches the real project database.
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.scrapers.bulk_deal_attribution import (
    attribute_bulk_deals,
    normalize_client_name,
)


@pytest.fixture
def conn():
    create_normalised.create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM large_deals")
        c.execute("DELETE FROM investor_family")
        c.execute("DELETE FROM bulk_deal_positions")


def _insert_deal(conn, trade_date, ticker, client_name, transaction_type, quantity, price,
                  exchange="NSE", deal_type="bulk"):
    conn.execute(
        """
        INSERT INTO large_deals (exchange, deal_type, trade_date, ticker, client_name,
                                  transaction_type, quantity, price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [exchange, deal_type, trade_date.isoformat(), ticker, client_name, transaction_type, quantity, price],
    )


class TestNormalizeClientName:
    def test_case_and_whitespace_collapsed(self):
        assert normalize_client_name("  Rakesh   Jhunjhunwala ") == "RAKESH JHUNJHUNWALA"

    def test_huf_suffix_variants_normalize_differently_unless_seeded(self):
        # normalize_client_name itself only upper-cases/collapses whitespace —
        # it does NOT strip "HUF"/"(HUF)" (that's a join-key exactness concern
        # for the investor_family seed to handle via explicit alias rows, not
        # something to silently paper over here).
        assert normalize_client_name("Rakesh Jhunjhunwala HUF") == "RAKESH JHUNJHUNWALA HUF"
        assert normalize_client_name("Rakesh Jhunjhunwala") == "RAKESH JHUNJHUNWALA"

    def test_none_returns_empty_string(self):
        assert normalize_client_name(None) == ""


class TestAttributeBulkDeals:
    def test_no_deals_for_date_returns_zero(self, conn):
        assert attribute_bulk_deals(conn, date(2026, 7, 1)) == 0

    def test_unmapped_client_gets_unmapped_prefix(self, conn):
        d = date(2026, 7, 1)
        _insert_deal(conn, d, "RELIANCE", "Some Random Trader", "B", 1000, 2500.0)
        written = attribute_bulk_deals(conn, d)
        assert written == 1
        row = conn.execute(
            "SELECT family_id, net_transaction_type, net_quantity, is_new_entry FROM bulk_deal_positions WHERE ticker = 'RELIANCE'"
        ).fetchone()
        assert row[0] == "unmapped:SOME RANDOM TRADER"
        assert row[1] == "BUY"
        assert row[2] == 1000
        assert row[3] is True

    def test_full_wash_trade_nets_to_zero_and_is_dropped(self, conn):
        d = date(2026, 7, 1)
        _insert_deal(conn, d, "TATASTEEL", "Wash Trader", "B", 500, 100.0)
        _insert_deal(conn, d, "TATASTEEL", "Wash Trader", "S", 500, 100.0)
        written = attribute_bulk_deals(conn, d)
        assert written == 0
        rows = conn.execute("SELECT * FROM bulk_deal_positions WHERE ticker = 'TATASTEEL'").fetchall()
        assert rows == []

    def test_partial_wash_nets_to_residual(self, conn):
        d = date(2026, 7, 1)
        _insert_deal(conn, d, "INFY", "Partial Trader", "B", 1000, 1500.0)
        _insert_deal(conn, d, "INFY", "Partial Trader", "S", 400, 1500.0)
        written = attribute_bulk_deals(conn, d)
        assert written == 1
        row = conn.execute(
            "SELECT net_transaction_type, net_quantity, cumulative_position_est FROM bulk_deal_positions WHERE ticker = 'INFY'"
        ).fetchone()
        assert row[0] == "BUY"
        assert row[1] == 600
        assert row[2] == 600

    def test_seeded_family_maps_to_family_id(self, conn):
        conn.execute(
            "INSERT INTO investor_family (entity_name, family_id, family_display_name, match_type, source, confidence, added_date) "
            "VALUES ('RAKESH JHUNJHUNWALA', 'jhunjhunwala', 'Rakesh Jhunjhunwala', 'exact', 'seed_yaml', 1.0, '2026-01-01')"
        )
        d = date(2026, 7, 1)
        _insert_deal(conn, d, "TITAN", "Rakesh Jhunjhunwala", "B", 2000, 3000.0)
        attribute_bulk_deals(conn, d)
        row = conn.execute("SELECT family_id FROM bulk_deal_positions WHERE ticker = 'TITAN'").fetchone()
        assert row[0] == "jhunjhunwala"

    def test_cumulative_position_carries_forward_across_dates(self, conn):
        d1, d2 = date(2026, 7, 1), date(2026, 7, 2)
        _insert_deal(conn, d1, "WIPRO", "Trader X", "B", 1000, 400.0)
        attribute_bulk_deals(conn, d1)
        _insert_deal(conn, d2, "WIPRO", "Trader X", "B", 500, 410.0)
        attribute_bulk_deals(conn, d2)
        row = conn.execute(
            "SELECT cumulative_position_est FROM bulk_deal_positions WHERE ticker = 'WIPRO' AND trade_date = ?",
            [d2.isoformat()],
        ).fetchone()
        assert row[0] == 1500

    def test_full_exit_flagged(self, conn):
        d1, d2 = date(2026, 7, 1), date(2026, 7, 2)
        _insert_deal(conn, d1, "IDEA", "Exit Trader", "B", 1000, 10.0)
        attribute_bulk_deals(conn, d1)
        _insert_deal(conn, d2, "IDEA", "Exit Trader", "S", 1000, 10.0)
        attribute_bulk_deals(conn, d2)
        row = conn.execute(
            "SELECT is_full_exit, cumulative_position_est FROM bulk_deal_positions WHERE ticker = 'IDEA' AND trade_date = ?",
            [d2.isoformat()],
        ).fetchone()
        assert row[0] is True
        assert row[1] == 0

    def test_rerun_for_same_date_is_idempotent(self, conn):
        d = date(2026, 7, 1)
        _insert_deal(conn, d, "HDFCBANK", "Trader Y", "B", 300, 1600.0)
        attribute_bulk_deals(conn, d)
        attribute_bulk_deals(conn, d)
        rows = conn.execute("SELECT * FROM bulk_deal_positions WHERE ticker = 'HDFCBANK'").fetchall()
        assert len(rows) == 1
