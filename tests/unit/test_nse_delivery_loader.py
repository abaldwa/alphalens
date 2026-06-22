"""
tests/unit/test_nse_delivery_loader.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-005
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/scrapers/nse_delivery_loader.py's UPDATE-only merge
behavior. NSE archive fetches are always mocked — these tests never make
real network calls.
"""

from datetime import date

import pandas as pd

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.scrapers.nse_delivery_loader import merge_delivery_into_ohlcv


def test_merge_only_updates_rows_that_already_exist():
    """SPEC-PIPE-001: merge_delivery_into_ohlcv must never insert new rows,
    and its return value must reflect rows actually matched in
    ohlcv_adjusted — not the row count of the input delivery_df. An earlier
    implementation returned len(delivery_df) regardless of how many tickers
    actually existed in ohlcv_adjusted, overstating a 5-year backfill's
    progress by >10x (reporting the full NSE EQ-series count per day
    instead of the much smaller FYERS-backfilled universe)."""
    create_normalised.create_schema(in_memory=True)

    with get_duckdb_connection(None) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["2024-01-01", "AAA", 100.0, 101.0, 99.0, 100.5, 1000, 1.0],
        )

        delivery_df = pd.DataFrame(
            {
                "ticker": ["AAA", "NOT_IN_OHLCV"],
                "delivery_qty": [500, 200],
                "delivery_pct": [50.0, 20.0],
            }
        )

        updated = merge_delivery_into_ohlcv(conn, date(2024, 1, 1), delivery_df)

        assert updated == 1

        row_count = conn.execute("SELECT COUNT(*) FROM ohlcv_adjusted").fetchone()[0]
        assert row_count == 1  # no new row inserted for NOT_IN_OHLCV

        aaa = conn.execute(
            "SELECT delivery_qty, delivery_pct FROM ohlcv_adjusted WHERE ticker = 'AAA'"
        ).fetchone()
        assert aaa == (500, 50.0)


def test_merge_with_empty_dataframe_returns_zero():
    """An empty delivery_df (e.g. a date with no EQ-series rows) must be a no-op."""
    create_normalised.create_schema(in_memory=True)

    with get_duckdb_connection(None) as conn:
        updated = merge_delivery_into_ohlcv(conn, date(2024, 1, 1), pd.DataFrame())
        assert updated == 0


def test_merge_does_not_cross_contaminate_other_dates():
    """A ticker present on one date must not have its delivery values
    overwritten when merging a different date's delivery_df."""
    create_normalised.create_schema(in_memory=True)

    with get_duckdb_connection(None) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "2024-01-01", "AAA", 100.0, 101.0, 99.0, 100.5, 1000, 1.0,
                "2024-01-02", "AAA", 100.0, 101.0, 99.0, 100.5, 1000, 1.0,
            ],
        )

        delivery_df = pd.DataFrame({"ticker": ["AAA"], "delivery_qty": [500], "delivery_pct": [50.0]})
        merge_delivery_into_ohlcv(conn, date(2024, 1, 1), delivery_df)

        jan2 = conn.execute(
            "SELECT delivery_qty, delivery_pct FROM ohlcv_adjusted WHERE ticker = 'AAA' AND date = '2024-01-02'"
        ).fetchone()
        assert jan2 == (None, None)
