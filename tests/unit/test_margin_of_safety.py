"""
tests/unit/test_margin_of_safety.py

Pure-logic tests for systems/fundamental_analysis/quality/margin_of_safety.py
using an in-memory DuckDB connection (fundamentals_history + ohlcv_adjusted
tables), no real DB writes.
"""

from datetime import datetime

import duckdb
import pytest

from systems.fundamental_analysis.quality.margin_of_safety import (
    MARGIN_OF_SAFETY_THRESHOLD,
    compute_margin_of_safety,
)


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE fundamentals_history (
            ticker VARCHAR, fiscal_year INT, quarter INT, quarter_end_date DATE, announcement_date DATE,
            recorded_at TIMESTAMP, history_id INT,
            eps DOUBLE, book_value_per_share DOUBLE, debt_to_equity DOUBLE, interest_coverage DOUBLE,
            fcf DOUBLE, capex DOUBLE
        )
    """)
    c.execute("CREATE TABLE ohlcv_adjusted (date DATE, ticker VARCHAR, close DOUBLE)")
    return c


def _insert_fundamentals(conn, **kw):
    row = {
        "ticker": "X", "fiscal_year": 2025, "quarter": 4, "quarter_end_date": "2025-12-31",
        "announcement_date": "2026-01-15", "recorded_at": "2026-01-15 00:00:00", "history_id": 1,
        "eps": 10.0, "book_value_per_share": 50.0, "debt_to_equity": 0.3, "interest_coverage": 8.0,
        "fcf": 20.0, "capex": 5.0,
    }
    row.update(kw)
    conn.execute(
        "INSERT INTO fundamentals_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [row["ticker"], row["fiscal_year"], row["quarter"], row["quarter_end_date"], row["announcement_date"],
         row["recorded_at"], row["history_id"], row["eps"], row["book_value_per_share"], row["debt_to_equity"],
         row["interest_coverage"], row["fcf"], row["capex"]],
    )


def _insert_close(conn, close: float, date: str = "2026-02-01"):
    conn.execute("INSERT INTO ohlcv_adjusted VALUES (?, 'X', ?)", [date, close])


class TestComputeMarginOfSafety:
    def test_empty_history_fails_conservatively(self, conn):
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["intrinsic_value"] is None
        assert result["passes"] is False

    def test_deep_discount_and_solvent_passes(self, conn):
        _insert_fundamentals(conn)
        _insert_close(conn, close=40.0)  # well below graham_value=85.0 and graham_number~106
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["margin_of_safety"] >= MARGIN_OF_SAFETY_THRESHOLD
        assert result["passes"] is True

    def test_expensive_price_fails(self, conn):
        _insert_fundamentals(conn)
        _insert_close(conn, close=95.0)  # close to intrinsic value, no margin of safety
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False

    def test_high_leverage_fails_solvency_gate_even_if_cheap(self, conn):
        _insert_fundamentals(conn, debt_to_equity=1.5)  # above SOLVENCY_MAX_DEBT_TO_EQUITY
        _insert_close(conn, close=20.0)
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["margin_of_safety"] >= MARGIN_OF_SAFETY_THRESHOLD
        assert result["passes"] is False

    def test_negative_cfo_fails_solvency_gate(self, conn):
        _insert_fundamentals(conn, fcf=-10.0, capex=5.0)  # cfo_proxy = fcf+capex < 0
        _insert_close(conn, close=20.0)
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False

    def test_missing_close_fails_conservatively(self, conn):
        _insert_fundamentals(conn)
        result = compute_margin_of_safety(conn, "X", datetime(2026, 2, 5))
        assert result["intrinsic_value"] is None
        assert result["passes"] is False
