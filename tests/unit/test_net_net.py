"""
tests/unit/test_net_net.py

Pure-logic tests for systems/fundamental_analysis/quality/net_net.py using
an in-memory DuckDB connection, no real DB writes.
"""

from datetime import datetime

import duckdb
import pytest

from systems.fundamental_analysis.quality.net_net import (
    LIQUIDITY_FLOOR_MARKET_CAP_CR,
    compute_net_net,
)


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE fundamentals_history (
            ticker VARCHAR, fiscal_year INT, quarter INT, quarter_end_date DATE, announcement_date DATE,
            recorded_at TIMESTAMP, history_id INT,
            current_assets DOUBLE, total_liabilities DOUBLE, shares_outstanding BIGINT,
            current_liabilities DOUBLE, non_current_liabilities DOUBLE
        )
    """)
    c.execute("CREATE TABLE ohlcv_adjusted (date DATE, ticker VARCHAR, close DOUBLE)")
    return c


def _insert_fundamentals(conn, **kw):
    row = {
        "ticker": "X", "fiscal_year": 2025, "quarter": 4, "quarter_end_date": "2025-12-31",
        "announcement_date": "2026-01-15", "recorded_at": "2026-01-15 00:00:00", "history_id": 1,
        "current_assets": 500.0, "total_liabilities": 200.0, "shares_outstanding": 1_000_000,
        "current_liabilities": None, "non_current_liabilities": None,
    }
    row.update(kw)
    conn.execute(
        "INSERT INTO fundamentals_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [row["ticker"], row["fiscal_year"], row["quarter"], row["quarter_end_date"], row["announcement_date"],
         row["recorded_at"], row["history_id"], row["current_assets"], row["total_liabilities"], row["shares_outstanding"],
         row["current_liabilities"], row["non_current_liabilities"]],
    )


def _insert_close(conn, close: float, date: str = "2026-02-01"):
    conn.execute("INSERT INTO ohlcv_adjusted VALUES (?, 'X', ?)", [date, close])


class TestComputeNetNet:
    def test_empty_history_fails_conservatively(self, conn):
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False

    def test_ncav_per_share_unit_conversion(self, conn):
        # current_assets/total_liabilities are rupee CRORE; shares_outstanding
        # is a raw share count -> ncav_per_share must be in raw rupees.
        _insert_fundamentals(conn)  # NCAV = 300 crore
        _insert_close(conn, close=40.0)
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["ncav_per_share"] == pytest.approx((300.0 * 1e7) / 1_000_000)

    def test_deep_discount_and_liquid_passes(self, conn):
        _insert_fundamentals(conn, shares_outstanding=20_000_000)  # ncav_per_share = 150
        _insert_close(conn, close=40.0)  # 40 <= 0.67*150=100.5; market_cap_cr = 40*20e6/1e7 = 80
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["market_cap_cr"] > LIQUIDITY_FLOOR_MARKET_CAP_CR
        assert result["passes"] is True

    def test_price_above_discount_threshold_fails(self, conn):
        _insert_fundamentals(conn, shares_outstanding=1_000_000)  # ncav_per_share = 3000
        _insert_close(conn, close=2500.0)  # above 0.67*3000=2010
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False

    def test_below_liquidity_floor_fails(self, conn):
        _insert_fundamentals(conn, shares_outstanding=10_000)  # tiny share count -> tiny market cap
        _insert_close(conn, close=1.0)
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["market_cap_cr"] < LIQUIDITY_FLOOR_MARKET_CAP_CR
        assert result["passes"] is False

    def test_negative_ncav_fails(self, conn):
        _insert_fundamentals(conn, current_assets=100.0, total_liabilities=300.0)  # NCAV < 0
        _insert_close(conn, close=1.0)
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False

    def test_missing_total_liabilities_falls_back_to_current_plus_non_current(self, conn):
        """[BUG FIX, 6th fundamental-strategies review, item 4] direct
        total_liabilities is NULL for ~89% of real rows and effectively
        absent before 2023 - deriving it from current_liabilities +
        non_current_liabilities (which agree with the direct column in
        ~98% of rows where both exist) restores the screen's ability to
        actually fire on real historical data."""
        _insert_fundamentals(
            conn, current_assets=500.0, total_liabilities=None,
            current_liabilities=120.0, non_current_liabilities=80.0,  # sums to 200, same as the direct-column tests above
            shares_outstanding=20_000_000,
        )
        _insert_close(conn, close=40.0)
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["ncav"] == pytest.approx(300.0)  # 500 - (120 + 80)
        assert result["passes"] is True

    def test_missing_total_liabilities_and_components_still_fails_conservatively(self, conn):
        _insert_fundamentals(
            conn, current_assets=500.0, total_liabilities=None,
            current_liabilities=None, non_current_liabilities=None,
        )
        _insert_close(conn, close=40.0)
        result = compute_net_net(conn, "X", datetime(2026, 2, 5))
        assert result["passes"] is False
        assert result["ncav_per_share"] != result["ncav_per_share"]  # NaN
