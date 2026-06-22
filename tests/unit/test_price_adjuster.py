"""
tests/unit/test_price_adjuster.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-002, SPEC-SCHED-010
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/adjust/price_adjuster.py, against an in-memory
DuckDB instance created via datastore/schema/create_normalised.py.
"""

from datetime import date

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.adjust import price_adjuster


@pytest.fixture
def conn():
    create_normalised.create_schema(in_memory=True)
    with get_duckdb_connection(None) as connection:
        yield connection


def _insert_ohlcv(conn, ticker, rows):
    conn.executemany(
        "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(date, ticker, o, h, l, c, 1_000_000, 1.0) for date, o, h, l, c in rows],
    )


def _insert_action(conn, ticker, ex_date, action_type, ratio):
    conn.execute(
        "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio, announcement_date, record_date) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [ticker, ex_date, action_type, ratio, "2025-12-20", "2026-01-04"],
    )


def _get_rows(conn, ticker):
    return conn.execute(
        "SELECT date, open, high, low, close, adj_factor FROM ohlcv_adjusted "
        "WHERE ticker = ? ORDER BY date",
        [ticker],
    ).fetchall()


def test_split_adjustment_is_idempotent(conn):
    """SPEC-PIPE-002: calling adjust_for_corporate_actions twice must give the same result."""
    ticker = "SPLITCO"
    _insert_ohlcv(
        conn,
        ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),
            ("2026-01-05", 99, 101, 97, 100),
            ("2026-01-06", 100, 102, 98, 101),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows_after_first = _get_rows(conn, ticker)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows_after_second = _get_rows(conn, ticker)

    assert rows_after_first == rows_after_second

    # SPLIT, ratio=2 -> pre-ex prices x 1/2; ex-date and later untouched.
    # DuckDB returns DATE columns as datetime.date objects, not strings.
    by_date = {r[0]: r for r in rows_after_first}
    assert by_date[date(2026, 1, 1)][4] == pytest.approx(100.0)  # close 200 -> 100
    assert by_date[date(2026, 1, 1)][5] == pytest.approx(0.5)  # adj_factor
    assert by_date[date(2026, 1, 2)][4] == pytest.approx(101.0)  # close 202 -> 101
    assert by_date[date(2026, 1, 5)][4] == pytest.approx(100.0)  # unaffected (on ex_date)
    assert by_date[date(2026, 1, 5)][5] == pytest.approx(1.0)
    assert by_date[date(2026, 1, 6)][4] == pytest.approx(101.0)  # unaffected


def test_bonus_adjustment_multiplies_by_one_over_one_plus_ratio(conn):
    """SPEC-PIPE-002: BONUS adjustment must multiply pre-ex prices by 1/(1+ratio)."""
    ticker = "BONUSCO"
    ratio = 0.5  # 1:2 bonus -> 0.5 extra share per share held
    _insert_ohlcv(
        conn,
        ticker,
        [
            ("2026-01-01", 294, 306, 291, 300),
            ("2026-01-05", 199, 201, 197, 200),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "BONUS", ratio)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)

    by_date = {r[0]: r for r in _get_rows(conn, ticker)}
    expected_factor = 1.0 / (1.0 + ratio)
    assert by_date[date(2026, 1, 1)][5] == pytest.approx(expected_factor)
    assert by_date[date(2026, 1, 1)][4] == pytest.approx(300.0 * expected_factor)
    assert by_date[date(2026, 1, 5)][5] == pytest.approx(1.0)  # on ex_date: untouched


def test_continuity_check_passes_for_valid_adjustment(conn):
    """SPEC-PIPE-002: post-adjustment, the price gap at ex_date must be < 1%."""
    ticker = "CONTINUITYCO"
    _insert_ohlcv(
        conn,
        ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),
            ("2026-01-05", 99, 101, 97, 100),
            ("2026-01-06", 100, 102, 98, 101),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)

    ok = price_adjuster.check_price_continuity(conn, ticker, ["2026-01-05"])
    assert ok is True


def test_continuity_check_fails_for_unadjusted_split(conn):
    """A split with NO adjustment applied must leave a large, detectable gap at ex_date."""
    ticker = "UNADJUSTEDCO"
    _insert_ohlcv(
        conn,
        ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),  # never adjusted: still 202
            ("2026-01-05", 99, 101, 97, 100),
        ],
    )

    # No adjust_for_corporate_actions() call -- prices remain raw/unadjusted.
    ok = price_adjuster.check_price_continuity(conn, ticker, ["2026-01-05"])
    assert ok is False
