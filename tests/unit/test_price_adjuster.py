"""
tests/unit/test_price_adjuster.py

Phase: 0.4 (Data Ingestion Scrapers) / 3.5 (dividend + volume + audit-table)
Specs: SPEC-PIPE-002, SPEC-SCHED-010
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/adjust/price_adjuster.py, against an in-memory
DuckDB instance created via datastore/schema/create_normalised.py.

Column index reference for _get_rows_full (LEFT JOIN with ohlcv_ca_audit):
  0  date
  1  open (adjusted)       9  raw_open  (audit; NULL if no CA)
  2  high (adjusted)      10  raw_high
  3  low  (adjusted)      11  raw_low
  4  close (adjusted)     12  raw_close
  5  volume (adjusted)    13  raw_volume
  6  delivery_qty (adj)   14  raw_delivery_qty
  7  adj_factor
  8  vol_adj_factor
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


def _insert_ohlcv(conn, ticker, rows, volume=1_000_000, delivery_qty=None):
    dq = delivery_qty if delivery_qty is not None else volume
    conn.executemany(
        "INSERT INTO ohlcv_adjusted "
        "(date, ticker, open, high, low, close, volume, delivery_qty, adj_factor) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(d, ticker, o, h, low, c, volume, dq, 1.0) for d, o, h, low, c in rows],
    )


def _insert_action(conn, ticker, ex_date, action_type, ratio):
    conn.execute(
        "INSERT INTO corporate_actions "
        "(ticker, ex_date, action_type, ratio, announcement_date, record_date) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [ticker, ex_date, action_type, ratio, "2025-12-20", "2026-01-04"],
    )


def _get_rows(conn, ticker):
    """Adjusted prices + factors only."""
    return conn.execute(
        "SELECT date, open, high, low, close, adj_factor FROM ohlcv_adjusted "
        "WHERE ticker = ? ORDER BY date",
        [ticker],
    ).fetchall()


def _get_rows_full(conn, ticker):
    """
    LEFT JOIN ohlcv_ca_audit so test assertions can check both adjusted values
    and the original NSE prices captured in the audit table.
    raw_* columns are NULL for rows that were not touched by the adjuster.
    """
    return conn.execute(
        """
        SELECT o.date,
               o.open, o.high, o.low, o.close, o.volume, o.delivery_qty,
               o.adj_factor, o.vol_adj_factor,
               a.raw_open, a.raw_high, a.raw_low, a.raw_close,
               a.raw_volume, a.raw_delivery_qty
        FROM ohlcv_adjusted o
        LEFT JOIN ohlcv_ca_audit a ON a.date = o.date AND a.ticker = o.ticker
        WHERE o.ticker = ?
        ORDER BY o.date
        """,
        [ticker],
    ).fetchall()


# ---------------------------------------------------------------------------
# SPLIT tests
# ---------------------------------------------------------------------------

def test_split_adjustment_is_idempotent(conn):
    """SPEC-PIPE-002: calling adjust_for_corporate_actions twice gives the same result."""
    ticker = "SPLITCO"
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),
            ("2026-01-05", 99, 101, 97, 100),
            ("2026-01-06", 100, 102, 98, 101),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows_first = _get_rows(conn, ticker)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows_second = _get_rows(conn, ticker)

    assert rows_first == rows_second

    by_date = {r[0]: r for r in rows_first}
    assert by_date[date(2026, 1, 1)][4] == pytest.approx(100.0)  # close 200 → 100
    assert by_date[date(2026, 1, 1)][5] == pytest.approx(0.5)    # adj_factor
    assert by_date[date(2026, 1, 2)][4] == pytest.approx(101.0)  # close 202 → 101
    assert by_date[date(2026, 1, 5)][4] == pytest.approx(100.0)  # on ex_date: untouched
    assert by_date[date(2026, 1, 5)][5] == pytest.approx(1.0)
    assert by_date[date(2026, 1, 6)][4] == pytest.approx(101.0)  # after ex_date: untouched


def test_split_volume_adjustment(conn):
    """SPEC-PIPE-002: SPLIT must scale pre-ex volume by ratio (opposite to price direction)."""
    ticker = "SPLITVOL"
    raw_vol = 1_000_000
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-05", 99,  101,  97, 100),   # ex_date
        ],
        volume=raw_vol,
    )
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 5.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows_full(conn, ticker)}

    # pre-ex row: adjusted volume = raw × vol_factor
    pre = rows[date(2026, 1, 1)]
    assert pre[5] == 5_000_000              # adjusted volume = 1M × 5
    assert pre[8] == pytest.approx(5.0)    # vol_adj_factor
    assert pre[13] == raw_vol              # raw_volume in audit table unchanged

    # ex_date: not affected
    on = rows[date(2026, 1, 5)]
    assert on[5] == raw_vol
    assert on[8] == pytest.approx(1.0)
    assert on[13] is None  # no audit entry for on-or-after ex_date row


def test_split_raw_values_in_audit_table(conn):
    """SPEC-PIPE-002: audit table must hold original NSE prices for adjusted rows."""
    ticker = "RAWCO"
    _insert_ohlcv(conn, ticker, [("2026-01-01", 196, 204, 194, 200)])
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows_full(conn, ticker)}
    pre = rows[date(2026, 1, 1)]

    # adjusted values halved
    assert pre[1] == pytest.approx(98.0)   # open
    assert pre[4] == pytest.approx(100.0)  # close
    # original NSE values in audit table (indices 9–12)
    assert pre[9]  == pytest.approx(196.0)  # raw_open
    assert pre[12] == pytest.approx(200.0)  # raw_close


def test_audit_raw_values_immutable_on_rerun(conn):
    """raw_* in the audit table must not change when the adjuster re-runs."""
    ticker = "IMMUTABLE"
    _insert_ohlcv(conn, ticker, [("2026-01-01", 196, 204, 194, 200)])
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    raw_close_first = conn.execute(
        "SELECT raw_close FROM ohlcv_ca_audit WHERE ticker=? AND date='2026-01-01'",
        [ticker],
    ).fetchone()[0]

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    raw_close_second = conn.execute(
        "SELECT raw_close FROM ohlcv_ca_audit WHERE ticker=? AND date='2026-01-01'",
        [ticker],
    ).fetchone()[0]

    assert raw_close_first == raw_close_second == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# BONUS tests
# ---------------------------------------------------------------------------

def test_bonus_adjustment_multiplies_by_one_over_one_plus_ratio(conn):
    """SPEC-PIPE-002: BONUS adjustment must multiply pre-ex prices by 1/(1+ratio)."""
    ticker = "BONUSCO"
    ratio = 0.5   # 1:2 bonus → 0.5 extra share per share held
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 294, 306, 291, 300),
            ("2026-01-05", 199, 201, 197, 200),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "BONUS", ratio)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)

    by_date = {r[0]: r for r in _get_rows(conn, ticker)}
    expected = 1.0 / (1.0 + ratio)
    assert by_date[date(2026, 1, 1)][5] == pytest.approx(expected)
    assert by_date[date(2026, 1, 1)][4] == pytest.approx(300.0 * expected)
    assert by_date[date(2026, 1, 5)][5] == pytest.approx(1.0)  # on ex_date: untouched


def test_bonus_volume_adjustment(conn):
    """SPEC-PIPE-002: BONUS must scale pre-ex volume by (1+ratio)."""
    ticker = "BONUSVOL"
    raw_vol = 2_000_000
    _insert_ohlcv(
        conn, ticker,
        [("2026-01-01", 294, 306, 291, 300), ("2026-01-05", 199, 201, 197, 200)],
        volume=raw_vol,
    )
    _insert_action(conn, ticker, "2026-01-05", "BONUS", 1.0)  # 1:1 bonus

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows_full(conn, ticker)}

    pre = rows[date(2026, 1, 1)]
    assert pre[5] == 4_000_000              # 2M × 2
    assert pre[8] == pytest.approx(2.0)    # vol_adj_factor
    assert pre[13] == raw_vol              # raw_volume in audit

    assert rows[date(2026, 1, 5)][8] == pytest.approx(1.0)  # no adjustment on ex_date


# ---------------------------------------------------------------------------
# DIVIDEND tests
# ---------------------------------------------------------------------------

def test_dividend_price_adjustment(conn):
    """SPEC-PIPE-002: DIVIDEND adjusts pre-ex price by 1 - (dividend / raw_close_before)."""
    ticker = "DIVCO"
    raw_close_before = 500.0
    dividend = 10.0
    expected_factor = 1.0 - (dividend / raw_close_before)  # 0.98

    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 495, 510, 490, raw_close_before),
            ("2026-01-05", 490, 495, 485, 490),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "DIVIDEND", dividend)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows(conn, ticker)}

    pre = rows[date(2026, 1, 1)]
    assert pre[5] == pytest.approx(expected_factor, rel=1e-6)
    assert pre[4] == pytest.approx(raw_close_before * expected_factor)
    assert rows[date(2026, 1, 5)][5] == pytest.approx(1.0)  # ex-date untouched


def test_dividend_does_not_adjust_volume(conn):
    """SPEC-PIPE-002: dividends don't change share count; vol_adj_factor stays 1.0."""
    ticker = "DIVVOLCO"
    raw_vol = 500_000
    _insert_ohlcv(
        conn, ticker,
        [("2026-01-01", 495, 510, 490, 500.0), ("2026-01-05", 490, 495, 485, 490.0)],
        volume=raw_vol,
    )
    _insert_action(conn, ticker, "2026-01-05", "DIVIDEND", 10.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows_full(conn, ticker)}

    pre = rows[date(2026, 1, 1)]
    assert pre[5] == raw_vol              # volume unchanged
    assert pre[8] == pytest.approx(1.0)  # vol_adj_factor = 1.0


def test_dividend_uses_raw_close_not_adjusted_close(conn):
    """
    SPEC-PIPE-002: when a split precedes a dividend, the dividend factor must
    use the original NSE close (from audit table or close/adj_factor), not the
    split-adjusted close.
    """
    ticker = "SPLITTHENDIV"

    # SPLIT ex_date 2026-01-10 (ratio=2): price halves from ~200 to ~100
    # DIVIDEND ex_date 2026-01-20 (Rs.5/share declared post-split)
    # Dividend factor uses raw_close on 2026-01-19 = 100 (NSE-reported post-split)
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),   # raw_close = 200
            ("2026-01-10", 99,  101,  97, 100),   # split ex_date
            ("2026-01-19", 99,  102,  97, 100),   # raw_close = 100 (post-split NSE)
            ("2026-01-20", 94,   98,  92,  95),   # dividend ex_date
        ],
    )
    _insert_action(conn, ticker, "2026-01-10", "SPLIT", 2.0)
    _insert_action(conn, ticker, "2026-01-20", "DIVIDEND", 5.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    rows = {r[0]: r for r in _get_rows_full(conn, ticker)}

    # dividend factor = 1 - 5/100 = 0.95
    div_factor = 1.0 - (5.0 / 100.0)

    # pre-split row: SPLIT (0.5) × DIVIDEND (0.95)
    pre_split = rows[date(2026, 1, 1)]
    combined = 0.5 * div_factor
    assert pre_split[7] == pytest.approx(combined, rel=1e-4)          # adj_factor
    assert pre_split[4] == pytest.approx(200.0 * combined, rel=1e-4)  # close

    # post-split, pre-dividend: only DIVIDEND
    pre_div = rows[date(2026, 1, 19)]
    assert pre_div[7] == pytest.approx(div_factor, rel=1e-4)
    assert pre_div[4] == pytest.approx(100.0 * div_factor, rel=1e-4)

    # audit table preserved original NSE prices
    assert rows[date(2026, 1, 1)][12]  == pytest.approx(200.0)   # raw_close
    assert rows[date(2026, 1, 19)][12] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Continuity check tests
# ---------------------------------------------------------------------------

def test_continuity_check_passes_for_valid_adjustment(conn):
    """SPEC-PIPE-002: post-adjustment, the price gap at ex_date must be < 1%."""
    ticker = "CONTINUITYCO"
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),
            ("2026-01-05", 99, 101, 97, 100),
            ("2026-01-06", 100, 102, 98, 101),
        ],
    )
    _insert_action(conn, ticker, "2026-01-05", "SPLIT", 2.0)

    price_adjuster.adjust_for_corporate_actions(conn, ticker)
    assert price_adjuster.check_price_continuity(conn, ticker, ["2026-01-05"]) is True


def test_continuity_check_fails_for_unadjusted_split(conn):
    """A split with no adjustment applied leaves a large, detectable gap."""
    ticker = "UNADJUSTEDCO"
    _insert_ohlcv(
        conn, ticker,
        [
            ("2026-01-01", 196, 204, 194, 200),
            ("2026-01-02", 198, 206, 196, 202),
            ("2026-01-05", 99, 101, 97, 100),
        ],
    )
    # No adjust_for_corporate_actions() call.
    assert price_adjuster.check_price_continuity(conn, ticker, ["2026-01-05"]) is False


# ---------------------------------------------------------------------------
# No-corporate-action stocks
# ---------------------------------------------------------------------------

def test_no_actions_no_audit_entry(conn):
    """
    SPEC-PIPE-002: stocks with no CAs must have adj_factor=1.0, vol_adj_factor=1.0,
    and zero rows in ohlcv_ca_audit (nothing was modified, nothing to audit).
    """
    ticker = "NOACTIONCO"
    _insert_ohlcv(conn, ticker, [("2026-01-01", 100, 110, 95, 105)], volume=500_000)
    price_adjuster.adjust_for_corporate_actions(conn, ticker)

    rows = _get_rows_full(conn, ticker)
    assert len(rows) == 1
    row = rows[0]

    assert row[7] == pytest.approx(1.0)   # adj_factor
    assert row[8] == pytest.approx(1.0)   # vol_adj_factor
    # No audit entry → raw_* columns are NULL in the LEFT JOIN
    assert row[9]  is None   # raw_open
    assert row[12] is None   # raw_close

    audit_count = conn.execute(
        "SELECT COUNT(*) FROM ohlcv_ca_audit WHERE ticker=?", [ticker]
    ).fetchone()[0]
    assert audit_count == 0
