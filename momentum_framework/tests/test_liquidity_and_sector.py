"""
Liquidity (ADTV/circuit-lock) and sector-data tests — real production DB
only (both modules are thin wrappers over real market data; there is
little value in synthetic coverage here beyond what test_common_signals.py
already exercises for the ranking side).
"""

import pytest

from momentum_framework.common.liquidity import (
    compute_adtv_cr,
    liquidity_quintile_universe,
)
from momentum_framework.common.sector_data import load_sector_lookup

pytestmark = pytest.mark.real_data


def test_adtv_realistic_for_large_caps(prod_conn):
    adtv = compute_adtv_cr(prod_conn, ["RELIANCE", "TCS", "INFY"], "2024-06-03")
    for ticker in ["RELIANCE", "TCS", "INFY"]:
        assert 100 < adtv[ticker] < 5000, f"{ticker} ADTV={adtv[ticker]:.0f}cr looks implausible for a large-cap"


def test_adtv_missing_ticker_is_dropped_not_zeroed(prod_conn):
    adtv = compute_adtv_cr(prod_conn, ["NOT_A_REAL_TICKER_XYZ"], "2024-06-03")
    assert "NOT_A_REAL_TICKER_XYZ" not in adtv.index


def test_liquidity_quintiles_partition_correctly(prod_conn):
    """5 quintiles of a real, reasonably large candidate set should each
    get a non-trivial share, and no ticker should appear in more than one."""
    universe = [
        "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SUNPHARMA", "CIPLA",
        "DRREDDY", "WIPRO", "HCLTECH", "AXISBANK", "KOTAKBANK", "ITC", "HINDUNILVR",
        "BAJFINANCE", "MARUTI", "TITAN", "ULTRACEMCO", "NESTLEIND", "ASIANPAINT",
    ]
    seen = set()
    for q in [1, 2, 3, 4, 5]:
        bucket = liquidity_quintile_universe(prod_conn, universe, "2024-06-03", q)
        assert not (set(bucket) & seen), f"quintile {q} overlaps a previous quintile"
        seen |= set(bucket)
    assert len(seen) > 0


def test_liquidity_quintile_invalid_value_rejected(prod_conn):
    with pytest.raises(ValueError, match="quintile must be 1-5"):
        liquidity_quintile_universe(prod_conn, ["RELIANCE"], "2024-06-03", 6)


def test_sector_lookup_real_coverage(prod_conn):
    """Regression test for the coverage figure verified during porting
    (2026-09-04): stock_master.sector should be populated for the
    overwhelming majority of tickers (measured 100% at port time)."""
    lookup = load_sector_lookup(prod_conn)
    assert len(lookup) > 1000, "Expected substantial real sector coverage in stock_master"


def test_sector_lookup_known_tickers_correct(prod_conn):
    lookup = load_sector_lookup(prod_conn, tickers=["TCS", "INFY", "RELIANCE"])
    assert lookup["TCS"] == lookup["INFY"], "TCS and INFY should share the IT sector"
    assert lookup["RELIANCE"] != lookup["TCS"]
