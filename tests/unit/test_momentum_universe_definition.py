"""tests/unit/test_momentum_universe_definition.py

The momentum universe, decided 2026-08-18: top 800 by ADTV FIRST, then
market-cap rank within that set, refreshed on a 21-trading-day grid.

Real schema, isolated in-memory DuckDB — never the production database, and
never a fabricated price used as evidence about a real stock (these rows are
a fixture for the RULE, not market data).
"""

from datetime import date

import duckdb
import pandas as pd
import pytest

from features import momentum_universe as mu
from features.momentum_universe import (
    ADTV_UNIVERSE_TOP_N,
    UNIVERSE_REFRESH_TRADING_DAYS,
    liquid_universe,
    momentum_band_universe,
    universe_refresh_dates,
    universe_snapshot_date,
)

AS_OF = "2026-06-30"


@pytest.fixture
def conn(monkeypatch):
    """Six tickers whose ADTV order is deliberately the REVERSE of their
    market-cap order, so the two steps cannot be confused for one another."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE ohlcv_adjusted (ticker VARCHAR, date DATE, close DOUBLE, volume BIGINT)")
    c.execute("CREATE TABLE fundamentals (ticker VARCHAR, announcement_date VARCHAR, shares_outstanding DOUBLE)")

    tickers = [f"T{i}" for i in range(6)]
    days = pd.bdate_range("2026-05-01", "2026-06-30")
    for i, ticker in enumerate(tickers):
        # T0 is the most liquid, T5 the least; shares_outstanding runs the
        # other way, so market-cap rank is the reverse of ADTV rank.
        volume = 10_000 * (6 - i)
        shares = 1_000_000 * (i + 1)
        c.execute(
            "INSERT INTO fundamentals VALUES (?, ?, ?)", [ticker, "2025-01-01", float(shares)]
        )
        for day in days:
            c.execute(
                "INSERT INTO ohlcv_adjusted VALUES (?, ?, ?, ?)",
                [ticker, day.date(), 100.0, volume],
            )

    monkeypatch.setattr(mu, "_all_candidate_tickers", lambda **kw: tickers)
    yield c
    c.close()


class TestTheGrid:
    def test_refresh_is_every_21_trading_days_from_the_start(self):
        days = pd.DatetimeIndex(pd.bdate_range("2026-01-01", "2026-06-30"))
        grid = universe_refresh_dates(days)
        assert grid[0] == days[0]
        assert all(
            days.get_loc(b) - days.get_loc(a) == UNIVERSE_REFRESH_TRADING_DAYS
            for a, b in zip(grid, grid[1:])
        )

    def test_a_strategy_uses_the_most_recent_snapshot_at_or_before_its_rebalance(self):
        days = pd.DatetimeIndex(pd.bdate_range("2026-01-01", "2026-06-30"))
        grid = universe_refresh_dates(days)
        # A weekly strategy rebalancing between grid points uses the earlier one.
        between = days[days.get_loc(grid[1]) + 3]
        assert universe_snapshot_date(days, between) == grid[1]
        # Monthly lands exactly on a grid point and uses that day's snapshot.
        assert universe_snapshot_date(days, grid[2]) == grid[2]
        # Bimonthly (42d) is every 2nd, quarterly (63d) every 3rd.
        assert universe_snapshot_date(days, days[42]) == grid[2]
        assert universe_snapshot_date(days, days[63]) == grid[3]

    def test_before_the_first_refresh_there_is_no_universe(self):
        """None, not "everything" — a run that starts before its first
        snapshot must trade nothing rather than the unranked candidate pool."""
        days = pd.DatetimeIndex(pd.bdate_range("2026-01-01", "2026-06-30"))
        assert universe_snapshot_date(days, date(2025, 12, 31)) is None


class TestLiquidUniverse:
    def test_ranks_by_trailing_adtv_descending(self, conn):
        assert liquid_universe(conn, AS_OF, top_n=3) == ["T0", "T1", "T2"]

    def test_a_ticker_with_no_volume_history_is_absent_not_assumed_liquid(self, conn):
        conn.execute("DELETE FROM ohlcv_adjusted WHERE ticker = 'T0'")
        assert "T0" not in liquid_universe(conn, AS_OF, top_n=6)

    def test_the_cap_is_the_universe_size(self, conn):
        assert len(liquid_universe(conn, AS_OF, top_n=2)) == 2


class TestBandComposition:
    def test_liquidity_filters_first_then_market_cap_ranks(self, conn):
        """The order is the whole decision. ADTV order here is T0..T5 and
        market-cap order is T5..T0, so a band built the wrong way round
        returns the opposite names."""
        # Universe = the 3 most liquid (T0, T1, T2). Within them, market cap
        # runs T2 > T1 > T0, so rank 1 is T2.
        assert momentum_band_universe(conn, AS_OF, 1, 1, top_n_by_adtv=3) == ["T2"]
        assert momentum_band_universe(conn, AS_OF, 1, 3, top_n_by_adtv=3) == ["T2", "T1", "T0"]

    def test_an_illiquid_large_cap_never_occupies_a_band_slot(self, conn):
        """T5 is the largest by market cap and the least liquid. Under a
        top-2 ADTV universe it must not appear at all — that is what keeps a
        band full of names an order could actually fill."""
        band = momentum_band_universe(conn, AS_OF, 1, 2, top_n_by_adtv=2)
        assert "T5" not in band
        assert band == ["T1", "T0"]

    def test_bands_partition_the_liquid_set_without_overlap(self, conn):
        first = momentum_band_universe(conn, AS_OF, 1, 3, top_n_by_adtv=6)
        second = momentum_band_universe(conn, AS_OF, 4, 6, top_n_by_adtv=6)
        assert set(first) & set(second) == set()
        assert len(first) == len(second) == 3

    def test_no_liquid_names_means_an_empty_band(self, conn):
        conn.execute("DELETE FROM ohlcv_adjusted")
        assert momentum_band_universe(conn, AS_OF, 1, 50) == []

    def test_the_default_universe_is_the_top_800(self):
        assert ADTV_UNIVERSE_TOP_N == 800
