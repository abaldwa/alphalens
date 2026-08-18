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


class TestLookaheadAndStaleness:
    """The three conventions adopted from the orchestrator's generic provider
    (2026-08-18), each of which momentum's own band provider lacked."""

    def test_the_adtv_window_ends_strictly_before_as_of(self, conn):
        """A spike on as_of_date itself must not promote a ticker: that volume
        had not printed when the decision was made."""
        # T5 is the least liquid. Give it an enormous bar ON the decision date.
        conn.execute(
            "INSERT INTO ohlcv_adjusted VALUES ('T5', DATE '2026-06-30', 100.0, 999999999)"
        )
        assert liquid_universe(conn, "2026-06-30", top_n=2) == ["T0", "T1"]
        # The same bar one day earlier DOES count — proving the test is
        # measuring the boundary, not simply ignoring the row.
        conn.execute(
            "INSERT INTO ohlcv_adjusted VALUES ('T5', DATE '2026-06-26', 100.0, 999999999)"
        )
        assert "T5" in liquid_universe(conn, "2026-06-30", top_n=2)

    def test_a_ticker_that_stopped_trading_is_excluded(self, conn):
        """Liquidity a fortnight ago is not evidence a suspended stock can be
        bought now."""
        conn.execute("DELETE FROM ohlcv_adjusted WHERE ticker = 'T0' AND date > DATE '2026-06-01'")
        assert "T0" not in liquid_universe(conn, "2026-06-30", top_n=6)
        # ...but it was tradeable back when it was still printing bars.
        assert "T0" in liquid_universe(conn, "2026-06-02", top_n=6)

    def test_delisted_names_are_included_by_default(self, monkeypatch, conn):
        """Survivorship bias: a stock alive on a past date belongs in that
        date's universe even though it later delisted. Momentum's 2026-07-20
        fix, kept — a relative-strength strategy is where vanishing losers
        flatter results most."""
        seen = {}

        def _candidates(include_delisted=False, normalised_conn=None):
            seen["include_delisted"] = include_delisted
            return [f"T{i}" for i in range(6)]

        monkeypatch.setattr(mu, "_all_candidate_tickers", _candidates)
        liquid_universe(conn, AS_OF)
        assert seen["include_delisted"] is True
        momentum_band_universe(conn, AS_OF, 1, 50)
        assert seen["include_delisted"] is True


class TestTheBacktestProviderIsTheSameDefinition:
    """The 21-day grid is a caching boundary, not a second rule: the provider
    a backtest calls must return exactly what live/paper get from
    momentum_band_universe on the snapshot date."""

    def test_the_provider_matches_a_direct_call_on_the_snapshot_date(self, conn):
        from features.momentum_universe import build_momentum_universe_provider

        days = pd.DatetimeIndex(pd.bdate_range("2026-05-01", "2026-06-30"))
        provider = build_momentum_universe_provider(conn, days, 1, 3, top_n_by_adtv=3)
        snapshot = universe_snapshot_date(days, days[25])
        assert provider(days[25].date()) == momentum_band_universe(
            conn, str(snapshot.date()), 1, 3, top_n_by_adtv=3
        )

    def test_membership_is_held_between_refreshes(self, conn):
        """A name must not leave the universe on a day no strategy trades."""
        from features.momentum_universe import build_momentum_universe_provider

        days = pd.DatetimeIndex(pd.bdate_range("2026-05-01", "2026-06-30"))
        provider = build_momentum_universe_provider(conn, days, 1, 3, top_n_by_adtv=3)
        grid_start = days.get_loc(universe_refresh_dates(days)[1])
        held = {tuple(provider(days[i].date())) for i in range(grid_start, grid_start + 5)}
        assert len(held) == 1

    def test_bands_beyond_rank_200_are_not_truncated_away(self, monkeypatch):
        """Regression: _momentum_rank_band_wiring called
        all_yearly_full_rankings without max_rank, defaulting to
        MAX_TRACKED_RANK=200, so bands 6/7/8 (201-300, 301-500, 501-800)
        sliced past the end of the frame and were EMPTY for every year."""
        c = duckdb.connect(":memory:")
        c.execute("CREATE TABLE ohlcv_adjusted (ticker VARCHAR, date DATE, close DOUBLE, volume BIGINT)")
        c.execute("CREATE TABLE fundamentals (ticker VARCHAR, announcement_date VARCHAR, shares_outstanding DOUBLE)")
        tickers = [f"S{i:03d}" for i in range(260)]
        for i, ticker in enumerate(tickers):
            c.execute("INSERT INTO fundamentals VALUES (?, ?, ?)", [ticker, "2025-01-01", float(260 - i) * 1e6])
            for day in pd.bdate_range("2026-05-01", "2026-06-30"):
                c.execute(
                    "INSERT INTO ohlcv_adjusted VALUES (?, ?, ?, ?)",
                    [ticker, day.date(), 100.0, 10_000 * (260 - i)],
                )
        monkeypatch.setattr(mu, "_all_candidate_tickers", lambda **kw: tickers)
        band = momentum_band_universe(c, "2026-06-30", 201, 250, top_n_by_adtv=260)
        c.close()
        assert len(band) == 50, "band 201-250 must not be empty"
