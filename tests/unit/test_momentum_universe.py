"""
tests/unit/test_momentum_universe.py

ML38 — features/momentum_universe.py. Real seeded DuckDB (ohlcv_adjusted +
fundamentals) via a fresh normalised schema per test, no mocks over the DB
layer, matching test_sector_accumulation.py's convention.
config.universe.load_universe_raw() is monkeypatched to a small controlled
DataFrame so the candidate ticker list is deterministic.
"""

import pandas as pd
import pytest

import features.momentum_universe as mu
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised


def _seed_ohlcv(db_path, ticker, date_str, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
            VALUES (?, ?, ?, ?, ?, ?, 1000, 500, 50.0)
            ON CONFLICT DO NOTHING
            """,
            [date_str, ticker, close, close, close, close],
        )


def _seed_fundamentals(db_path, ticker, announcement_date, shares_outstanding):
    fiscal_year = int(announcement_date[:4])
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, shares_outstanding)
            VALUES (?, ?, 1, ?, ?, ?)
            """,
            [ticker, fiscal_year, announcement_date, announcement_date, shares_outstanding],
        )


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


@pytest.fixture
def raw_universe(monkeypatch):
    tickers = ["AAA", "BBB", "CCC", "DDD"]
    df = pd.DataFrame({"ticker": tickers})
    monkeypatch.setattr(mu, "load_universe_raw", lambda: df)
    return tickers


class TestMarketCapSnapshot:
    def test_excludes_ticker_missing_fundamentals(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100)
        _seed_ohlcv(normalised_db, "BBB", "2026-01-02", 200)  # no fundamentals seeded for BBB

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            snapshot = mu.market_cap_snapshot(conn, ["AAA", "BBB"], "2026-01-02")

        assert list(snapshot["ticker"]) == ["AAA"]

    def test_falls_back_to_earliest_known_shares_outstanding_when_no_pit_row(self, normalised_db, raw_universe):
        """2026-07-14 user decision: a date with no real PIT-eligible
        fundamentals row (e.g. 10 years before this DB's real
        shares_outstanding coverage starts) falls back to the ticker's
        earliest-ever real observation, flagged as approximated — rather
        than excluding the ticker outright, which left every pre-2024 year
        with zero constituents on the first real run."""
        _seed_fundamentals(normalised_db, "AAA", "2025-06-01", 1_000_000)  # only real row is AFTER as_of_date
        _seed_ohlcv(normalised_db, "AAA", "2016-01-04", 100.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            snapshot = mu.market_cap_snapshot(conn, ["AAA"], "2016-01-04")

        assert list(snapshot["ticker"]) == ["AAA"]
        assert bool(snapshot.iloc[0]["shares_outstanding_is_approximated"]) is True
        assert snapshot.iloc[0]["market_cap_cr"] == pytest.approx(10.0)  # 100 * 1,000,000 / 1e7

    def test_real_pit_row_not_overridden_by_earlier_fallback(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2015-01-01", 500_000)  # earliest real row
        _seed_fundamentals(normalised_db, "AAA", "2025-06-01", 1_000_000)  # PIT-eligible for 2026 as_of_date
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 500.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            snapshot = mu.market_cap_snapshot(conn, ["AAA"], "2026-01-02")

        assert bool(snapshot.iloc[0]["shares_outstanding_is_approximated"]) is False
        assert snapshot.iloc[0]["shares_outstanding"] == 1_000_000

    def test_market_cap_computed_correctly(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 500.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            snapshot = mu.market_cap_snapshot(conn, ["AAA"], "2026-01-02")

        # 500 * 1,000,000 / 1e7 = 50 cr
        assert snapshot.iloc[0]["market_cap_cr"] == pytest.approx(50.0)


class TestRankBandTickers:
    def test_ranks_descending_by_market_cap(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_fundamentals(normalised_db, "BBB", "2025-12-01", 1_000_000)
        _seed_fundamentals(normalised_db, "CCC", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)
        _seed_ohlcv(normalised_db, "BBB", "2026-01-02", 300.0)
        _seed_ohlcv(normalised_db, "CCC", "2026-01-02", 200.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            top2 = mu.rank_band_tickers(conn, "2026-01-02", 1, 2)

        assert top2 == ["BBB", "CCC"]

    def test_missing_data_returns_empty(self, normalised_db, raw_universe):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = mu.rank_band_tickers(conn, "2026-01-02", 1, 2)
        assert result == []


class TestYearlyBandUniverses:
    def test_one_list_per_calendar_year(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_fundamentals(normalised_db, "BBB", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)
        _seed_ohlcv(normalised_db, "BBB", "2026-01-02", 300.0)
        _seed_ohlcv(normalised_db, "AAA", "2027-01-04", 400.0)
        _seed_ohlcv(normalised_db, "BBB", "2027-01-04", 100.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            universes = mu.yearly_band_universes(conn, "2026-01-01", "2027-12-31", 1, 1)

        assert universes["2026-01-02"] == ["BBB"]
        assert universes["2027-01-04"] == ["AAA"]


class TestApproximationFlagsThreading:
    """2026-07-19 full-codebase-review Fix 6: yearly_band_approximation_flags_from_rankings
    preserves the shares_outstanding_is_approximated flag that
    yearly_band_universes_from_rankings' plain ticker-list slice drops."""

    def test_flags_match_ticker_list_membership(self, normalised_db, raw_universe):
        # AAA has a real PIT-eligible row on the as_of date -> not approximated.
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        # BBB's only fundamentals row is AFTER the as_of date -> falls back
        # to the earliest-known-shares-outstanding proxy -> approximated.
        _seed_fundamentals(normalised_db, "BBB", "2026-06-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)
        _seed_ohlcv(normalised_db, "BBB", "2026-01-02", 300.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            rankings = mu.all_yearly_full_rankings(conn, "2026-01-01", "2026-12-31")
            tickers = mu.yearly_band_universes_from_rankings(rankings, 1, 2)
            flags = mu.yearly_band_approximation_flags_from_rankings(rankings, 1, 2)

        date_key = "2026-01-02"
        assert set(tickers[date_key]) == set(flags[date_key].keys())
        assert flags[date_key]["AAA"] is False
        assert flags[date_key]["BBB"] is True

    def test_empty_rankings_produce_empty_flags(self, normalised_db, raw_universe):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            rankings = mu.all_yearly_full_rankings(conn, "2026-01-01", "2026-12-31")
            flags = mu.yearly_band_approximation_flags_from_rankings(rankings, 1, 2)
        assert flags == {}
