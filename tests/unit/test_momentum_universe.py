"""
tests/unit/test_momentum_universe.py

ML38 — features/momentum_universe.py. Real seeded DuckDB (ohlcv_adjusted +
fundamentals) via a fresh normalised schema per test, no mocks over the DB
layer, matching test_sector_accumulation.py's convention.
config.universe.load_universe_raw() is monkeypatched to a small controlled
DataFrame so the candidate ticker list is deterministic.
"""

from datetime import date

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


class TestYearlyBandUniversesIncludeDelisted:
    """2026-07-20 survivorship-bias fix: yearly_band_universes() previously
    had NO way to opt into include_delisted at all — every caller was
    silently stuck on the current-snapshot universe regardless of intent.
    ZZZ below is a real, seeded delisted ticker with a real historical
    price and market cap on the as_of date, absent from the (monkeypatched)
    current-snapshot raw_universe fixture — proving it only appears when
    include_delisted=True is actually threaded all the way through."""

    def _seed_delisted(self, db_path, ticker, delisting_date="2026-06-01"):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO delisted_companies (ticker, delisting_date) VALUES (?, ?)",
                [ticker, delisting_date],
            )

    def test_default_excludes_delisted_ticker(self, normalised_db, raw_universe, monkeypatch):
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: pd.DataFrame({"ticker": raw_universe}))
        self._seed_delisted(normalised_db, "ZZZ")
        _seed_fundamentals(normalised_db, "ZZZ", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "ZZZ", "2026-01-02", 500.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            universes = mu.yearly_band_universes(conn, "2026-01-01", "2026-12-31", 1, 1)

        assert "ZZZ" not in universes.get("2026-01-02", [])

    def test_include_delisted_true_surfaces_the_delisted_ticker(self, normalised_db, raw_universe, monkeypatch):
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: pd.DataFrame({"ticker": raw_universe}))
        self._seed_delisted(normalised_db, "ZZZ")
        _seed_fundamentals(normalised_db, "ZZZ", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "ZZZ", "2026-01-02", 500.0)  # highest close -> rank 1 if included

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            universes = mu.yearly_band_universes(
                conn, "2026-01-01", "2026-12-31", 1, 1, include_delisted=True,
            )

        assert universes["2026-01-02"] == ["ZZZ"]

    def test_reuses_the_supplied_connection_no_second_conflicting_connection(
        self, normalised_db, raw_universe, monkeypatch
    ):
        """Regression test for the real bug this fix uncovered: opening a
        SECOND connection (build_historical_universe_from_delisted's old
        behavior) to a file already open read_only=True/persist=False
        raises 'Connection Error: Can't open a connection to same database
        file with a different configuration than existing connections'."""
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: pd.DataFrame({"ticker": raw_universe}))
        self._seed_delisted(normalised_db, "ZZZ")
        _seed_fundamentals(normalised_db, "ZZZ", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "ZZZ", "2026-01-02", 500.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            # Must not raise — this exact call previously conflicted.
            mu.yearly_band_universes(conn, "2026-01-01", "2026-12-31", 1, 1, include_delisted=True)


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


class TestNifty500ProxyUniverse:
    """2026-07-20 (BacktestUmbrellaPlan.md Truthful Review Gap #5, decided
    methodology): rank the full candidate pool by real PIT market cap and
    take the top 500 as the historical-membership proxy, since real
    historical Nifty500 constituent lists aren't sourceable here."""

    def test_ranks_by_market_cap_and_defaults_to_including_delisted(self, normalised_db, raw_universe, monkeypatch):
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: pd.DataFrame({"ticker": raw_universe}))
        with get_duckdb_connection(normalised_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO delisted_companies (ticker, delisting_date) VALUES (?, ?)", ["ZZZ", "2026-06-01"],
            )
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_fundamentals(normalised_db, "ZZZ", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)
        _seed_ohlcv(normalised_db, "ZZZ", "2026-01-02", 999.0)  # highest close -> rank 1

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            top = mu.nifty500_proxy_universe(conn, "2026-01-02")

        assert top[0] == "ZZZ"  # default include_delisted=True surfaces it, ranked correctly
        assert "AAA" in top

    def test_exceeds_the_200_cap_used_by_the_narrower_rank_bands(self, normalised_db, monkeypatch):
        tickers = [f"T{i:03d}" for i in range(250)]
        monkeypatch.setattr(mu, "load_universe_raw", lambda: pd.DataFrame({"ticker": tickers}))
        with get_duckdb_connection(normalised_db, persist=False, read_only=False) as conn:
            for i, t in enumerate(tickers):
                conn.execute(
                    "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, "
                    "announcement_date, shares_outstanding) VALUES (?, 2025, 1, '2025-12-01', '2025-12-01', ?)",
                    [t, 1_000_000],
                )
                conn.execute(
                    "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, "
                    "delivery_qty, delivery_pct) VALUES ('2026-01-02', ?, ?, ?, ?, ?, 1000, 500, 50.0)",
                    [t, float(i), float(i), float(i), float(i)],
                )

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            top = mu.nifty500_proxy_universe(conn, "2026-01-02", include_delisted=False)

        assert len(top) == 250  # every seeded ticker ranked -> proves the 200 cap doesn't truncate this
        assert top[0] == "T249"  # highest close -> rank 1

    def test_yearly_variant_fixes_one_list_per_year(self, normalised_db, raw_universe, monkeypatch):
        # yearly_nifty500_proxy_universes defaults include_delisted=True, which
        # routes through config.universe.load_universe_raw (not mu.load_universe_raw)
        # for the "active" half of the candidate union — patch both.
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: pd.DataFrame({"ticker": raw_universe}))
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            yearly = mu.yearly_nifty500_proxy_universes(conn, "2026-01-01", "2026-12-31")

        assert "AAA" in yearly["2026-01-02"]


class TestYearlyRankLookupFromRankings:
    """2026-08-05 Momentum engine consolidation Phase 3 —
    yearly_rank_lookup_from_rankings mirrors
    yearly_band_approximation_flags_from_rankings' shape but over the FULL
    ranking (not a band slice), so a consumer can see ranks ABOVE its own
    band's rank_start."""

    def test_maps_every_ranked_ticker_to_its_real_rank(self, normalised_db, raw_universe):
        # Closes descending -> AAA rank 1, BBB rank 2, CCC rank 3 (equal shares).
        for ticker, close in [("AAA", 300.0), ("BBB", 200.0), ("CCC", 100.0)]:
            _seed_fundamentals(normalised_db, ticker, "2025-12-01", 1_000_000)
            _seed_ohlcv(normalised_db, ticker, "2026-01-02", close)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            rankings = mu.all_yearly_full_rankings(conn, "2026-01-01", "2026-12-31")
            lookup = mu.yearly_rank_lookup_from_rankings(rankings)

        assert lookup["2026-01-02"] == {"AAA": 1, "BBB": 2, "CCC": 3}
        # Not band-sliced: the same keys as the full ranking, regardless of
        # any band a caller might later ask about.
        assert set(lookup) == set(rankings)

    def test_ticker_absent_from_ranking_has_no_fabricated_rank(self, normalised_db, raw_universe):
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 1_000_000)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 100.0)
        # DDD has OHLCV but no shares_outstanding ever -> excluded outright.
        _seed_ohlcv(normalised_db, "DDD", "2026-01-02", 500.0)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            rankings = mu.all_yearly_full_rankings(conn, "2026-01-01", "2026-12-31")
            lookup = mu.yearly_rank_lookup_from_rankings(rankings)

        assert "DDD" not in lookup["2026-01-02"]
        assert lookup["2026-01-02"]["AAA"] == 1

    def test_empty_rankings_produce_empty_lookup(self, normalised_db, raw_universe):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            rankings = mu.all_yearly_full_rankings(conn, "2026-01-01", "2026-12-31")
            lookup = mu.yearly_rank_lookup_from_rankings(rankings)
        assert lookup == {}


class TestBuildYearlyRankBandUniverseProvider:
    """2026-08-05 Momentum engine consolidation Phase 2 — adapts this
    module's yearly-fixed band universes to backtest/core/engine.py's
    UniverseProvider = Callable[[date], List[str]] contract."""

    @pytest.fixture
    def two_year_rankings(self, normalised_db, raw_universe):
        # 2025: AAA(1) > BBB(2) > CCC(3).  2026: CCC(1) > AAA(2) > BBB(3)
        # — the band's membership genuinely changes between the two years.
        for ticker in ("AAA", "BBB", "CCC"):
            _seed_fundamentals(normalised_db, ticker, "2024-12-01", 1_000_000)
        for ticker, close in [("AAA", 300.0), ("BBB", 200.0), ("CCC", 100.0)]:
            _seed_ohlcv(normalised_db, ticker, "2025-01-01", close)
        for ticker, close in [("CCC", 900.0), ("AAA", 300.0), ("BBB", 200.0)]:
            _seed_ohlcv(normalised_db, ticker, "2026-01-02", close)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            return mu.all_yearly_full_rankings(conn, "2025-01-01", "2026-12-31")

    def test_returns_that_years_band_for_a_date_in_each_year(self, two_year_rankings):
        provider = mu.build_yearly_rank_band_universe_provider(two_year_rankings, 1, 2)

        # Mid-2025 -> the 2025-01-01 list, held constant all year.
        assert provider(date(2025, 6, 30)) == ["AAA", "BBB"]
        # Mid-2026 -> the 2026-01-02 list.
        assert provider(date(2026, 6, 30)) == ["CCC", "AAA"]

    def test_year_start_date_itself_uses_that_years_new_list(self, two_year_rankings):
        provider = mu.build_yearly_rank_band_universe_provider(two_year_rankings, 1, 2)
        # Boundary: <= is inclusive, so the first trading day already gets
        # its own year's list, not the previous year's.
        assert provider(date(2026, 1, 2)) == ["CCC", "AAA"]
        assert provider(date(2026, 1, 1)) == ["AAA", "BBB"]

    def test_date_before_any_year_start_is_empty_not_backdated(self, two_year_rankings):
        provider = mu.build_yearly_rank_band_universe_provider(two_year_rankings, 1, 2)
        # Back-dating the first year's membership here would be exactly the
        # look-ahead bias the yearly-fixing convention exists to avoid.
        assert provider(date(2024, 12, 31)) == []

    def test_slices_to_the_requested_band_only(self, two_year_rankings):
        band2 = mu.build_yearly_rank_band_universe_provider(two_year_rankings, 2, 3)
        assert band2(date(2025, 6, 30)) == ["BBB", "CCC"]

    def test_matches_orchestrator_universe_provider_contract(self, two_year_rankings):
        """OrchestratorConfig.universe_provider is Callable[[date], List[str]]
        and is called once per rebalance date — a plain datetime.date must
        work, and the result must be a real list of str."""
        from backtest.core.engine import OrchestratorConfig

        provider = mu.build_yearly_rank_band_universe_provider(two_year_rankings, 1, 2)
        config = OrchestratorConfig(
            trading_days=pd.DatetimeIndex([pd.Timestamp("2025-06-30")]),
            universe_provider=provider,
            price_lookup=lambda ticker, as_of: None,
        )
        result = config.universe_provider(date(2025, 6, 30))
        assert isinstance(result, list) and all(isinstance(t, str) for t in result)
        assert result == ["AAA", "BBB"]

    def test_empty_rankings_provider_returns_empty(self):
        provider = mu.build_yearly_rank_band_universe_provider({}, 1, 50)
        assert provider(date(2026, 6, 30)) == []
