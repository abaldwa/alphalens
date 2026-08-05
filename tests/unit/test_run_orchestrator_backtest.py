"""
tests/unit/test_run_orchestrator_backtest.py

Unit tests for backtest/run_orchestrator_backtest.py's _build_config —
specifically the PIT-safe universe_provider (a ticker is only a
candidate on dates it actually has a recent real OHLCV bar), which
replaced a static "same ticker list for every date" simplification.
"""

from datetime import date
from unittest.mock import patch

import pandas as pd

import pytest

import backtest.run_orchestrator_backtest as ro
from backtest.run_orchestrator_backtest import (
    _build_config,
    _fetch_real_ohlcv,
    _momentum_rank_band_wiring,
    build_technical_feature_lookup,
)


def _ohlcv_row(ticker, d, close=100.0):
    return {"date": pd.Timestamp(d), "ticker": ticker, "close": close}


class TestUniverseProviderPitSafety:
    def test_ticker_absent_before_its_real_listing_date(self):
        # A listed the whole window; B only starts trading mid-window (a
        # real IPO partway through) — B must not be a candidate before
        # its first real bar.
        rows = [_ohlcv_row("A", d) for d in pd.bdate_range("2023-01-01", "2023-06-30")]
        rows += [_ohlcv_row("B", d) for d in pd.bdate_range("2023-04-01", "2023-06-30")]
        ohlcv = pd.DataFrame(rows)
        config = _build_config(ohlcv, sector_map={})

        assert "B" not in config.universe_provider(date(2023, 2, 1))
        assert "A" in config.universe_provider(date(2023, 2, 1))
        assert "B" in config.universe_provider(date(2023, 4, 5))

    def test_ticker_absent_after_it_stops_trading(self):
        # C delists (stops appearing) partway through — must not be a
        # candidate long after its last real bar.
        rows = [_ohlcv_row("A", d) for d in pd.bdate_range("2023-01-01", "2023-06-30")]
        rows += [_ohlcv_row("C", d) for d in pd.bdate_range("2023-01-01", "2023-03-01")]
        ohlcv = pd.DataFrame(rows)
        config = _build_config(ohlcv, sector_map={})

        assert "C" in config.universe_provider(date(2023, 2, 15))
        assert "C" not in config.universe_provider(date(2023, 6, 1))
        assert "A" in config.universe_provider(date(2023, 6, 1))

    def test_short_gap_within_tolerance_still_included(self):
        # A trading holiday / brief suspension shouldn't falsely exclude
        # a still-genuinely-listed ticker.
        dates = list(pd.bdate_range("2023-01-01", "2023-01-10")) + list(pd.bdate_range("2023-01-15", "2023-06-30"))
        rows = [_ohlcv_row("A", d) for d in dates]
        ohlcv = pd.DataFrame(rows)
        config = _build_config(ohlcv, sector_map={})

        # A short gap (a few days) around 2023-01-12 should not exclude A.
        assert "A" in config.universe_provider(date(2023, 1, 12))


def _feature_df(date_str: str) -> pd.DataFrame:
    # A distinct DataFrame per date (score derived from the date string) so
    # a test can tell whether a returned row came from the date it asked
    # for, or a stale cached one.
    return pd.DataFrame([{"ticker": "TICK", "score": date_str}])


class TestBuildTechnicalFeatureLookupCaching:
    """build_technical_feature_lookup()'s cache regressed to an unbounded
    per-date dict at one point during 2026-07-25 development (never
    evicted — confirmed live via py-spy to accumulate one full-universe
    Parquet DataFrame per trading day across an entire multi-year backtest,
    see FeatureBacklog.md); fixed to a single-slot (current date only)
    cache since _apply_exit_policy() always walks dates strictly forward
    and never revisits an earlier date within one run. These tests pin
    both halves of that contract: same-day calls must not re-hit disk
    (the whole point of caching), and a date change must not leak/return
    stale data from a previous date (the bug a naive "just cache the last
    N" fix could reintroduce if N were 0 instead of exactly 1).

    [PERF, 2026-08-02] the size-1 cache now lives inside
    ScreenerEngine._load_df itself (shared with TechnicalAdapter's entry
    screening — see run_orchestrator_backtest.py's _shared_screener_engine
    wiring), not in this closure's own state, so these tests patch
    pd.read_parquet (what _load_df calls on a real cache miss) and
    Path.exists (to avoid needing a real Parquet file on disk) instead of
    replacing _load_df outright — mocking _load_df itself would bypass the
    very cache logic under test."""

    def _patched(self):
        return patch(
            "systems.technical_analysis.screener.engine.pd.read_parquet",
            side_effect=lambda path: _feature_df(path.stem),
        ), patch("pathlib.Path.exists", return_value=True)

    def test_repeated_lookups_on_the_same_date_load_once(self):
        p_read, p_exists = self._patched()
        with p_read as mock_read, p_exists:
            lookup = build_technical_feature_lookup()
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 3))
        assert mock_read.call_count == 1

    def test_new_date_reloads_and_returns_that_dates_data_not_stale(self):
        p_read, p_exists = self._patched()
        with p_read as mock_read, p_exists:
            lookup = build_technical_feature_lookup()
            first = lookup("TICK", date(2023, 1, 3))
            second = lookup("TICK", date(2023, 1, 4))
        assert mock_read.call_count == 2
        assert first["score"] == "2023-01-03"
        assert second["score"] == "2023-01-04"  # not the stale 01-03 value

    def test_returning_to_an_earlier_date_reloads_rather_than_erroring(self):
        # Not the real access pattern (dates are walked strictly forward in
        # practice), but the single-slot cache must degrade to "just reload
        # it" rather than ever return wrong data for an out-of-order call.
        p_read, p_exists = self._patched()
        with p_read as mock_read, p_exists:
            lookup = build_technical_feature_lookup()
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 4))
            third = lookup("TICK", date(2023, 1, 3))
        assert mock_read.call_count == 3
        assert third["score"] == "2023-01-03"

    def test_missing_ticker_on_a_date_returns_empty_dict(self):
        p_read, p_exists = self._patched()
        with p_read, p_exists:
            lookup = build_technical_feature_lookup()
            result = lookup("NOT_IN_SNAPSHOT", date(2023, 1, 3))
        assert result == {}

    def test_none_dataframe_for_a_date_returns_empty_dict(self):
        with patch("pathlib.Path.exists", return_value=False):
            lookup = build_technical_feature_lookup()
            result = lookup("TICK", date(2023, 1, 3))
        assert result == {}


class TestTradingDayStringsForPreload:
    """[PERF, 2026-08-02] --prefetch-feature-parquets threads
    [d.date().isoformat() for d in config.trading_days] into
    ScreenerEngine.preload_dates() (see _run_immediate/_run_deferred).
    _run_immediate/_run_deferred are heavy, full-pipeline functions this
    file doesn't unit-test directly (consistent with its existing scope —
    only extracted helpers like _build_config are tested here); this
    instead pins the actual integration point: the date-string conversion
    must produce exactly the sorted, deduplicated, YYYY-MM-DD-formatted
    list ScreenerEngine.preload_dates()/_load_df expect (same format
    TechnicalAdapter._filtered_candidates's `str(as_of_date)` and
    scripts/precompute_technical_screener_matches.py's manifest both
    use — a format mismatch here would silently make every preloaded
    date a cache miss)."""

    def test_trading_days_convert_to_sorted_deduplicated_iso_strings(self):
        rows = [_ohlcv_row("A", d) for d in pd.bdate_range("2023-01-01", "2023-01-10")]
        rows += [_ohlcv_row("B", d) for d in pd.bdate_range("2023-01-03", "2023-01-08")]  # overlapping dates
        ohlcv = pd.DataFrame(rows)
        config = _build_config(ohlcv, sector_map={})

        date_strings = [d.date().isoformat() for d in config.trading_days]

        assert date_strings == sorted(date_strings)
        assert len(date_strings) == len(set(date_strings))  # no duplicates despite 2 tickers' overlapping dates
        assert date_strings[0] == "2023-01-02"  # bdate_range starts on a Monday
        assert all(len(s) == 10 and s[4] == "-" and s[7] == "-" for s in date_strings)  # YYYY-MM-DD


class TestFetchRealOhlcvKeepsColumnsPivotNeeds:
    """Regression test for the crash confirmed by 3 independent reviewers:
    _fetch_real_ohlcv used to select only ["date", "ticker", "close"] from
    the bulk pull, but run_orchestrator_backtest's ADTV wiring pivots the
    SAME returned DataFrame on a "volume" column too
    (ohlcv.pivot(..., values="volume")), raising KeyError on every real
    run. Unlike hand-built panel fixtures elsewhere in this suite (which
    already have "volume" present and would never catch this), this test
    mocks only the DB/API call (DataStoreClient.get_ohlcv_bulk) and keeps
    the full realistic multi-column OHLCV shape
    (date/ticker/open/high/low/close/volume/...) that the real /_bulk
    endpoint returns, then exercises the exact downstream pivot calls
    run_orchestrator_backtest.py performs on _fetch_real_ohlcv's output."""

    def _bulk_df(self):
        rows = []
        for ticker in ("TICK_A", "TICK_B"):
            for d in pd.bdate_range("2023-01-01", "2023-03-31"):
                rows.append({
                    "date": d, "ticker": ticker,
                    "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                    "volume": 12345.0, "delivery_pct": 40.0, "adj_factor": 1.0,
                })
        return pd.DataFrame(rows)

    def test_pivot_on_volume_and_close_succeeds(self):
        with patch(
            "backtest.run_orchestrator_backtest.DataStoreClient.get_ohlcv_bulk",
            return_value=self._bulk_df(),
        ), patch(
            "backtest.run_orchestrator_backtest.get_tickers",
            return_value=["TICK_A", "TICK_B"],
        ):
            ohlcv = _fetch_real_ohlcv(
                max_tickers=None, min_history_days=1,
                start_date=date(2023, 1, 1), end_date=date(2023, 3, 31),
            )

        assert "volume" in ohlcv.columns
        assert "close" in ohlcv.columns

        # Exercises the exact pivot calls run_orchestrator_backtest.py's
        # run_orchestrator_backtest() performs right after _fetch_real_ohlcv
        # — this used to raise KeyError('volume').
        price_panel = ohlcv.pivot(index="date", columns="ticker", values="close")
        volume_panel = ohlcv.pivot(index="date", columns="ticker", values="volume")
        assert not price_panel.empty
        assert not volume_panel.empty


class TestFetchRealOhlcvMaxTickersUsesAdtvOrdering:
    """[2026-08-04] --max-tickers used to slice get_tickers()'s CSV-row-order
    list, which carries no liquidity meaning — a user reviewing partial
    sweep results (e.g. after --max-tickers 800 jobs finish) expects that
    800 to be the most-liquid names, not an arbitrary alphabetical/CSV-order
    slice. Fixed to use get_top_adtv_tickers (the same helper
    run_phase1_backtest.py already uses) whenever max_tickers is set."""

    def _bulk_df(self, tickers):
        rows = []
        for ticker in tickers:
            for d in pd.bdate_range("2023-01-01", "2023-01-10"):
                rows.append({
                    "date": d, "ticker": ticker,
                    "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                    "volume": 12345.0, "delivery_pct": 40.0, "adj_factor": 1.0,
                })
        return pd.DataFrame(rows)

    def test_max_tickers_set_calls_get_top_adtv_tickers_not_get_tickers(self):
        with patch(
            "backtest.run_orchestrator_backtest.DataStoreClient.get_ohlcv_bulk",
            return_value=self._bulk_df(["HIGH_ADTV"]),
        ), patch(
            "backtest.run_orchestrator_backtest.get_top_adtv_tickers",
            return_value=["HIGH_ADTV"],
        ) as mock_top_adtv, patch(
            "backtest.run_orchestrator_backtest.get_tickers",
        ) as mock_plain:
            _fetch_real_ohlcv(
                max_tickers=800, min_history_days=1,
                start_date=date(2023, 1, 1), end_date=date(2023, 1, 10),
            )

        mock_top_adtv.assert_called_once_with(800)
        mock_plain.assert_not_called()

    def test_max_tickers_none_falls_back_to_plain_get_tickers(self):
        with patch(
            "backtest.run_orchestrator_backtest.DataStoreClient.get_ohlcv_bulk",
            return_value=self._bulk_df(["ANY"]),
        ), patch(
            "backtest.run_orchestrator_backtest.get_tickers",
            return_value=["ANY"],
        ) as mock_plain, patch(
            "backtest.run_orchestrator_backtest.get_top_adtv_tickers",
        ) as mock_top_adtv:
            _fetch_real_ohlcv(
                max_tickers=None, min_history_days=1,
                start_date=date(2023, 1, 1), end_date=date(2023, 1, 10),
            )

        mock_plain.assert_called_once()
        mock_top_adtv.assert_not_called()


class TestMomentumRankBandWiring:
    """2026-08-05 Momentum engine consolidation Phase 2 —
    _momentum_rank_band_wiring builds the rank-band universe_provider,
    approximation_flags and sticky-promotion rank lookup from ONE
    all_yearly_full_rankings() call against a real seeded normalised DB
    (no DB mocking — same convention as tests/unit/test_momentum_universe.py).
    """

    @pytest.fixture
    def seeded_db(self, tmp_path, monkeypatch):
        from datastore.api.db import close_all_connections, get_duckdb_connection
        from datastore.schema import create_normalised

        db_path = tmp_path / "normalised_rank_band.duckdb"
        create_normalised.create_schema(db_path=db_path)
        close_all_connections()

        # Ranks by close (equal shares): AAA 1, BBB 2, CCC 3, DDD 4.
        rows = [("AAA", 400.0), ("BBB", 300.0), ("CCC", 200.0), ("DDD", 100.0)]
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            for ticker, close in rows:
                conn.execute(
                    "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, "
                    "announcement_date, shares_outstanding) VALUES (?, 2025, 1, '2025-12-01', '2025-12-01', ?)",
                    [ticker, 1_000_000],
                )
                conn.execute(
                    "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, "
                    "delivery_qty, delivery_pct) VALUES ('2026-01-02', ?, ?, ?, ?, ?, 1000, 500, 50.0)",
                    [ticker, close, close, close, close],
                )

        monkeypatch.setattr(ro, "DUCKDB_PATH", db_path)
        universe_df = pd.DataFrame({"ticker": [t for t, _ in rows]})
        monkeypatch.setattr("config.universe.load_universe_raw", lambda: universe_df)
        return db_path

    def test_builds_provider_flags_and_rank_lookup_for_the_requested_band(self, seeded_db):
        # RANK_BANDS band 2 is rank 51-100, far past this 4-ticker fixture,
        # so exercise the plumbing with band 1 (rank 1-50) and assert the
        # rank_start it reports back matches RANK_BANDS.
        wiring = _momentum_rank_band_wiring(1, date(2026, 1, 1), date(2026, 12, 31))

        assert wiring["rank_start"] == 1
        assert wiring["universe_provider"](date(2026, 6, 30)) == ["AAA", "BBB", "CCC", "DDD"]
        # Real PIT shares_outstanding on record -> nothing approximated.
        assert wiring["approximation_flags"]["2026-01-02"] == {
            "AAA": False, "BBB": False, "CCC": False, "DDD": False
        }
        # Full ranking (not the band slice) for sticky promotion.
        assert wiring["yearly_rank_lookup"]["2026-01-02"] == {"AAA": 1, "BBB": 2, "CCC": 3, "DDD": 4}

    def test_universe_is_empty_before_the_first_year_start(self, seeded_db):
        wiring = _momentum_rank_band_wiring(1, date(2026, 1, 1), date(2026, 12, 31))
        assert wiring["universe_provider"](date(2025, 12, 31)) == []

    def test_uses_include_delisted_so_survivorship_bias_is_not_reintroduced(self, seeded_db):
        """include_delisted=True is hard-wired (the module's own 2026-07-20
        survivorship-bias fix) — a rank-band universe built without it
        silently drops anything that delisted after the tested period."""
        seen = {}
        real = ro.all_yearly_full_rankings

        def _spy(conn, start, end, **kwargs):
            seen.update(kwargs)
            seen["n_calls"] = seen.get("n_calls", 0) + 1
            return real(conn, start, end, **kwargs)

        with patch.object(ro, "all_yearly_full_rankings", _spy):
            _momentum_rank_band_wiring(1, date(2026, 1, 1), date(2026, 12, 31))

        assert seen["include_delisted"] is True
        # One round trip total — the band slice, the approximation flags and
        # the rank lookup are all re-slices of this single result.
        assert seen["n_calls"] == 1

    def test_unknown_band_id_is_rejected(self, seeded_db):
        with pytest.raises(ValueError, match="unknown rank_band_id"):
            _momentum_rank_band_wiring(99, date(2026, 1, 1), date(2026, 12, 31))
