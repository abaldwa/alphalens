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

from backtest.run_orchestrator_backtest import _build_config, build_technical_feature_lookup


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
    N" fix could reintroduce if N were 0 instead of exactly 1)."""

    def test_repeated_lookups_on_the_same_date_load_once(self):
        with patch(
            "systems.technical_analysis.screener.engine.ScreenerEngine._load_df",
            side_effect=lambda date_str: _feature_df(date_str),
        ) as mock_load:
            lookup = build_technical_feature_lookup()
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 3))
        assert mock_load.call_count == 1

    def test_new_date_reloads_and_returns_that_dates_data_not_stale(self):
        with patch(
            "systems.technical_analysis.screener.engine.ScreenerEngine._load_df",
            side_effect=lambda date_str: _feature_df(date_str),
        ) as mock_load:
            lookup = build_technical_feature_lookup()
            first = lookup("TICK", date(2023, 1, 3))
            second = lookup("TICK", date(2023, 1, 4))
        assert mock_load.call_count == 2
        assert first["score"] == "2023-01-03"
        assert second["score"] == "2023-01-04"  # not the stale 01-03 value

    def test_returning_to_an_earlier_date_reloads_rather_than_erroring(self):
        # Not the real access pattern (dates are walked strictly forward in
        # practice), but the single-slot cache must degrade to "just reload
        # it" rather than ever return wrong data for an out-of-order call.
        with patch(
            "systems.technical_analysis.screener.engine.ScreenerEngine._load_df",
            side_effect=lambda date_str: _feature_df(date_str),
        ) as mock_load:
            lookup = build_technical_feature_lookup()
            lookup("TICK", date(2023, 1, 3))
            lookup("TICK", date(2023, 1, 4))
            third = lookup("TICK", date(2023, 1, 3))
        assert mock_load.call_count == 3
        assert third["score"] == "2023-01-03"

    def test_missing_ticker_on_a_date_returns_empty_dict(self):
        with patch(
            "systems.technical_analysis.screener.engine.ScreenerEngine._load_df",
            side_effect=lambda date_str: _feature_df(date_str),
        ):
            lookup = build_technical_feature_lookup()
            result = lookup("NOT_IN_SNAPSHOT", date(2023, 1, 3))
        assert result == {}

    def test_none_dataframe_for_a_date_returns_empty_dict(self):
        with patch(
            "systems.technical_analysis.screener.engine.ScreenerEngine._load_df",
            return_value=None,
        ):
            lookup = build_technical_feature_lookup()
            result = lookup("TICK", date(2023, 1, 3))
        assert result == {}
