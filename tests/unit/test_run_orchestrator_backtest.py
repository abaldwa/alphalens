"""
tests/unit/test_run_orchestrator_backtest.py

Unit tests for backtest/run_orchestrator_backtest.py's _build_config —
specifically the PIT-safe universe_provider (a ticker is only a
candidate on dates it actually has a recent real OHLCV bar), which
replaced a static "same ticker list for every date" simplification.
"""

from datetime import date

import pandas as pd

from backtest.run_orchestrator_backtest import _build_config


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
