"""
tests/unit/test_market_regime.py

Unit tests for systems/regime/market_regime.py's classify_regimes() —
the rule-based Bull/Bear/Sideways segmenter (20% threshold + sideways
timeout).
"""

from datetime import date, timedelta

import pandas as pd

from systems.regime.market_regime import (
    BULL_BEAR_THRESHOLD_PCT,
    SIDEWAYS_WINDOW_TRADING_DAYS,
    classify_regimes,
)


def _series(prices: list, start: date = date(2020, 1, 1)) -> pd.Series:
    idx = [start + timedelta(days=i) for i in range(len(prices))]
    return pd.Series(prices, index=pd.DatetimeIndex(idx))


class TestClassifyRegimes:
    def test_empty_series_returns_no_segments(self):
        assert classify_regimes(pd.Series(dtype=float)) == []

    def test_single_price_returns_one_open_sideways_segment(self):
        segs = classify_regimes(_series([100.0]))
        assert len(segs) == 1
        assert segs[0].regime == "sideways"

    def test_clean_rally_confirms_bull_backdated_to_trough(self):
        # 100 -> 79 (trough) -> 100 (25% rally from 79, confirms bull)
        prices = [100.0, 95.0, 90.0, 85.0, 79.0, 85.0, 92.0, 100.0]
        segs = classify_regimes(_series(prices))
        bull_segs = [s for s in segs if s.regime == "bull"]
        assert bull_segs, "expected a confirmed bull segment"
        # backdated start should be the trough (index 4 -> day 2020-01-05)
        assert bull_segs[0].start_date == date(2020, 1, 5)
        assert bull_segs[0].move_pct is not None
        assert bull_segs[0].move_pct >= BULL_BEAR_THRESHOLD_PCT

    def test_clean_decline_confirms_bear_backdated_to_peak(self):
        prices = [100.0, 105.0, 110.0, 120.0, 115.0, 105.0, 95.0]
        segs = classify_regimes(_series(prices))
        bear_segs = [s for s in segs if s.regime == "bear"]
        assert bear_segs
        # peak was 120 at index 3 -> day 2020-01-04
        assert bear_segs[0].start_date == date(2020, 1, 4)
        assert bear_segs[0].move_pct is not None and bear_segs[0].move_pct < 0

    def test_flat_prices_stay_sideways_and_split_on_timeout(self):
        n = SIDEWAYS_WINDOW_TRADING_DAYS * 2 + 5
        prices = [100.0] * n
        segs = classify_regimes(_series(prices))
        assert all(s.regime == "sideways" for s in segs)
        # should split into at least 2 sideways segments given the timeout
        assert len(segs) >= 2

    def test_segments_are_contiguous_and_cover_full_range(self):
        prices = [100.0, 95.0, 90.0, 79.0, 85.0, 92.0, 100.0, 105.0, 130.0, 120.0, 95.0]
        s = _series(prices)
        segs = classify_regimes(s)
        assert segs[0].start_date == s.index[0].date()
        assert segs[-1].end_date == s.index[-1].date()
        for prev, nxt in zip(segs, segs[1:]):
            assert nxt.start_date == prev.end_date or nxt.start_date == prev.end_date + timedelta(days=1)

    def test_confirmed_date_never_before_start_date(self):
        prices = [100.0, 95.0, 90.0, 79.0, 85.0, 92.0, 100.0, 130.0, 100.0, 80.0]
        segs = classify_regimes(_series(prices))
        for s in segs:
            assert s.confirmed_date >= s.start_date

    def test_last_segment_is_open_and_uses_final_price(self):
        prices = [100.0, 95.0, 90.0, 79.0, 90.0]  # rallying but not yet 20% from trough
        segs = classify_regimes(_series(prices))
        assert segs[-1].end_date == date(2020, 1, 5)

    def test_dropna_handles_missing_values(self):
        s = _series([100.0, 95.0]).reindex(
            pd.date_range("2020-01-01", periods=4)
        )
        segs = classify_regimes(s)
        assert segs  # doesn't raise, produces something sensible
