"""
tests/unit/test_regime_pit_gating.py

Phase: 3.x (Backtest — regime gating)
Specs: SPEC-BT-REGIME
Owner: QA / Platform
Consumers: pytest CI

Regression tests for the 2026-08-09 regime lookahead bug and for the
running-peak drawdown gate added to replace it.

The bug: BacktestOrchestrator/MomentumAdapter/TechnicalAdapter each matched
a regime segment with `start_date <= as_of <= end_date`. classify_regimes()
backdates start_date to the anchoring peak, so that lookup returned "bear"
on dates months before the rule confirming that bear could possibly have
fired. Measured on the real Nifty 500 at a 12% threshold, every 2021-2026
bear segment confirmed AFTER it had already ended (109-228 days of lookahead
each). These tests pin the corrected semantics so the old lookup cannot be
reintroduced.
"""

import pandas as pd
import pytest

from systems.regime.market_regime import (
    bear_by_running_peak_drawdown,
    classify_regimes,
    drawdown_method_name,
    method_name,
)
from systems.regime.regime_store import regime_known_as_of


def _seg(regime, start, confirmed, end):
    return {
        "regime": regime,
        "start_date": pd.Timestamp(start).date(),
        "confirmed_date": pd.Timestamp(confirmed).date(),
        "end_date": pd.Timestamp(end).date(),
    }


class TestRegimeKnownAsOf:
    """The core PIT contract: a segment is invisible until it confirms."""

    def test_backdated_segment_is_not_visible_before_confirmation(self):
        """THE regression test. A bear backdated to 2021-10-18 but only
        confirmed on 2022-04-04 must not be reported on 2021-10-18 — that
        is precisely the lookahead the old start_date lookup granted."""
        segs = [
            _seg("bull", "2020-03-23", "2020-06-01", "2021-10-17"),
            _seg("bear", "2021-10-18", "2022-04-04", "2022-03-04"),
        ]
        as_of = pd.Timestamp("2021-10-18").date()
        assert regime_known_as_of(segs, as_of) == "bull", (
            "regime on the backdated bear start date must still be the last CONFIRMED "
            "regime (bull), not the not-yet-confirmed bear"
        )

    def test_segment_becomes_visible_on_its_confirmation_date(self):
        segs = [
            _seg("bull", "2020-03-23", "2020-06-01", "2021-10-17"),
            _seg("bear", "2021-10-18", "2022-04-04", "2022-03-04"),
        ]
        assert regime_known_as_of(segs, pd.Timestamp("2022-04-03").date()) == "bull"
        assert regime_known_as_of(segs, pd.Timestamp("2022-04-04").date()) == "bear"

    def test_latest_confirmed_segment_wins(self):
        segs = [
            _seg("bull", "2020-03-23", "2020-06-01", "2021-10-17"),
            _seg("bear", "2021-10-18", "2022-04-04", "2022-03-04"),
            _seg("bull", "2022-03-07", "2022-05-12", "2024-09-25"),
        ]
        assert regime_known_as_of(segs, pd.Timestamp("2022-05-12").date()) == "bull"
        assert regime_known_as_of(segs, pd.Timestamp("2022-05-11").date()) == "bear"

    def test_regime_persists_past_segment_end_until_next_confirmation(self):
        """A segment's end_date is itself only knowable in hindsight, so the
        confirmed label stays in force until a LATER segment confirms —
        bounding by end_date would reintroduce hindsight through the back
        door and yield None for real trading days."""
        segs = [
            _seg("bear", "2021-10-18", "2022-04-04", "2022-03-04"),
            _seg("bull", "2022-03-07", "2022-05-12", "2024-09-25"),
        ]
        # 2022-04-20 is past the bear's end_date but before the bull confirms.
        assert regime_known_as_of(segs, pd.Timestamp("2022-04-20").date()) == "bear"

    def test_none_before_anything_confirms(self):
        segs = [_seg("bear", "2021-10-18", "2022-04-04", "2022-03-04")]
        assert regime_known_as_of(segs, pd.Timestamp("2021-01-01").date()) is None

    def test_empty_segments_returns_none(self):
        assert regime_known_as_of([], pd.Timestamp("2022-01-01").date()) is None

    def test_never_returns_a_regime_confirmed_in_the_future(self):
        """Property-style sweep: across a range of as_of dates, the returned
        label must always belong to a segment already confirmed."""
        segs = [
            _seg("bull", "2020-03-23", "2020-06-01", "2021-10-17"),
            _seg("bear", "2021-10-18", "2022-04-04", "2022-03-04"),
            _seg("bull", "2022-03-07", "2022-05-12", "2024-09-25"),
        ]
        for ts in pd.date_range("2020-01-01", "2023-01-01", freq="7D"):
            as_of = ts.date()
            label = regime_known_as_of(segs, as_of)
            if label is None:
                continue
            confirmed = [s for s in segs if s["confirmed_date"] <= as_of]
            assert label in {s["regime"] for s in confirmed}


class TestRunningPeakDrawdownGate:
    """The live-deployable replacement: no confirmation lag by construction."""

    def test_bear_once_below_threshold_from_running_peak(self):
        prices = pd.Series(
            [100.0, 110.0, 100.0, 96.0, 88.0],
            index=pd.date_range("2021-01-01", periods=5, freq="D"),
        )
        labels = bear_by_running_peak_drawdown(prices, threshold_pct=0.12)
        # running peak is 110 from day 2 on; 96/110-1 = -12.7% -> bear
        assert list(labels) == ["bull", "bull", "bull", "bear", "bear"]

    def test_label_depends_only_on_past_data(self):
        """THE property that makes this deployable: truncating the series
        after date t must not change the label at t. classify_regimes()
        cannot satisfy this — that is why this function exists."""
        prices = pd.Series(
            [100.0, 120.0, 105.0, 100.0, 130.0, 110.0, 90.0],
            index=pd.date_range("2021-01-01", periods=7, freq="D"),
        )
        full = bear_by_running_peak_drawdown(prices, threshold_pct=0.12)
        for i in range(1, len(prices) + 1):
            truncated = bear_by_running_peak_drawdown(prices.iloc[:i], threshold_pct=0.12)
            assert truncated.iloc[-1] == full.iloc[i - 1], (
                f"label at position {i - 1} changed when future data was removed — "
                "the gate is peeking ahead"
            )

    def test_recovers_to_bull_when_price_returns_near_peak(self):
        prices = pd.Series(
            [100.0, 80.0, 99.0],
            index=pd.date_range("2021-01-01", periods=3, freq="D"),
        )
        labels = bear_by_running_peak_drawdown(prices, threshold_pct=0.12)
        assert list(labels) == ["bull", "bear", "bull"]

    def test_threshold_is_respected(self):
        prices = pd.Series(
            [100.0, 85.0], index=pd.date_range("2021-01-01", periods=2, freq="D")
        )
        # -15%: crosses a 12% threshold, does not cross a 20% one.
        assert list(bear_by_running_peak_drawdown(prices, 0.12)) == ["bull", "bear"]
        assert list(bear_by_running_peak_drawdown(prices, 0.20)) == ["bull", "bull"]
        # Boundary: exactly at the threshold counts as bear (<= -threshold).
        at_threshold = pd.Series(
            [100.0, 88.0], index=pd.date_range("2021-01-01", periods=2, freq="D")
        )
        assert list(bear_by_running_peak_drawdown(at_threshold, 0.12)) == ["bull", "bear"]

    def test_empty_series_returns_empty(self):
        assert bear_by_running_peak_drawdown(pd.Series(dtype=float)).empty

    def test_method_names_are_distinct_from_segment_methods(self):
        """The two instruments must never collide in market_regimes.method."""
        assert drawdown_method_name(0.12) == "12pct_drawdown_from_running_peak_v1"
        assert drawdown_method_name(0.12) != method_name(0.12)


class TestClassifyRegimesStillBackdates:
    """Documents WHY the gate above is needed — this is not a bug in
    classify_regimes (backdating is correct for reporting), it is a reason
    not to use it as a trading gate."""

    def test_confirmed_date_lags_start_date_on_a_real_shaped_series(self):
        prices = pd.Series(
            [100.0] + [100.0 - i for i in range(1, 40)],
            index=pd.date_range("2021-01-01", periods=40, freq="D"),
        )
        segs = classify_regimes(prices, threshold_pct=0.12)
        bears = [s for s in segs if s.regime == "bear"]
        assert bears, "expected a bear on a monotonically falling series"
        assert bears[0].confirmed_date > bears[0].start_date, (
            "classify_regimes must backdate start_date behind confirmed_date — "
            "if this ever stops being true, re-check whether the PIT gate is still needed"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
