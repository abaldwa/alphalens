"""
tests/unit/test_features_intraday.py

Phase: 1.2 (Core Feature Computation)
Specs: SPEC-FEAT-001, SPEC-PIPE-004
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for features/intraday.py.
"""

import numpy as np
import pandas as pd
import pytest

from features.intraday import INTRADAY_FEATURES, compute_intraday_features


def _row(open_, high, low, close, ticker="A", date="2026-01-01"):
    return {"date": pd.Timestamp(date), "ticker": ticker, "open": open_, "high": high, "low": low, "close": close}


class TestComputeIntradayFeatures:
    def test_output_columns_and_dtype(self):
        df = pd.DataFrame([_row(100, 110, 95, 108)])
        out = compute_intraday_features(df)

        assert list(out.columns) == ["date", "ticker"] + INTRADAY_FEATURES
        for col in INTRADAY_FEATURES:
            assert out[col].dtype == np.float64

    def test_missing_required_column_raises(self):
        df = pd.DataFrame([{"date": "2026-01-01", "ticker": "A", "open": 1, "high": 2, "low": 0.5}])  # no close
        with pytest.raises(ValueError):
            compute_intraday_features(df)

    def test_clean_breakout_to_high_has_zero_upper_shadow_and_full_drive(self):
        """A bar that closes exactly at its high, opening at the low, has no upper shadow
        and a maximal (+1) opening-drive score (no giveback at all)."""
        df = pd.DataFrame([_row(open_=100, high=110, low=100, close=110)])
        out = compute_intraday_features(df)

        assert out.loc[0, "upper_shadow_pct"] == pytest.approx(0.0)
        assert out.loc[0, "opening_drive_strength"] == pytest.approx(1.0)

    def test_clean_breakdown_to_low_has_zero_lower_shadow_and_full_negative_drive(self):
        df = pd.DataFrame([_row(open_=110, high=110, low=100, close=100)])
        out = compute_intraday_features(df)

        assert out.loc[0, "lower_shadow_pct"] == pytest.approx(0.0)
        assert out.loc[0, "opening_drive_strength"] == pytest.approx(-1.0)

    def test_full_reversal_back_to_open_has_near_zero_drive(self):
        """Price ran up to the high then fully reverted to the open by close: no sustained drive."""
        df = pd.DataFrame([_row(open_=100, high=110, low=100, close=100)])
        out = compute_intraday_features(df)

        # direction is 0 here (close == open) -> opening_drive_strength is exactly 0
        assert out.loc[0, "opening_drive_strength"] == pytest.approx(0.0)

    def test_zero_range_day_yields_nan_not_inf_or_crash(self):
        """A circuit-locked / no-movement day has high == low; shadows/drive must be NaN, not a divide error."""
        df = pd.DataFrame([_row(open_=100, high=100, low=100, close=100)])
        out = compute_intraday_features(df)

        for col in INTRADAY_FEATURES:
            assert pd.isna(out.loc[0, col])
        assert not np.isinf(out[INTRADAY_FEATURES].to_numpy()).any()

    def test_multi_ticker_no_cross_contamination(self):
        """Each row's features depend only on that row's own OHLC — independent of other rows/tickers."""
        df = pd.DataFrame(
            [
                _row(100, 110, 95, 108, ticker="A", date="2026-01-01"),
                _row(50, 52, 48, 49, ticker="B", date="2026-01-01"),
            ]
        )
        out_both = compute_intraday_features(df)
        out_a_alone = compute_intraday_features(df[df["ticker"] == "A"].reset_index(drop=True))

        row_a_in_both = out_both[out_both["ticker"] == "A"][INTRADAY_FEATURES].reset_index(drop=True)
        row_a_alone = out_a_alone[INTRADAY_FEATURES].reset_index(drop=True)
        pd.testing.assert_frame_equal(row_a_in_both, row_a_alone)
