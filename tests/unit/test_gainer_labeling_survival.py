"""
tests/unit/test_gainer_labeling_survival.py

ML33 (2026-07-13, development phase) — GAINER EXPERIMENT ONLY.
systems/ml_signal_engine_gainer/training/labeling.py::compute_fixed_pct_labels
gained a first_touch_day field (the day index the forward path first
reached +target_pct, NaN if never touched/censored) to support an RSF
survival head for the 21d/63d gainer models. Does not touch production
systems/ml_signal_engine/ labeling at all.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.training.labeling import (
    FixedPercentLabeler,
    compute_fixed_pct_labels,
)


def _closes(values):
    return pd.Series(values, index=pd.RangeIndex(len(values)), dtype="float64")


class TestFirstTouchDay:
    def test_touch_on_expected_day(self):
        # horizon=5, target=10%: day1 +2%, day2 +5%, day3 +11% (touch), day4/5 irrelevant after
        closes = _closes([100, 102, 105, 111, 108, 107])
        out = compute_fixed_pct_labels(closes, horizon_days=5, target_pct=0.10)
        assert out["label"].iloc[0] == 1.0
        assert out["first_touch_day"].iloc[0] == 3

    def test_no_touch_is_nan_first_touch_day(self):
        closes = _closes([100, 101, 102, 103, 104, 105])
        out = compute_fixed_pct_labels(closes, horizon_days=5, target_pct=0.50)
        assert out["label"].iloc[0] == 0.0
        assert np.isnan(out["first_touch_day"].iloc[0])

    def test_unresolvable_tail_is_nan(self):
        closes = _closes([100, 101, 102])
        out = compute_fixed_pct_labels(closes, horizon_days=5, target_pct=0.05)
        assert out["label"].isna().all()
        assert out["first_touch_day"].isna().all()

    def test_pnd_downgrade_also_clears_first_touch_day(self):
        closes = _closes([100, 115, 108, 107, 106, 105])
        pnd_block = pd.Series([True, False, False, False, False, False], index=closes.index)
        out = compute_fixed_pct_labels(closes, horizon_days=5, target_pct=0.10, pnd_block=pnd_block)
        assert out["label"].iloc[0] == 0.0  # downgraded from a would-be 1
        assert np.isnan(out["first_touch_day"].iloc[0])

    def test_first_touch_day_is_first_hit_not_max_return_day(self):
        # Touches target on day 2 (+10%), then spikes even higher on day 4 (+30%)
        # — first_touch_day must be 2 (first touch), not wherever max_return occurs.
        closes = _closes([100, 105, 111, 108, 131, 120])
        out = compute_fixed_pct_labels(closes, horizon_days=5, target_pct=0.10)
        assert out["label"].iloc[0] == 1.0
        assert out["first_touch_day"].iloc[0] == 2
        assert out["max_return"].iloc[0] == pytest.approx(0.31)

    def test_label_panel_preserves_first_touch_day_per_ticker(self):
        df = pd.DataFrame(
            {
                "ticker": ["AAA"] * 6 + ["BBB"] * 6,
                "close": [100, 102, 111, 108, 107, 106] + [100, 100, 100, 100, 105, 130],
            }
        )
        labeler = FixedPercentLabeler(horizon_days=5, target_pct=0.10)
        out = labeler.label_panel(df, close_col="close", ticker_col="ticker")
        assert "first_touch_day" in out.columns
        aaa_first_row = out.iloc[0]
        assert aaa_first_row["label"] == 1.0
        assert aaa_first_row["first_touch_day"] == 2
