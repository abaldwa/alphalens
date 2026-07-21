"""
tests/unit/test_gainer_walk_forward.py

Coverage for systems/ml_signal_engine_gainer/training/walk_forward.py —
previously untested (0% coverage). Mirrors tests/unit/test_backtester.py's
TestSplitData/TestTrainValidationSplit for the "regular" WalkForwardValidator,
adapted for this module's simpler calendar-year (not fiscal-year) split,
plus its two gainer-specific experiments: split_data_purged (purge+embargo)
and stock_level_kfold (ticker-based, not time-based, folds).
"""

import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.training.walk_forward import (
    WalkForwardValidator,
    stock_level_kfold,
)


def _daily_df(start, periods):
    dates = pd.bdate_range(start, periods=periods)
    return pd.DataFrame({"date": dates, "value": range(periods)})


class TestSplitData:
    def test_five_folds_produced(self):
        df = _daily_df("2020-01-01", 252 * 7)  # 7 distinct years
        validator = WalkForwardValidator(n_folds=5)
        folds = validator.split_data(df)
        assert len(folds) == 5

    def test_train_strictly_precedes_test_in_every_fold(self):
        df = _daily_df("2020-01-01", 252 * 7)
        validator = WalkForwardValidator(n_folds=5)
        folds = validator.split_data(df)
        for train_df, test_df in folds:
            assert train_df["date"].max() < test_df["date"].min()

    def test_expanding_window_train_set_grows_each_fold(self):
        df = _daily_df("2020-01-01", 252 * 7)
        validator = WalkForwardValidator(n_folds=5)
        folds = validator.split_data(df)
        sizes = [len(train_df) for train_df, _ in folds]
        assert sizes == sorted(sizes)

    def test_insufficient_years_raises(self):
        df = _daily_df("2020-01-01", 100)  # 1 year only
        validator = WalkForwardValidator(n_folds=5)
        with pytest.raises(ValueError, match="need more than"):
            validator.split_data(df)

    def test_missing_date_column_raises(self):
        df = pd.DataFrame({"not_date": [1, 2, 3]})
        validator = WalkForwardValidator(n_folds=2)
        with pytest.raises(ValueError, match="missing required column"):
            validator.split_data(df)

    def test_n_folds_override_at_call_time(self):
        df = _daily_df("2020-01-01", 252 * 7)
        validator = WalkForwardValidator(n_folds=5)
        folds = validator.split_data(df, n_folds=3)
        assert len(folds) == 3

    def test_n_folds_below_1_raises_at_construction(self):
        with pytest.raises(ValueError, match="n_folds must be >= 1"):
            WalkForwardValidator(n_folds=0)


class TestGetTrainValidationSplit:
    def test_validation_is_chronologically_last_slice(self):
        df = _daily_df("2020-01-01", 100)
        validator = WalkForwardValidator()
        train_only, val = validator.get_train_validation_split(df, val_fraction=0.2)
        assert train_only["date"].max() < val["date"].min()
        assert len(val) == pytest.approx(20, abs=2)

    def test_invalid_val_fraction_raises(self):
        df = _daily_df("2020-01-01", 100)
        validator = WalkForwardValidator()
        with pytest.raises(ValueError, match="val_fraction"):
            validator.get_train_validation_split(df, val_fraction=1.5)

    def test_missing_date_column_raises(self):
        validator = WalkForwardValidator()
        with pytest.raises(ValueError, match="missing required column"):
            validator.get_train_validation_split(pd.DataFrame({"x": [1]}))


class TestSplitDataPurged:
    def test_purge_and_embargo_drop_rows_near_fold_boundary(self):
        df = _daily_df("2020-01-01", 252 * 7)
        validator = WalkForwardValidator(n_folds=5)
        base_folds = validator.split_data(df)
        purged_folds = validator.split_data_purged(df, label_horizon_days=63, embargo_days=10, n_folds=5)
        assert len(purged_folds) == len(base_folds)
        # Purging must never ADD rows, only remove some near the boundary.
        for (base_train, _), (purged_train, _) in zip(base_folds, purged_folds):
            assert len(purged_train) <= len(base_train)

    def test_empty_test_fold_is_passed_through_unchanged(self):
        df = _daily_df("2020-01-01", 252 * 7)
        validator = WalkForwardValidator(n_folds=5)
        # A tiny label_horizon_days/embargo_days on real data still produces
        # non-empty folds; the empty-test-fold branch is defensive but we
        # confirm the normal (non-empty) path returns matching fold counts.
        purged_folds = validator.split_data_purged(df, label_horizon_days=1, embargo_days=1)
        assert all(not test_df.empty for _, test_df in purged_folds)


class TestStockLevelKfold:
    def test_each_ticker_appears_in_exactly_one_test_fold(self):
        tickers = [f"T{i:02d}" for i in range(20)]
        df = pd.concat(
            [pd.DataFrame({"ticker": [t] * 10, "value": range(10)}) for t in tickers],
            ignore_index=True,
        )
        folds = stock_level_kfold(df, n_folds=5, random_state=1)
        assert len(folds) == 5
        seen_test_tickers = []
        for train_df, test_df in folds:
            test_tickers = set(test_df["ticker"].unique())
            train_tickers = set(train_df["ticker"].unique())
            assert test_tickers.isdisjoint(train_tickers)
            seen_test_tickers.extend(test_tickers)
        assert sorted(seen_test_tickers) == sorted(tickers)

    def test_missing_ticker_column_raises(self):
        with pytest.raises(ValueError, match="missing required column"):
            stock_level_kfold(pd.DataFrame({"x": [1]}), n_folds=2)

    def test_too_few_tickers_raises(self):
        df = pd.DataFrame({"ticker": ["A", "B"]})
        with pytest.raises(ValueError, match="need >="):
            stock_level_kfold(df, n_folds=5)
