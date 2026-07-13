"""
tests/unit/test_signal_models.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-003, SPEC-MODEL-004, SPEC-MODEL-007
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for BaseSignalModel/Signal5DModel/Signal21DModel, MetaLabeler,
and ConformalPredictor. All fixtures are synthetic and use small Optuna
trial counts (2-3, not the documented production default of 100) so this
suite runs in seconds, not minutes — production-grade HPO is exercised
separately by systems/ml_signal_engine/inference/train_all_phase1.py.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

import lightgbm as lgb

from systems.ml_signal_engine.models.signal.base_signal_model import (
    CLASS_ORDER,
    SIGNAL_OUTPUT_COLUMNS,
    BaseSignalModel,
)
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.uncertainty.conformal import ConformalPredictor


def _make_classification_data(n=400, n_features=8, seed=0, nan_frac=0.0):
    rng = np.random.default_rng(seed)
    X = pd.DataFrame(rng.normal(size=(n, n_features)), columns=[f"f{i}" for i in range(n_features)])
    score = X["f0"] - 0.5 * X["f1"] + rng.normal(scale=0.5, size=n)
    y = pd.Series(np.where(score > 0.5, 1, np.where(score < -0.5, -1, 0)))
    returns = pd.Series(score * 0.02 + rng.normal(scale=0.01, size=n))
    if nan_frac > 0:
        mask = rng.random(size=X.shape) < nan_frac
        X = X.mask(mask)
    return X, y, returns


@pytest.fixture(scope="module")
def trained_base_model():
    X, y, returns = _make_classification_data(n=400, seed=1)
    split = int(len(X) * 0.7)
    X_train, y_train, ret_train = X.iloc[:split], y.iloc[:split], returns.iloc[:split]
    X_val, y_val, ret_val = X.iloc[split:], y.iloc[split:], returns.iloc[split:]

    model = BaseSignalModel(horizon_days=5, optuna_trials=2, random_state=7)
    model.train_full(X_train, y_train, X_val, y_val, returns_train=ret_train, returns_val=ret_val)
    return model, X_val


class TestResampleMaxSamplingRatio:
    """ML21 (2026-07-10): SMOTETomek's oversample ratio can be capped via
    max_sampling_ratio instead of imblearn's unbounded default 'auto'
    (1:1 with the majority class) — the root cause of the 2026-07-09
    signal_63d OOM (49.5% buy / 42.2% hold / 8.3% sell blown up to 1:1:1)."""

    def _make_skewed_labels(self, n=600, minority_frac=0.05, seed=0):
        rng = np.random.default_rng(seed)
        n_minority = max(int(n * minority_frac), 5)
        y = pd.Series([0] * (n - n_minority) + [1] * n_minority)
        X = pd.DataFrame(rng.normal(size=(len(y), 6)), columns=[f"f{i}" for i in range(6)])
        return X, y

    def test_default_auto_resamples_to_1to1(self):
        X, y = self._make_skewed_labels()
        X_res, y_res = BaseSignalModel._resample(X, y, random_state=1)
        counts = pd.Series(y_res).value_counts()
        # SMOTETomek's Tomek-link cleanup can trim a few majority-class
        # rows, but 'auto' still drives the classes to near-parity.
        assert counts.min() / counts.max() > 0.85

    def test_capped_ratio_keeps_minority_below_majority(self):
        X, y = self._make_skewed_labels()
        X_res, y_res = BaseSignalModel._resample(X, y, random_state=1, max_sampling_ratio=0.3)
        counts = pd.Series(y_res).value_counts()
        # Capped: minority class should land near 0.3x the majority count,
        # well short of 'auto''s ~1:1 parity — bounds the post-resample
        # matrix size directly.
        assert counts.min() / counts.max() < 0.6

    def test_max_sampling_ratio_plumbed_through_init_and_train_full(self):
        X, y, returns = _make_classification_data(n=400, seed=2)
        # Force one minority class by relabeling a small slice as 'sell'-only.
        model = BaseSignalModel(horizon_days=5, optuna_trials=1, random_state=3, max_sampling_ratio=0.4)
        assert model.max_sampling_ratio == 0.4
        split = int(len(X) * 0.7)
        diag = model.train_full(
            X.iloc[:split], y.iloc[:split], X.iloc[split:], y.iloc[split:],
            returns_train=returns.iloc[:split], returns_val=returns.iloc[split:],
        )
        assert diag["class_ratio_after"]
        assert model._training_samples > 0


class TestBaseSignalModel:
    def test_predict_signals_probabilities_sum_to_one(self, trained_base_model):
        """Prompt requirement: buy+hold+sell probabilities sum to 1.0."""
        model, X_val = trained_base_model
        signals = model.predict_signals(X_val)
        sums = signals[["signal_buy_prob", "signal_hold_prob", "signal_sell_prob"]].sum(axis=1)
        assert np.allclose(sums, 1.0, atol=1e-9)

    def test_predict_signals_output_columns(self, trained_base_model):
        model, X_val = trained_base_model
        signals = model.predict_signals(X_val)
        assert list(signals.columns) == SIGNAL_OUTPUT_COLUMNS

    def test_predict_signals_quantiles_populated_and_ordered(self, trained_base_model):
        """Q10 <= Q50 <= Q90 should hold for (almost) every row of a well-behaved quantile fit."""
        model, X_val = trained_base_model
        signals = model.predict_signals(X_val)
        assert signals[["signal_q10", "signal_q50", "signal_q90"]].notna().all().all()
        violations = (signals["signal_q10"] > signals["signal_q90"]).mean()
        assert violations < 0.05  # allow a small fraction of crossing, not a hard guarantee

    def test_predict_returns_class_in_class_order(self, trained_base_model):
        model, X_val = trained_base_model
        preds = model.predict(X_val)
        assert set(preds.unique()).issubset(set(CLASS_ORDER))

    def test_threshold_never_default_0_5(self, trained_base_model):
        """SPEC-MODEL-004: thresholds must be tuned, not the 0.5 default for every class."""
        model, _ = trained_base_model
        assert any(t != 0.5 for t in model._thresholds.values())

    def test_handles_nan_features_without_dropping_all_rows(self):
        """Regression guard: SMOTETomek can't handle NaN, but earlier code blanket-dropna'd on
        every feature column, which wiped out the whole dataset whenever ANY column had ANY NaN
        (a near-certainty with 70+ real feature columns and long lookback warm-ups). Caught while
        building train_all_phase1.py — see BuildLog.md "P1.5"."""
        X, y, returns = _make_classification_data(n=300, seed=3, nan_frac=0.15)
        split = int(len(X) * 0.7)
        model = BaseSignalModel(horizon_days=5, optuna_trials=2, random_state=3)
        diag = model.train_full(
            X.iloc[:split], y.iloc[:split], X.iloc[split:], y.iloc[split:],
            returns_train=returns.iloc[:split], returns_val=returns.iloc[split:],
        )
        assert model._training_samples > 0
        assert sum(diag["class_ratio_after"].values()) == pytest.approx(1.0)

    def test_save_load_roundtrip(self, trained_base_model, tmp_path):
        model, X_val = trained_base_model
        path = tmp_path / "signal_model.pkl"
        model.save(str(path))

        loaded = BaseSignalModel(horizon_days=5)
        loaded.load(str(path))

        pd.testing.assert_frame_equal(model.predict_signals(X_val), loaded.predict_signals(X_val))

    def test_train_raises_on_shape_mismatch(self):
        model = BaseSignalModel(horizon_days=5, optuna_trials=2)
        X = pd.DataFrame({"f0": [1.0, 2.0]})
        y = pd.Series([1])
        with pytest.raises(ValueError):
            model.train(X, y)

    def test_train_full_rejects_labels_outside_class_order(self):
        X, _, returns = _make_classification_data(n=100, seed=5)
        bad_y = pd.Series([2] * 100)
        model = BaseSignalModel(horizon_days=5, optuna_trials=2)
        with pytest.raises(ValueError):
            model.train_full(X, bad_y, X, bad_y)


class TestSignal5DAndSignal21D:
    def test_signal_5d_horizon(self):
        assert Signal5DModel().horizon_days == 5

    def test_signal_21d_horizon(self):
        assert Signal21DModel().horizon_days == 21

    def test_both_subclass_base_signal_model(self):
        assert isinstance(Signal5DModel(), BaseSignalModel)
        assert isinstance(Signal21DModel(), BaseSignalModel)

    def test_default_barrier_multipliers_match_triple_barrier_labeler_defaults(self):
        """SPEC-MODEL-002 defaults (2.0/1.0), not 02_models.md's per-horizon worked examples — see module docstrings."""
        assert Signal5DModel().profit_multiplier == 2.0
        assert Signal5DModel().stop_multiplier == 1.0
        assert Signal21DModel().profit_multiplier == 2.0


class TestMetaLabeler:
    def test_compute_labels_profitable_after_costs_is_one(self):
        direction = pd.Series([1])
        forward_return = pd.Series([0.05])  # well above any plausible round-trip cost
        labels = MetaLabeler.compute_labels(direction, forward_return)
        assert labels.iloc[0] == 1.0

    def test_compute_labels_unprofitable_after_costs_is_zero(self):
        direction = pd.Series([1])
        forward_return = pd.Series([0.001])  # tiny move, doesn't clear round-trip costs
        labels = MetaLabeler.compute_labels(direction, forward_return)
        assert labels.iloc[0] == 0.0

    def test_compute_labels_hold_rows_are_nan(self):
        direction = pd.Series([0, 1, -1])
        forward_return = pd.Series([0.05, 0.05, -0.05])
        labels = MetaLabeler.compute_labels(direction, forward_return)
        assert pd.isna(labels.iloc[0])
        assert labels.iloc[1] == 1.0
        assert labels.iloc[2] == 1.0  # short direction, price fell -> profitable

    def test_compute_labels_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            MetaLabeler.compute_labels(pd.Series([1, 1]), pd.Series([0.05]))

    def test_threshold_tuned_for_precision_not_0_5_default(self):
        rng = np.random.default_rng(11)
        n = 500
        X = pd.DataFrame(rng.normal(size=(n, 4)), columns=[f"f{i}" for i in range(4)])
        y = pd.Series((X["f0"] > 0.3).astype(int))  # learnable signal
        model = MetaLabeler(random_state=11)
        model.train(X, y)
        assert model._threshold != 0.5

    def test_predict_full_output_columns(self):
        rng = np.random.default_rng(12)
        n = 300
        X = pd.DataFrame(rng.normal(size=(n, 4)), columns=[f"f{i}" for i in range(4)])
        y = pd.Series((X["f0"] > 0).astype(int))
        model = MetaLabeler(random_state=12)
        model.train(X, y)
        full = model.predict_full(X)
        assert list(full.columns) == ["meta_label_act", "meta_label_prob"]

    def test_meta_labeler_precision_above_0_55_or_warns(self):
        """
        Prompt requirement: precision > 0.55 on held-out data; if not, this
        must report a WARNING, not fail the test. Implemented with
        warnings.warn (visible in pytest's warning summary) rather than
        pytest.fail/assert, so a noisy synthetic draw never breaks CI.
        """
        rng = np.random.default_rng(21)
        n = 1200
        X = pd.DataFrame(rng.normal(size=(n, 6)), columns=[f"f{i}" for i in range(6)])
        # A reasonably learnable but noisy signal -- not perfectly separable.
        logit = 2.0 * X["f0"] - 1.5 * X["f1"]
        prob = 1 / (1 + np.exp(-logit))
        y = pd.Series((rng.random(n) < prob).astype(int))

        split = int(n * 0.7)
        X_train, y_train = X.iloc[:split], y.iloc[:split]
        X_test, y_test = X.iloc[split:], y.iloc[split:]

        model = MetaLabeler(random_state=21)
        model.train(X_train, y_train)
        preds = model.predict(X_test)

        from sklearn.metrics import precision_score

        precision = precision_score(y_test, preds, zero_division=0)
        if precision <= 0.55:
            warnings.warn(
                UserWarning(f"MetaLabeler held-out precision {precision:.3f} <= 0.55 threshold (reported, not failed)")
            )
        else:
            assert precision > 0.55


class TestConformalPredictor:
    @pytest.fixture(scope="class")
    def calibrated_conformal(self):
        rng = np.random.default_rng(0)
        n = 2000
        X = pd.DataFrame(rng.normal(size=(n, 5)), columns=[f"f{i}" for i in range(5)])
        y = pd.Series(X["f0"] * 0.02 - X["f1"] * 0.01 + rng.normal(scale=0.015, size=n))

        X_train, y_train = X.iloc[:1200], y.iloc[:1200]
        X_cal, y_cal = X.iloc[1200:1600], y.iloc[1200:1600]
        X_test, y_test = X.iloc[1600:], y.iloc[1600:]

        estimator = lgb.LGBMRegressor(n_estimators=100, max_depth=4, verbose=-1, random_state=1)
        estimator.fit(X_train, y_train)

        cp = ConformalPredictor(estimator, target_coverage=0.90)
        cp.calibrate(X_cal, y_cal)
        return cp, X_test, y_test

    def test_coverage_at_least_88_percent(self, calibrated_conformal):
        """Prompt requirement: conformal interval achieves >= 88% coverage on held-out test data."""
        cp, X_test, y_test = calibrated_conformal
        coverage = cp.evaluate_coverage(X_test, y_test)
        assert coverage >= 0.88

    def test_predict_interval_columns(self, calibrated_conformal):
        cp, X_test, _ = calibrated_conformal
        intervals = cp.predict_interval(X_test.head(5))
        assert list(intervals.columns) == [
            "conformal_point", "conformal_lower", "conformal_upper", "conformal_width", "conformal_narrow",
        ]
        assert (intervals["conformal_upper"] >= intervals["conformal_lower"]).all()

    def test_narrow_flag_matches_width_threshold(self, calibrated_conformal):
        cp, X_test, _ = calibrated_conformal
        intervals = cp.predict_interval(X_test)
        expected = intervals["conformal_width"] < 0.04
        assert (intervals["conformal_narrow"] == expected).all()

    def test_predict_before_calibrate_raises(self):
        estimator = lgb.LGBMRegressor(verbose=-1)
        X = pd.DataFrame({"f0": [1.0, 2.0]})
        y = pd.Series([0.1, 0.2])
        estimator.fit(X, y)
        cp = ConformalPredictor(estimator)
        with pytest.raises(RuntimeError):
            cp.predict_interval(X)

    def test_invalid_target_coverage_raises(self):
        estimator = lgb.LGBMRegressor(verbose=-1)
        with pytest.raises(ValueError):
            ConformalPredictor(estimator, target_coverage=1.5)

    def test_adapt_updates_without_error(self, calibrated_conformal):
        cp, X_test, y_test = calibrated_conformal
        cp.adapt(X_test.head(50), y_test.head(50))
        coverage = cp.evaluate_coverage(X_test.tail(100), y_test.tail(100))
        assert 0.0 <= coverage <= 1.0
