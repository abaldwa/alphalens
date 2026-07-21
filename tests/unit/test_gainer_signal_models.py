"""
tests/unit/test_gainer_signal_models.py

Coverage for systems/ml_signal_engine_gainer/models/signal/base_signal_model.py
and gainer_signal_models.py — previously untested (0% coverage) despite
being near-identical to systems/ml_signal_engine/models/signal/
base_signal_model.py (already covered by tests/unit/test_signal_models.py,
which this file mirrors). One real structural difference exercised here:
gainer signal models are one-sided (FixedPercentLabeler only ever emits
HOLD=0/BUY=1, never SELL=-1), which is exactly the fold-composition case
base_signal_model.py's _present_classes handling was built for.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.models.signal.base_signal_model import (
    CLASS_ORDER,
    SIGNAL_OUTPUT_COLUMNS,
    BaseSignalModel,
)
from systems.ml_signal_engine_gainer.models.signal.gainer_signal_models import (
    GainerSignal6DModel,
    GainerSignal21DModel,
    GainerSignal63DModel,
)


def _make_one_sided_data(n=300, n_features=6, seed=0, nan_frac=0.0):
    """HOLD(0)/BUY(1) only — matches FixedPercentLabeler's real output shape."""
    rng = np.random.default_rng(seed)
    X = pd.DataFrame(rng.normal(size=(n, n_features)), columns=[f"f{i}" for i in range(n_features)])
    score = X["f0"] - 0.5 * X["f1"] + rng.normal(scale=0.5, size=n)
    y = pd.Series(np.where(score > 0.3, 1, 0))
    if nan_frac > 0:
        mask = rng.random(size=X.shape) < nan_frac
        X = X.mask(mask)
    return X, y


@pytest.fixture(scope="module")
def trained_one_sided_model():
    X, y = _make_one_sided_data(n=300, seed=1)
    model = BaseSignalModel(horizon_days=6, random_state=7)
    model.train(X, y)
    return model, X


class TestOneSidedFoldHandling:
    """The exact scenario base_signal_model.py's _present_classes exists for:
    a fold with only 2 of the 3 CLASS_ORDER values present."""

    def test_trains_without_crashing_on_hold_buy_only_labels(self):
        X, y = _make_one_sided_data(n=250, seed=2)
        model = BaseSignalModel(horizon_days=6, random_state=3)
        model.train(X, y)  # must not raise despite y never containing -1 (sell)
        assert model._present_classes == [0, 1]

    def test_predict_proba_still_returns_full_sell_hold_buy_columns(self):
        X, y = _make_one_sided_data(n=250, seed=4)
        model = BaseSignalModel(horizon_days=6, random_state=5)
        model.train(X, y)
        proba = model.predict_proba(X)
        assert set(proba.columns) == {"sell", "hold", "buy"}
        # Sell was never in training data -> its probability must be exactly 0,
        # not NaN or fabricated.
        assert (proba["sell"] == 0.0).all()

    def test_probabilities_sum_to_one_even_with_unseen_class(self):
        X, y = _make_one_sided_data(n=250, seed=6)
        model = BaseSignalModel(horizon_days=6, random_state=7)
        model.train(X, y)
        proba = model.predict_proba(X)
        sums = proba[["sell", "hold", "buy"]].sum(axis=1)
        assert np.allclose(sums, 1.0, atol=1e-9)


class TestBaseSignalModelPredictSignals:
    def test_predict_signals_output_columns(self, trained_one_sided_model):
        model, X = trained_one_sided_model
        signals = model.predict_signals(X)
        assert list(signals.columns) == SIGNAL_OUTPUT_COLUMNS

    def test_predict_signals_probabilities_sum_to_one(self, trained_one_sided_model):
        model, X = trained_one_sided_model
        signals = model.predict_signals(X)
        sums = signals[["signal_buy_prob", "signal_hold_prob", "signal_sell_prob"]].sum(axis=1)
        assert np.allclose(sums, 1.0, atol=1e-9)

    def test_predict_returns_class_in_class_order(self, trained_one_sided_model):
        model, X = trained_one_sided_model
        preds = model.predict(X)
        assert set(preds.unique()).issubset(set(CLASS_ORDER))

    def test_handles_nan_features_without_dropping_all_rows(self):
        X, y = _make_one_sided_data(n=300, seed=8, nan_frac=0.15)
        model = BaseSignalModel(horizon_days=6, random_state=9)
        model.train(X, y)
        assert model._training_samples > 0
        preds = model.predict(X)
        assert len(preds) == len(X)


class TestSaveLoadRoundTrip:
    def test_save_and_load_preserve_predictions(self, tmp_path, trained_one_sided_model):
        model, X = trained_one_sided_model
        path = str(tmp_path / "gainer_model.pkl")
        model.save(path)

        loaded = BaseSignalModel(horizon_days=6)
        loaded.load(path)

        pd.testing.assert_series_equal(model.predict(X).reset_index(drop=True), loaded.predict(X).reset_index(drop=True))


class TestResample:
    def test_capped_max_sampling_ratio_keeps_minority_below_majority(self):
        rng = np.random.default_rng(1)
        n, minority_frac = 500, 0.05
        n_minority = max(int(n * minority_frac), 5)
        y = pd.Series([0] * (n - n_minority) + [1] * n_minority)
        X = pd.DataFrame(rng.normal(size=(len(y), 5)), columns=[f"f{i}" for i in range(5)])

        X_res, y_res = BaseSignalModel._resample(X, y, random_state=1, max_sampling_ratio=0.3)
        counts = pd.Series(y_res).value_counts()
        assert counts.min() / counts.max() < 0.6


class TestConcreteGainerModels:
    """GainerSignal6D/21D/63D just fix horizon_days/TARGET_PCT via __init__ —
    exercised here so those three thin subclasses aren't 0%-covered dead
    code relative to the base class."""

    @pytest.mark.parametrize(
        "cls,expected_horizon,expected_target",
        [
            (GainerSignal6DModel, 6, 0.05),
            (GainerSignal21DModel, 21, 0.10),
            (GainerSignal63DModel, 63, 0.20),
        ],
    )
    def test_horizon_and_target_pct_set_correctly(self, cls, expected_horizon, expected_target):
        model = cls(random_state=1)
        assert model.horizon_days == expected_horizon
        assert cls.TARGET_PCT == expected_target

    def test_gainer_signal_6d_trains_and_predicts(self):
        X, y = _make_one_sided_data(n=200, seed=10)
        model = GainerSignal6DModel(random_state=1)
        model.train(X, y)
        preds = model.predict(X)
        assert len(preds) == len(X)
