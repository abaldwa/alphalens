"""
tests/unit/test_gainer_signal_ranker.py

Coverage for systems/ml_signal_engine_gainer/models/signal/signal_ranker.py
(SignalRankerModel) — previously untested (0% coverage). Real (if small)
lambdarank + Platt-scaling fit/predict/top-N cycle, not mocked.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.models.signal.signal_ranker import SignalRankerModel


def _make_ranking_data(n_dates=10, rows_per_date=15, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.Series(np.repeat(pd.bdate_range("2024-01-01", periods=n_dates), rows_per_date))
    n = len(dates)
    X = pd.DataFrame(rng.normal(size=(n, 5)), columns=[f"f{i}" for i in range(5)])
    score = X["f0"] - 0.5 * X["f1"]
    y = pd.Series((score > score.quantile(0.8)).astype(int).to_numpy())
    return X, y, dates


class TestSignalRankerModelTrain:
    def test_train_returns_diagnostics(self):
        X, y, dates = _make_ranking_data()
        model = SignalRankerModel(horizon_days=6, target_pct=0.05, random_state=1, n_estimators=20)
        diag = model.train(X, y, dates)
        assert diag["training_samples"] == len(X)
        assert 0.0 <= diag["positive_rate"] <= 1.0

    def test_train_with_only_one_class_uses_degenerate_calibrator(self):
        X, y, dates = _make_ranking_data()
        y = pd.Series([0] * len(y))  # force single-class labels
        model = SignalRankerModel(horizon_days=6, target_pct=0.05, random_state=1, n_estimators=20)
        model.train(X, y, dates)
        proba = model.predict_proba(X)
        # Degenerate calibrator: constant probability for every row.
        assert proba.nunique() == 1


class TestSignalRankerModelPredict:
    @pytest.fixture(scope="class")
    def trained_model(self):
        X, y, dates = _make_ranking_data(seed=2)
        model = SignalRankerModel(horizon_days=21, target_pct=0.10, random_state=3, n_estimators=20)
        model.train(X, y, dates)
        return model, X, dates

    def test_predict_proba_before_train_raises(self):
        model = SignalRankerModel(horizon_days=6, target_pct=0.05)
        with pytest.raises(RuntimeError, match="train"):
            model.predict_proba(pd.DataFrame({"f0": [1.0]}))

    def test_predict_proba_bounded_0_1(self, trained_model):
        model, X, _ = trained_model
        proba = model.predict_proba(X)
        assert (proba >= 0).all() and (proba <= 1).all()
        assert len(proba) == len(X)

    def test_top_n_per_date_returns_at_most_n_rows_per_date(self, trained_model):
        model, X, dates = trained_model
        top = model.top_n_per_date(X, dates, n=5)
        counts = top.groupby("date").size()
        assert (counts <= 5).all()

    def test_top_n_per_date_ranks_by_descending_probability_within_date(self, trained_model):
        model, X, dates = trained_model
        top = model.top_n_per_date(X, dates, n=5)
        for _, group in top.groupby("date"):
            assert (group["proba"].diff().dropna() <= 0).all()
