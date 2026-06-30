"""
tests/unit/test_score_forensic.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-MODEL-009, SPEC-MODEL-010
Owner: Platform / QA
Consumers: CI, pytest

Tests systems/ml_signal_engine/inference/score_forensic.py's orchestration
(per-ticker isolation, write gating, NaN-to-None mapping) with a mocked
DataStoreClient and a pre-trained ForensicMLModel — entirely offline, no
real network/DB access. The underlying building blocks (compute_forensic_
ml_features, compute_forensic_classical_scores, ForensicMLModel itself)
already have their own dedicated test coverage (test_forensic_classical.py,
test_known_frauds.py) — this file only exercises score_universe()'s own
orchestration logic.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.inference.score_forensic import _none_if_nan, score_universe
from systems.ml_signal_engine.models.forensic.forensic_ml import (
    FORENSIC_ML_FEATURES,
    ForensicMLModel,
    load_forensic_training_data_from_db,
)

_ALL_NAN_FEATURES = {f: np.nan for f in FORENSIC_ML_FEATURES}
_ALL_NAN_CLASSICAL = {
    "m_score": np.nan, "z_score": np.nan, "f_score": np.nan, "o_score": np.nan,
    "dechow_f_score": np.nan, "sloan_accrual": np.nan, "benford_mad": np.nan,
    "forensic_classical_composite": np.nan,
}


@pytest.fixture(scope="module")
def trained_model() -> ForensicMLModel:
    try:
        X, y = load_forensic_training_data_from_db()
    except RuntimeError as exc:
        pytest.skip(f"real forensic training data not yet available: {exc}")
    model = ForensicMLModel(random_state=1, n_estimators=30)
    model.train_full(X, y)
    return model


class TestNoneIfNan:
    def test_nan_becomes_none(self):
        assert _none_if_nan(np.nan) is None

    def test_real_value_passes_through(self):
        assert _none_if_nan(42.5) == 42.5


class TestScoreUniverse:
    def test_one_bad_ticker_does_not_abort_the_batch(self, trained_model):
        client = MagicMock()

        def fake_ml_features(client_, ticker, as_of):
            if ticker == "BADCO":
                raise ConnectionError("boom")
            return dict(_ALL_NAN_FEATURES)

        with patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_ml_features", fake_ml_features
        ), patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_classical_scores",
            return_value=_ALL_NAN_CLASSICAL,
        ):
            results = score_universe(["GOODCO", "BADCO"], client=client, model=trained_model, write=False)

        assert results == {"GOODCO": True, "BADCO": False}

    def test_write_false_skips_api_calls(self, trained_model):
        client = MagicMock()
        with patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_ml_features",
            return_value=dict(_ALL_NAN_FEATURES),
        ), patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_classical_scores",
            return_value=_ALL_NAN_CLASSICAL,
        ):
            results = score_universe(["GOODCO"], client=client, model=trained_model, write=False)

        assert results == {"GOODCO": True}
        client.write_forensic_score.assert_not_called()

    def test_write_true_calls_client_with_expected_fields(self, trained_model):
        client = MagicMock()
        classical = dict(_ALL_NAN_CLASSICAL)
        classical.update({
            "m_score": -2.0, "z_score": 1.5, "f_score": 4.0, "o_score": -1.0,
            "dechow_f_score": 0.1, "sloan_accrual": 0.05, "benford_mad": 0.01,
            "forensic_classical_composite": 30.0,
        })
        with patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_ml_features",
            return_value=dict(_ALL_NAN_FEATURES),
        ), patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_classical_scores",
            return_value=classical,
        ):
            score_universe(["GOODCO"], client=client, model=trained_model, write=True)

        client.write_forensic_score.assert_called_once()
        written = client.write_forensic_score.call_args[0][0]
        assert written["ticker"] == "GOODCO"
        assert written["beneish_m"] == -2.0
        assert written["forensic_flag_label"] in ("green", "yellow", "orange", "red", "black")
        assert isinstance(written["forensic_flag"], bool)
        assert 0.0 <= written["forensic_ml_prob"] <= 1.0

    def test_default_model_trains_when_none_injected(self):
        """score_universe() must work with no injected model (the real CLI path)."""
        client = MagicMock()
        with patch(
            "systems.ml_signal_engine.inference.score_forensic.load_forensic_training_data_from_db",
            return_value=(
                pd.DataFrame([_ALL_NAN_FEATURES] * 2)[FORENSIC_ML_FEATURES],
                pd.Series([1, 0]),
            ),
        ), patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_ml_features",
            return_value=dict(_ALL_NAN_FEATURES),
        ), patch(
            "systems.ml_signal_engine.inference.score_forensic.compute_forensic_classical_scores",
            return_value=_ALL_NAN_CLASSICAL,
        ):
            results = score_universe(["GOODCO"], client=client, write=False)

        assert results == {"GOODCO": True}
