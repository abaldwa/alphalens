"""
tests/unit/test_stacking_ensemble_wiring.py

2026-07-19 full-codebase-review Fix A3: verifies the previously dead-code
StackingMetaLearner is actually invoked and its output written when a
trained artifact is present, and is cleanly skipped (no crash, signal_5d/
meta_labeler still written) when it's absent — the common case.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

import systems.ml_signal_engine.inference.daily_inference as di
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.models.deep.stacking import StackingMetaLearner
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel

TICKERS = [f"TKR{i:03d}" for i in range(12)]
RUN_DATE = date(2026, 7, 19)


class _FakeResponse:
    status_code = 200

    def raise_for_status(self):
        pass


class _RecordingClient:
    def __init__(self):
        self.calls = []

    def post(self, url, json, timeout=None):  # noqa: A002
        self.calls.append(dict(json))
        return _FakeResponse()


def _train_small_signal_model(cls, seed, n=200, **kwargs):
    rng = np.random.default_rng(seed)
    X = pd.DataFrame(rng.normal(size=(n, len(CORE_TECHNICAL_FEATURES))), columns=CORE_TECHNICAL_FEATURES)
    score = X.iloc[:, 0] - 0.5 * X.iloc[:, 1] + rng.normal(scale=0.5, size=n)
    y = pd.Series(np.where(score > 0.5, 1, np.where(score < -0.5, -1, 0)))
    returns = pd.Series(score * 0.02 + rng.normal(scale=0.01, size=n))
    model = cls(optuna_trials=2, random_state=seed, **kwargs)
    model.train_full(
        X.iloc[: n // 2], y.iloc[: n // 2], X.iloc[n // 2:], y.iloc[n // 2:],
        returns_train=returns.iloc[: n // 2], returns_val=returns.iloc[n // 2:],
    )
    return model, X


@pytest.fixture(scope="module")
def trained_models():
    signal_5d, X = _train_small_signal_model(Signal5DModel, seed=21)
    signal_21d, _ = _train_small_signal_model(Signal21DModel, seed=22)
    signal_63d, _ = _train_small_signal_model(Signal63DModel, seed=23)

    direction = signal_5d.predict(X)
    returns = pd.Series(np.random.default_rng(24).normal(scale=0.01, size=len(X)))
    meta_labels = MetaLabeler.compute_labels(direction, returns)
    mask = meta_labels.notna()
    meta_model = MetaLabeler(random_state=21)
    meta_model.train(X[mask], meta_labels[mask])

    return signal_5d, signal_21d, signal_63d, meta_model


@pytest.fixture
def feature_matrix():
    rng = np.random.default_rng(31)
    rows = []
    for t in TICKERS:
        row = {"ticker": t}
        row.update({f: rng.normal() for f in CORE_TECHNICAL_FEATURES})
        rows.append(row)
    return pd.DataFrame(rows)


def _patch_common(monkeypatch, signal_5d, signal_21d, signal_63d, meta_model):
    monkeypatch.setattr(di, "_load_model", lambda cls, name, models_dir: {
        di.SIGNAL_MODEL_NAME: signal_5d, di.META_MODEL_NAME: meta_model,
        di.SIGNAL_21D_MODEL_NAME: signal_21d, di.SIGNAL_63D_MODEL_NAME: signal_63d,
    }[name])
    monkeypatch.setattr(di, "_load_conformal", lambda models_dir: (_ for _ in ()).throw(FileNotFoundError()))
    monkeypatch.setattr("config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE", 1000)


class TestEnsembleWiredWhenArtifactPresent:
    def test_stacking_ensemble_row_written_for_every_ticker(
        self, trained_models, feature_matrix, tmp_path, monkeypatch
    ):
        signal_5d, signal_21d, signal_63d, meta_model = trained_models
        _patch_common(monkeypatch, signal_5d, signal_21d, signal_63d, meta_model)

        # Real StackingMetaLearner, fit on synthetic-but-real-shaped OOF
        # data (unit-test-only synthetic input, not a claim of real OOF
        # provenance — mirrors tests/unit/test_stacking.py's own fixture
        # style for exercising fit_meta/predict_ensemble in isolation).
        rng = np.random.default_rng(41)
        n = 100
        oof = {
            name: np.array([rng.dirichlet(np.ones(3)) for _ in range(n)])
            for name in di.STACKING_ENSEMBLE_BASE_MODELS
        }
        y_oof = rng.choice([-1, 0, 1], size=n)
        ensemble = StackingMetaLearner(base_model_names=di.STACKING_ENSEMBLE_BASE_MODELS)
        ensemble.fit_meta(oof, y_oof)
        monkeypatch.setattr(di, "_load_stacking_ensemble", lambda models_dir: ensemble)

        client = _RecordingClient()
        di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)

        ensemble_rows = [c for c in client.calls if c["model_name"] == di.STACKING_ENSEMBLE_MODEL_NAME]
        assert {r["ticker"] for r in ensemble_rows} == set(TICKERS)
        for r in ensemble_rows:
            assert r["buy_prob"] + r["hold_prob"] + r["sell_prob"] == pytest.approx(1.0, abs=1e-5)
            assert r["signal_direction"] in ("buy", "hold", "sell")

        # signal_5d/meta_labeler must still be written regardless.
        assert any(c["model_name"] == di.SIGNAL_MODEL_NAME for c in client.calls)
        assert any(c["model_name"] == di.META_MODEL_NAME for c in client.calls)


class TestEnsembleSkippedWhenArtifactAbsent:
    def test_no_artifact_no_crash_no_ensemble_row(
        self, trained_models, feature_matrix, tmp_path, monkeypatch
    ):
        signal_5d, signal_21d, signal_63d, meta_model = trained_models
        _patch_common(monkeypatch, signal_5d, signal_21d, signal_63d, meta_model)
        monkeypatch.setattr(di, "_load_stacking_ensemble", lambda models_dir: None)

        client = _RecordingClient()
        result = di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)

        assert len(result) == len(TICKERS)
        assert not any(c["model_name"] == di.STACKING_ENSEMBLE_MODEL_NAME for c in client.calls)
        assert any(c["model_name"] == di.SIGNAL_MODEL_NAME for c in client.calls)

    def test_ensemble_load_exception_no_crash(
        self, trained_models, feature_matrix, tmp_path, monkeypatch
    ):
        signal_5d, signal_21d, signal_63d, meta_model = trained_models
        _patch_common(monkeypatch, signal_5d, signal_21d, signal_63d, meta_model)

        def _raise(models_dir):
            raise FileNotFoundError("no stacking artifact")

        monkeypatch.setattr(di, "_load_stacking_ensemble", _raise)

        client = _RecordingClient()
        result = di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)

        assert len(result) == len(TICKERS)
        assert not any(c["model_name"] == di.STACKING_ENSEMBLE_MODEL_NAME for c in client.calls)

    def test_mismatched_artifact_base_models_skips_gracefully(
        self, trained_models, feature_matrix, tmp_path, monkeypatch
    ):
        """An artifact trained on the full 5-model set (tft/bilstm
        included) must not crash inference when only 3 base models are
        available — predict_ensemble's KeyError on missing tft/bilstm
        keys is caught, logged, and skipped."""
        signal_5d, signal_21d, signal_63d, meta_model = trained_models
        _patch_common(monkeypatch, signal_5d, signal_21d, signal_63d, meta_model)

        rng = np.random.default_rng(43)
        n = 60
        full_base_models = ["signal_5d", "signal_21d", "signal_63d", "tft", "bilstm"]
        oof = {name: np.array([rng.dirichlet(np.ones(3)) for _ in range(n)]) for name in full_base_models}
        y_oof = rng.choice([-1, 0, 1], size=n)
        mismatched_ensemble = StackingMetaLearner(base_model_names=full_base_models)
        mismatched_ensemble.fit_meta(oof, y_oof)
        monkeypatch.setattr(di, "_load_stacking_ensemble", lambda models_dir: mismatched_ensemble)

        client = _RecordingClient()
        result = di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)

        assert len(result) == len(TICKERS)
        assert not any(c["model_name"] == di.STACKING_ENSEMBLE_MODEL_NAME for c in client.calls)
        assert any(c["model_name"] == di.SIGNAL_MODEL_NAME for c in client.calls)
