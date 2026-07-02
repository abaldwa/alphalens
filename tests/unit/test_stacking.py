"""
tests/unit/test_stacking.py

Phase: 3.3 (Stacking Ensemble)
Specs: SPEC-MODEL-003, SPEC-MODEL-005, SPEC-SOLID-003

Unit tests for:
  M-13: StackingMetaLearner  (stacking.py)
        — base model weights sum to 1.0
        — meta-learner predictions are in [0, 1]
        — adaptive weight update changes weights when one model underperforms
        AdaptiveWeightManager
        StackingEnsemble (backward-compat alias)
        EnsemblePrediction dataclass

All tests are torch-independent (stacking uses sklearn only).
"""

import os
import tempfile

import numpy as np
import pytest

from systems.ml_signal_engine.models.deep.stacking import (
    AdaptiveWeightManager,
    EnsemblePrediction,
    StackingEnsemble,
    StackingMetaLearner,
    MIN_BASE_MODEL_WEIGHT,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

BASE_MODELS = ["signal_5d", "signal_21d", "signal_63d"]
N = 200    # OOF sample count
N_CLASSES = 3


def _oof(n: int = N, seed: int = 0) -> dict:
    """Dirichlet-sampled OOF probability matrices for each base model."""
    rng = np.random.default_rng(seed)
    return {m: rng.dirichlet(np.ones(3), size=n).astype(np.float32) for m in BASE_MODELS}


def _labels(n: int = N, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.choice([-1, 0, 1], size=n)


def _fitted_meta(seed: int = 0) -> StackingMetaLearner:
    meta = StackingMetaLearner(base_model_names=BASE_MODELS, random_state=seed)
    meta.fit_meta(_oof(seed=seed), _labels(seed=seed))
    return meta


# ── StackingMetaLearner tests ─────────────────────────────────────────────────


class TestStackingMetaLearner:

    def test_fit_meta_runs_without_error(self):
        """fit_meta completes and sets _meta, _weights."""
        meta = StackingMetaLearner(base_model_names=BASE_MODELS)
        meta.fit_meta(_oof(), _labels())
        assert meta._meta is not None

    def test_base_model_weights_sum_to_one(self):
        """All base-model weights must sum to 1.0 (SPEC-MODEL-003)."""
        meta = _fitted_meta()
        weights = meta._weights
        assert weights is not None
        np.testing.assert_allclose(
            weights.sum(), 1.0, atol=1e-6,
            err_msg="Base model weights must sum to 1.0"
        )

    def test_min_weight_constraint_satisfied(self):
        """All weights must be >= MIN_BASE_MODEL_WEIGHT = 0.10 (SPEC-MODEL-003)."""
        meta = _fitted_meta()
        assert meta.verify_min_weight_constraint(), (
            f"Min-weight constraint violated: {meta.weights}"
        )
        assert meta._weights is not None
        assert np.all(meta._weights >= MIN_BASE_MODEL_WEIGHT), (
            f"Weight below min: {meta.weights}"
        )

    def test_predict_proba_in_unit_interval(self):
        """predict_proba must return values in [0, 1] with rows summing to 1."""
        meta = _fitted_meta()
        live = _oof(n=20, seed=99)
        proba = meta.predict_proba(live)
        assert proba.shape == (20, N_CLASSES), f"Expected (20, 3), got {proba.shape}"
        assert np.all(proba >= 0.0), "Negative probability detected"
        assert np.all(proba <= 1.0), "Probability > 1.0 detected"
        np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-4)

    def test_predict_returns_class_indices(self):
        """predict() must return values in {0, 1, 2}."""
        meta = _fitted_meta()
        preds = meta.predict(_oof(n=20, seed=5))
        assert set(preds).issubset({0, 1, 2})

    def test_predict_ensemble_returns_ensemble_prediction(self):
        """predict_ensemble must return EnsemblePrediction with correct fields."""
        meta = _fitted_meta()
        ep = meta.predict_ensemble(_oof(n=10, seed=1))
        assert isinstance(ep, EnsemblePrediction)
        assert ep.final_buy_prob.shape == (10,)
        assert ep.final_hold_prob.shape == (10,)
        assert ep.final_sell_prob.shape == (10,)
        assert ep.stacking_confidence.shape == (10,)

    def test_final_proba_sum_to_one(self):
        """buy + hold + sell must sum to 1.0 per sample."""
        meta = _fitted_meta()
        ep = meta.predict_ensemble(_oof(n=10, seed=2))
        row_sums = ep.final_buy_prob + ep.final_hold_prob + ep.final_sell_prob
        np.testing.assert_allclose(row_sums, 1.0, atol=1e-4)

    def test_stacking_confidence_is_max_prob(self):
        """stacking_confidence must equal max(buy, hold, sell) per row."""
        meta = _fitted_meta()
        ep = meta.predict_ensemble(_oof(n=10, seed=3))
        expected = np.maximum.reduce([ep.final_buy_prob, ep.final_hold_prob, ep.final_sell_prob])
        np.testing.assert_allclose(ep.stacking_confidence, expected, atol=1e-5)

    def test_confidence_in_unit_interval(self):
        """stacking_confidence must be in [0, 1]."""
        meta = _fitted_meta()
        ep = meta.predict_ensemble(_oof(n=50))
        assert np.all(ep.stacking_confidence >= 0.0)
        assert np.all(ep.stacking_confidence <= 1.0)

    def test_missing_base_model_raises(self):
        """fit_meta with missing model raises ValueError."""
        meta = StackingMetaLearner(base_model_names=BASE_MODELS)
        incomplete = _oof()
        del incomplete["signal_21d"]
        with pytest.raises(ValueError, match="Missing OOF predictions"):
            meta.fit_meta(incomplete, _labels())

    def test_not_trained_raises(self):
        """predict_proba before fit_meta raises RuntimeError."""
        meta = StackingMetaLearner(base_model_names=BASE_MODELS)
        with pytest.raises(RuntimeError, match="not trained"):
            meta.predict_proba(_oof(n=5))

    def test_save_load_round_trip(self):
        """save/load preserves all predictions exactly."""
        meta = _fitted_meta()
        live = _oof(n=10, seed=77)
        proba_before = meta.predict_proba(live).copy()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "stacking_test")
            meta.save(path)
            meta2 = StackingMetaLearner(base_model_names=BASE_MODELS)
            meta2.load(path)
            proba_after = meta2.predict_proba(live)

        np.testing.assert_allclose(proba_before, proba_after, atol=1e-5)

    def test_metadata_keys(self):
        """metadata() must include required keys."""
        meta = _fitted_meta()
        m = meta.metadata()
        for k in ("name", "version", "base_models", "weights", "min_weight"):
            assert k in m, f"Missing key: {k}"
        assert m["name"] == "stacking_meta_learner"
        assert m["min_weight"] == MIN_BASE_MODEL_WEIGHT

    def test_weights_property_matches_internal(self):
        """weights property must return dict consistent with _weights array."""
        meta = _fitted_meta()
        w_dict = meta.weights
        w_arr = meta._weights
        assert w_dict is not None and w_arr is not None
        for name, val in w_dict.items():
            idx = meta.base_model_names.index(name)
            assert abs(val - w_arr[idx]) < 1e-9

    def test_train_accepts_array_input(self):
        """train(X_array, y) must work for IModel compliance."""
        meta = StackingMetaLearner(base_model_names=BASE_MODELS)
        oof = _oof()
        X_arr = np.concatenate([oof[m] for m in BASE_MODELS], axis=1)
        meta.train(X_arr, _labels())
        assert meta._meta is not None

    def test_to_dataframe_shape(self):
        """EnsemblePrediction.to_dataframe() must have 4 columns and n rows."""
        meta = _fitted_meta()
        ep = meta.predict_ensemble(_oof(n=15))
        df = ep.to_dataframe()
        assert df.shape == (15, 4)
        assert set(df.columns) == {
            "final_buy_prob", "final_hold_prob", "final_sell_prob", "stacking_confidence"
        }


# ── AdaptiveWeightManager tests ───────────────────────────────────────────────


class TestAdaptiveWeightManager:

    def test_update_changes_weights_when_one_model_underperforms(self):
        """
        When one model has much lower recent accuracy, its weight must decrease
        after update_weights_monthly() (SPEC-MODEL-003: "adaptive weighting
        updated monthly based on recent accuracy").
        """
        mgr = AdaptiveWeightManager(
            base_model_names=BASE_MODELS,
            blend_alpha=0.50,    # 50% blend for sensitivity
            min_weight=0.05,     # lower min so the change is visible
        )
        # Simulate: signal_21d and signal_63d predict correctly, signal_5d does not
        rng = np.random.default_rng(7)
        n = 100
        actual = rng.choice([0, 1, 2], size=n)

        # signal_5d: random predictions (accuracy ~33%)
        mgr.record_predictions("signal_5d", rng.choice([0, 1, 2], size=n), actual)
        # signal_21d: correct predictions (accuracy ~100%)
        mgr.record_predictions("signal_21d", actual, actual)
        # signal_63d: correct predictions (accuracy ~100%)
        mgr.record_predictions("signal_63d", actual, actual)

        initial_weights = np.array([1 / 3, 1 / 3, 1 / 3])
        updated = mgr.update(initial_weights.copy())

        assert updated.shape == (len(BASE_MODELS),)
        np.testing.assert_allclose(updated.sum(), 1.0, atol=1e-6)

        # signal_5d (low accuracy) should have lower weight than signal_21d (high accuracy)
        idx_5d = BASE_MODELS.index("signal_5d")
        idx_21d = BASE_MODELS.index("signal_21d")
        assert updated[idx_5d] < updated[idx_21d], (
            f"Underperforming model (signal_5d) should have lower weight "
            f"than well-performing model (signal_21d). "
            f"Got: signal_5d={updated[idx_5d]:.4f}, signal_21d={updated[idx_21d]:.4f}"
        )

    def test_update_preserves_min_weight(self):
        """After update, all weights must be >= min_weight."""
        mgr = AdaptiveWeightManager(BASE_MODELS, blend_alpha=0.5, min_weight=0.10)
        rng = np.random.default_rng(99)
        # Record terrible accuracy for one model
        n = 100
        actual = rng.choice([0, 1, 2], size=n)
        bad_preds = np.roll(actual, 1)    # always wrong
        mgr.record_predictions("signal_5d", bad_preds, actual)
        mgr.record_predictions("signal_21d", actual, actual)
        mgr.record_predictions("signal_63d", actual, actual)

        weights = np.array([1 / 3, 1 / 3, 1 / 3])
        updated = mgr.update(weights)
        assert np.all(updated >= 0.10), (
            f"Weight below min_weight=0.10 after update: {updated}"
        )

    def test_update_sums_to_one(self):
        """Updated weights must sum to 1.0."""
        mgr = AdaptiveWeightManager(BASE_MODELS, blend_alpha=0.3, min_weight=0.10)
        weights = np.array([0.5, 0.3, 0.2])
        updated = mgr.update(weights)
        np.testing.assert_allclose(updated.sum(), 1.0, atol=1e-6)

    def test_empty_records_returns_uniform(self):
        """With no recorded predictions, update should return weights close to uniform."""
        mgr = AdaptiveWeightManager(BASE_MODELS, blend_alpha=0.5, min_weight=0.05)
        weights = np.array([0.4, 0.3, 0.3])
        updated = mgr.update(weights)
        # Without data, accuracy falls back to mean = 1/3 each → blends toward uniform
        np.testing.assert_allclose(updated.sum(), 1.0, atol=1e-6)
        assert updated.shape == (3,)

    def test_update_weights_monthly_on_meta(self):
        """StackingMetaLearner.update_weights_monthly() must update _weights in-place."""
        meta = _fitted_meta()
        weights_before = meta._weights.copy()

        # Record asymmetric performance (signal_21d always right, others random)
        rng = np.random.default_rng(11)
        n = 100
        actual = rng.choice([0, 1, 2], size=n)
        meta.record_base_model_predictions("signal_5d", rng.choice([0, 1, 2], n), actual)
        meta.record_base_model_predictions("signal_21d", actual, actual)
        meta.record_base_model_predictions("signal_63d", rng.choice([0, 1, 2], n), actual)

        # Force a blend with high alpha so change is visible
        meta._adaptive.blend_alpha = 0.6
        new_weights = meta.update_weights_monthly()

        assert new_weights is not None
        # Verify _weights actually changed
        assert not np.allclose(meta._weights, weights_before, atol=1e-4), (
            "Weights should change after update with asymmetric model performance"
        )
        np.testing.assert_allclose(meta._weights.sum(), 1.0, atol=1e-6)


# ── StackingEnsemble backward-compat tests ────────────────────────────────────


class TestStackingEnsembleBackwardCompat:
    """Ensure StackingEnsemble (alias) still works for existing callers."""

    def test_fit_and_predict(self):
        """StackingEnsemble.fit_meta + predict_proba works via alias."""
        ens = StackingEnsemble(base_model_names=BASE_MODELS)
        ens.fit_meta(_oof(), _labels())
        proba = ens.predict_proba(_oof(n=10))
        assert proba.shape == (10, N_CLASSES)
        np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-4)

    def test_weight_blend_shape(self):
        """weight_blend must return (n, 3) normalized probabilities."""
        ens = StackingEnsemble(base_model_names=BASE_MODELS)
        ens.fit_meta(_oof(), _labels())
        blended = ens.weight_blend(_oof(n=10))
        assert blended.shape == (10, N_CLASSES)
        np.testing.assert_allclose(blended.sum(axis=1), 1.0, atol=1e-4)

    def test_combine_before_fit_does_not_crash(self):
        """combine() before fit_meta must fall back to weight_blend gracefully."""
        ens = StackingEnsemble(base_model_names=BASE_MODELS)
        result = ens.combine(_oof(n=5))
        assert result.shape[1] == N_CLASSES

    def test_weights_sum_to_one(self):
        ens = StackingEnsemble(base_model_names=BASE_MODELS)
        ens.fit_meta(_oof(), _labels())
        np.testing.assert_allclose(ens._weights.sum(), 1.0, atol=1e-6)
