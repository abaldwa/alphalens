"""
systems/ml_signal_engine/models/deep/stacking.py

Phase: 3.3 (Stacking Ensemble + Adaptive Weighting)
Specs: SPEC-MODEL-003, SPEC-MODEL-005, SPEC-SOLID-003
Owner: ml_signal_engine / deep
Consumers: systems/ml_signal_engine/inference/daily_inference.py,
           backtest/run_phase3_backtest.py

M-13: Stacking Ensemble Meta-Learner with adaptive monthly weight updates.

Base models combined:
  Signal5D  (LGB + CatBoost + XGB internal stack, 5d horizon)
  Signal21D (LGB + CatBoost + XGB internal stack, 21d horizon)
  Signal63D (LGB + CatBoost + XGB internal stack, 63d horizon)
  TFT       (Temporal Fusion Transformer, M-11)
  BiLSTM    (Bidirectional LSTM + Mamba-2, M-12)

Meta-learner: LogisticRegression on out-of-fold (OOF) predictions.
  CRITICAL: trained ONLY on OOF predictions — never on full training data.
  Minimum weight per base model = 0.10. Any weight < 0.10 triggers
  a WARNING log message (SPEC-MODEL-003).

Adaptive weighting (monthly):
  Each month, the meta-learner's coefficient-derived weights are blended
  with a recent-accuracy-based weight via `AdaptiveWeightManager.update()`.
  Weights are re-normalized after blending, with minimum weight enforced.

Output contract:
  `predict_ensemble()` returns `EnsemblePrediction`:
    final_buy_prob  : float in [0, 1]
    final_hold_prob : float in [0, 1]
    final_sell_prob : float in [0, 1]
    stacking_confidence : float — max class probability (proxy for certainty)
    base_model_weights  : Dict[str, float]

PIT Assumptions
---------------
OOF predictions are built by callers (walk_forward.py) using only
past-fold data. This class never touches raw feature data or prices.
"""

import json
import logging
import pickle
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from contracts.interfaces import IClassificationModel

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MIN_BASE_MODEL_WEIGHT: float = 0.10
DEFAULT_BASE_MODELS: List[str] = ["signal_5d", "signal_21d", "signal_63d", "tft", "bilstm"]
CLASS_NAMES: List[str] = ["sell", "hold", "buy"]
N_CLASSES: int = 3

# Blend ratio for adaptive update: 70% existing weights, 30% recent accuracy
_ADAPTIVE_BLEND_ALPHA: float = 0.30
# Minimum samples required to compute a meaningful recent accuracy estimate
_MIN_ACCURACY_SAMPLES: int = 20


# ── Output dataclass ──────────────────────────────────────────────────────────


@dataclass
class EnsemblePrediction:
    """
    Structured output from StackingMetaLearner.predict_ensemble().

    final_buy_prob + final_hold_prob + final_sell_prob = 1.0 per row.
    stacking_confidence = max(buy, hold, sell) per row.
    """

    final_buy_prob: np.ndarray   # shape (n_samples,)
    final_hold_prob: np.ndarray  # shape (n_samples,)
    final_sell_prob: np.ndarray  # shape (n_samples,)
    stacking_confidence: np.ndarray  # shape (n_samples,)
    base_model_weights: Dict[str, float]

    def to_dataframe(self) -> pd.DataFrame:
        """Return wide-format DataFrame with one row per sample."""
        return pd.DataFrame({
            "final_buy_prob": self.final_buy_prob,
            "final_hold_prob": self.final_hold_prob,
            "final_sell_prob": self.final_sell_prob,
            "stacking_confidence": self.stacking_confidence,
        })

    def predict_class(self) -> np.ndarray:
        """Return argmax class index: 0=Sell, 1=Hold, 2=Buy."""
        return np.stack([self.final_sell_prob, self.final_hold_prob, self.final_buy_prob], axis=1).argmax(axis=1)


# ── Adaptive weight manager ───────────────────────────────────────────────────


class AdaptiveWeightManager:
    """
    Tracks each base model's recent prediction accuracy and blends it with
    the coefficient-derived weights to produce an updated weight vector.

    Monthly update cadence (SPEC-MODEL-003: "base model weights updated
    monthly based on recent accuracy").

    Accuracy metric: macro-averaged per-class accuracy (direction correct
    = predicted class matches actual class).
    """

    def __init__(
        self,
        base_model_names: List[str],
        blend_alpha: float = _ADAPTIVE_BLEND_ALPHA,
        min_weight: float = MIN_BASE_MODEL_WEIGHT,
    ) -> None:
        self.base_model_names = base_model_names
        self.blend_alpha = blend_alpha
        self.min_weight = min_weight
        # Ring buffer: (model_name, predicted_class, actual_class)
        self._recent_records: List[Tuple[str, int, int]] = []
        self._last_update: Optional[datetime] = None

    def record_predictions(
        self,
        model_name: str,
        predicted_classes: np.ndarray,
        actual_classes: np.ndarray,
    ) -> None:
        """
        Append recent predictions for one base model (call after each batch).

        Parameters
        ----------
        model_name : str — must be in base_model_names
        predicted_classes : ndarray (n,) — int {0=Sell, 1=Hold, 2=Buy}
        actual_classes : ndarray (n,) — int {0=Sell, 1=Hold, 2=Buy}
        """
        for p, a in zip(predicted_classes, actual_classes):
            self._recent_records.append((model_name, int(p), int(a)))
        # Cap buffer at last 504 samples per model (2 trading years)
        max_buf = 504 * len(self.base_model_names)
        if len(self._recent_records) > max_buf:
            self._recent_records = self._recent_records[-max_buf:]

    def compute_recent_accuracy(self) -> Dict[str, float]:
        """
        Compute recent directional accuracy for each base model.

        Returns
        -------
        Dict[str, float] — accuracy per model (0–1). Models with fewer
        than _MIN_ACCURACY_SAMPLES records receive the mean accuracy.
        """
        counts: Dict[str, int] = {m: 0 for m in self.base_model_names}
        correct: Dict[str, int] = {m: 0 for m in self.base_model_names}
        for name, pred, actual in self._recent_records:
            if name in counts:
                counts[name] += 1
                correct[name] += int(pred == actual)

        mean_acc = (
            sum(correct.values()) / max(sum(counts.values()), 1)
        )
        accs = {}
        for m in self.base_model_names:
            if counts[m] >= _MIN_ACCURACY_SAMPLES:
                accs[m] = correct[m] / counts[m]
            else:
                accs[m] = mean_acc
        return accs

    def update(
        self,
        current_weights: np.ndarray,
    ) -> np.ndarray:
        """
        Blend current weights with recent accuracy-derived weights.

        Parameters
        ----------
        current_weights : ndarray (n_models,) — existing normalized weights

        Returns
        -------
        ndarray (n_models,) — updated normalized weights with min_weight enforced
        """
        accs = self.compute_recent_accuracy()
        acc_vec = np.array([accs[m] for m in self.base_model_names], dtype=np.float64)
        # Normalize accuracy to sum to 1
        acc_sum = acc_vec.sum()
        if acc_sum < 1e-9:
            acc_weights = np.ones(len(self.base_model_names)) / len(self.base_model_names)
        else:
            acc_weights = acc_vec / acc_sum

        # Blend: (1 - alpha) * current + alpha * accuracy-derived
        blended = (1.0 - self.blend_alpha) * current_weights + self.blend_alpha * acc_weights
        # Enforce minimum weight
        blended = np.maximum(blended, self.min_weight)
        blended /= blended.sum()

        # Warn on any model that had to be clipped up to min_weight
        for i, (m, w_before, w_after) in enumerate(
            zip(self.base_model_names, current_weights, blended)
        ):
            if w_before < self.min_weight:
                logger.warning(
                    "AdaptiveWeightManager: model '%s' weight was %.4f < min_weight=%.2f "
                    "— clipped to minimum. This may indicate model underperformance. "
                    "Recent accuracy: %.3f",
                    m, w_before, self.min_weight, accs[m],
                )
        self._last_update = datetime.utcnow()
        return blended


# ── StackingMetaLearner ───────────────────────────────────────────────────────


class StackingMetaLearner(IClassificationModel):
    """
    M-13: Stacking ensemble meta-learner combining all Phase 3 base models.

    Training contract
    -----------------
    Call `fit_meta(oof_predictions, y_oof)` to train the meta-learner on
    out-of-fold predictions. The caller (walk_forward.py) is responsible for
    constructing OOF predictions that do NOT include the test fold.

    Inference contract
    ------------------
    Call `predict_ensemble(base_predictions)` to get `EnsemblePrediction`
    with final_buy_prob, final_hold_prob, final_sell_prob, confidence, weights.

    Adaptive weighting
    ------------------
    Call `record_base_model_predictions()` to log recent predictions for
    each base model. Call `update_weights_monthly()` once per calendar month
    to blend recent-accuracy-derived weights into the coefficient weights.

    Minimum weight enforcement
    --------------------------
    Any base-model weight < 0.10 triggers WARNING in the log.
    The minimum is enforced after every weight update operation.

    Spec References
    ---------------
    SPEC-MODEL-003: OOF-only training; minimum weight; monthly adaptive update.
    SPEC-MODEL-005: versioned save/load (saves .pkl + .json).
    SPEC-SOLID-003: IClassificationModel implementation.
    """

    MODEL_NAME = "stacking_meta_learner"

    def __init__(
        self,
        base_model_names: Optional[List[str]] = None,
        C: float = 1.0,
        min_weight: float = MIN_BASE_MODEL_WEIGHT,
        blend_alpha: float = _ADAPTIVE_BLEND_ALPHA,
        random_state: int = 42,
    ) -> None:
        self.base_model_names: List[str] = base_model_names or DEFAULT_BASE_MODELS
        self.C = C
        self.min_weight = min_weight
        self.random_state = random_state

        self._meta: Optional[LogisticRegression] = None
        self._scaler: StandardScaler = StandardScaler()
        self._weights: Optional[np.ndarray] = None
        self._training_samples: int = 0
        self._created_at: Optional[str] = None
        self._version: str = datetime.utcnow().strftime("%Y%m%d")

        self._adaptive = AdaptiveWeightManager(
            self.base_model_names, blend_alpha=blend_alpha, min_weight=min_weight
        )

    # ── OOF training ──────────────────────────────────────────────────────────

    def fit_meta(
        self,
        oof_predictions: Dict[str, np.ndarray],
        y_oof: np.ndarray,
    ) -> None:
        """
        Train LogisticRegression meta-learner on OOF predictions.

        Parameters
        ----------
        oof_predictions : dict {model_name: ndarray (n_samples, 3)}
            OOF probability matrices. Keys must match base_model_names.
            Values are P(Sell)/P(Hold)/P(Buy) per row.
        y_oof : ndarray (n_samples,)
            True class labels: -1=Sell, 0=Hold, 1=Buy.

        Raises
        ------
        ValueError  If any base model is missing from oof_predictions.

        Spec References
        ---------------
        SPEC-MODEL-003: "Train on OUT-OF-FOLD predictions only — never on
        full training data."
        """
        missing = [m for m in self.base_model_names if m not in oof_predictions]
        if missing:
            raise ValueError(f"Missing OOF predictions for: {missing}")

        X_meta = self._build_meta_input(oof_predictions)
        y_remapped = self._remap_labels(y_oof)

        X_scaled = self._scaler.fit_transform(X_meta)
        self._training_samples = len(y_remapped)

        self._meta = LogisticRegression(
            C=self.C,
            max_iter=1000,
            solver="lbfgs",
            random_state=self.random_state,
        )
        self._meta.fit(X_scaled, y_remapped)
        self._extract_weights()
        self._created_at = datetime.utcnow().isoformat()

        logger.info(
            "StackingMetaLearner trained on %d OOF samples. Weights: %s",
            self._training_samples,
            {k: round(v, 3) for k, v in (self.weights or {}).items()},
        )

    def _build_meta_input(self, preds: Dict[str, np.ndarray]) -> np.ndarray:
        """Stack model probability matrices: (n, n_models * 3)."""
        return np.concatenate([preds[m] for m in self.base_model_names], axis=1)

    def _extract_weights(self) -> None:
        """
        Derive per-model weights from meta-learner coefficients.

        Coefficient matrix is (n_classes, n_features). Mean |coefficient|
        per base model (each contributes N_CLASSES columns). Min-weight
        enforced before normalization. WARNING logged for violations.
        """
        if self._meta is None or not hasattr(self._meta, "coef_"):
            return
        coef = np.abs(self._meta.coef_)       # (n_classes, n_models * n_classes)
        n_models = len(self.base_model_names)
        model_weights = np.array([
            coef[:, i * N_CLASSES: (i + 1) * N_CLASSES].mean()
            for i in range(n_models)
        ])
        for i, (name, w) in enumerate(zip(self.base_model_names, model_weights)):
            if w < self.min_weight:
                logger.warning(
                    "StackingMetaLearner: base model '%s' derived weight=%.4f < "
                    "min_weight=%.2f — clipping to minimum (SPEC-MODEL-003).",
                    name, w, self.min_weight,
                )
        model_weights = np.maximum(model_weights, self.min_weight)
        model_weights /= model_weights.sum()
        self._weights = model_weights

    # ── Inference ─────────────────────────────────────────────────────────────

    def predict_ensemble(
        self, base_predictions: Dict[str, np.ndarray]
    ) -> EnsemblePrediction:
        """
        Combine base-model predictions into a structured ensemble output.

        Parameters
        ----------
        base_predictions : dict {model_name: ndarray (n_samples, 3)}
            Live P(Sell)/P(Hold)/P(Buy) from each base model.

        Returns
        -------
        EnsemblePrediction
            final_buy_prob, final_hold_prob, final_sell_prob (sum to 1),
            stacking_confidence (max probability per sample),
            base_model_weights (current weights dict).
        """
        proba = self.predict_proba(base_predictions)        # (n, 3) — P(Sell), P(Hold), P(Buy)
        return EnsemblePrediction(
            final_sell_prob=proba[:, 0],
            final_hold_prob=proba[:, 1],
            final_buy_prob=proba[:, 2],
            stacking_confidence=proba.max(axis=1),
            base_model_weights=self.weights or {},
        )

    def predict_proba(self, X: Any) -> np.ndarray:
        """
        Return (n_samples, 3) class probabilities [P(Sell), P(Hold), P(Buy)].

        Accepts:
          - dict {model_name: ndarray (n, 3)} — live base-model predictions
          - ndarray (n, n_models * 3) — pre-stacked input
        """
        if self._meta is None:
            raise RuntimeError("Meta-learner not trained. Call fit_meta() first.")
        if isinstance(X, dict):
            X_arr = self._build_meta_input(X)
        else:
            X_arr = np.asarray(X, dtype=np.float32)
        X_scaled = self._scaler.transform(X_arr)
        return self._meta.predict_proba(X_scaled).astype(np.float32)

    def predict(self, X: Any) -> np.ndarray:
        """Return argmax class index: 0=Sell, 1=Hold, 2=Buy."""
        return self.predict_proba(X).argmax(axis=1)

    # ── Adaptive weight update ────────────────────────────────────────────────

    def record_base_model_predictions(
        self,
        model_name: str,
        predicted_classes: np.ndarray,
        actual_classes: np.ndarray,
    ) -> None:
        """
        Log recent base-model predictions for accuracy tracking.

        Call this during live inference after outcomes are known (i.e., after
        `horizon_days` have elapsed). Used by `update_weights_monthly()`.

        Parameters
        ----------
        model_name : str — base model identifier
        predicted_classes : ndarray (n,) — int {0=Sell, 1=Hold, 2=Buy}
        actual_classes : ndarray (n,) — realized outcome labels
        """
        self._adaptive.record_predictions(model_name, predicted_classes, actual_classes)

    def update_weights_monthly(self) -> Dict[str, float]:
        """
        Blend current coefficient weights with recent accuracy-derived weights.

        Call once per calendar month in the daily pipeline (SPEC-MODEL-003:
        "Adaptive weighting: base model weights updated monthly based on
        recent accuracy").

        Returns
        -------
        Dict[str, float] — updated weights after monthly blend.
        """
        if self._weights is None:
            logger.warning("update_weights_monthly called before fit_meta — no weights to update.")
            return {}
        self._weights = self._adaptive.update(self._weights)
        logger.info(
            "Monthly weight update complete. New weights: %s",
            {k: round(v, 3) for k, v in zip(self.base_model_names, self._weights)},
        )
        return dict(zip(self.base_model_names, self._weights.tolist()))

    # ── Integrity check ───────────────────────────────────────────────────────

    def verify_min_weight_constraint(self) -> bool:
        """
        Return True if all base-model weights >= min_weight.

        WARNING logged per violating model (SPEC-MODEL-003).
        """
        if self._weights is None:
            return True
        ok = True
        for name, w in zip(self.base_model_names, self._weights):
            if w < self.min_weight:
                logger.warning(
                    "Min-weight violation: model '%s' weight=%.4f < %.2f",
                    name, w, self.min_weight,
                )
                ok = False
        return ok

    # ── IModel interface ──────────────────────────────────────────────────────

    def train(self, X: Any, y: Any, sample_weight: Optional[np.ndarray] = None) -> None:
        """IModel compatibility wrapper. X may be dict or ndarray."""
        if isinstance(X, dict):
            self.fit_meta(X, y)
        else:
            X_arr = np.asarray(X, dtype=np.float32)
            oof: Dict[str, np.ndarray] = {}
            for i, name in enumerate(self.base_model_names):
                oof[name] = X_arr[:, i * N_CLASSES: (i + 1) * N_CLASSES]
            self.fit_meta(oof, y)

    def save(self, path: str) -> None:
        """Save meta-learner, scaler, weights, and adaptive manager (SPEC-MODEL-005)."""
        if self._meta is None:
            raise RuntimeError("Nothing to save — not trained yet.")
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "meta": self._meta,
            "scaler": self._scaler,
            "weights": self._weights,
            "adaptive": self._adaptive,
            "base_model_names": self.base_model_names,
        }
        with open(str(p) + ".pkl", "wb") as f:
            pickle.dump(payload, f)
        with open(str(p) + ".json", "w") as f:
            json.dump(self.metadata(), f, indent=2, default=str)
        logger.info("StackingMetaLearner saved to %s.pkl", p)

    def load(self, path: str) -> None:
        """Load meta-learner, scaler, weights, and adaptive manager."""
        p = Path(path)
        with open(str(p) + ".pkl", "rb") as f:
            payload = pickle.load(f)
        self._meta = payload["meta"]
        self._scaler = payload["scaler"]
        self._weights = payload.get("weights")
        self._adaptive = payload.get("adaptive", AdaptiveWeightManager(self.base_model_names))
        self.base_model_names = payload.get("base_model_names", self.base_model_names)
        logger.info("StackingMetaLearner loaded from %s.pkl", p)

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": self.MODEL_NAME,
            "version": self._version,
            "created_at": self._created_at,
            "training_samples": self._training_samples,
            "base_models": self.base_model_names,
            "weights": self.weights,
            "min_weight": self.min_weight,
            "adaptive_blend_alpha": self._adaptive.blend_alpha,
            "last_adaptive_update": str(self._adaptive._last_update),
            "hyperparams": {"C": self.C, "random_state": self.random_state},
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _remap_labels(y: np.ndarray) -> np.ndarray:
        """Map {-1, 0, 1} → {0, 1, 2} for sklearn."""
        out = np.empty(len(y), dtype=int)
        for i, v in enumerate(y):
            if v == -1 or float(v) < -0.5:
                out[i] = 0
            elif v == 1 or float(v) > 0.5:
                out[i] = 2
            else:
                out[i] = 1
        return out

    @property
    def weights(self) -> Optional[Dict[str, float]]:
        if self._weights is None:
            return None
        return dict(zip(self.base_model_names, self._weights.tolist()))


# ── Backward-compatible alias ─────────────────────────────────────────────────


class StackingEnsemble(StackingMetaLearner):
    """
    Backward-compatible alias for StackingMetaLearner.

    Earlier code in this session used `StackingEnsemble`; new code should
    use `StackingMetaLearner` directly.
    """

    def fit_meta(  # type: ignore[override]
        self,
        oof_predictions: Dict[str, np.ndarray],
        y_oof: np.ndarray,
    ) -> None:
        super().fit_meta(oof_predictions, y_oof)

    # predict_proba_from_base → predict_ensemble
    def predict_proba_from_base(self, predictions: Dict[str, np.ndarray]) -> np.ndarray:
        return self.predict_proba(predictions)

    def weight_blend(self, predictions: Dict[str, np.ndarray]) -> np.ndarray:
        """Simple weighted blend (kept for backward compatibility)."""
        if self._weights is None:
            parts = [predictions.get(m, np.zeros((1, N_CLASSES))) for m in self.base_model_names]
            return np.mean(parts, axis=0)
        blended = np.zeros_like(next(iter(predictions.values())), dtype=np.float64)
        for i, name in enumerate(self.base_model_names):
            if name in predictions:
                blended += self._weights[i] * predictions[name]
        total = blended.sum(axis=1, keepdims=True) + 1e-10
        return (blended / total).astype(np.float32)

    def combine(self, live_predictions: Dict[str, np.ndarray]) -> np.ndarray:
        if self._meta is not None:
            return self.predict_proba(live_predictions)
        return self.weight_blend(live_predictions)


# ── Utility: softmax ──────────────────────────────────────────────────────────


def softmax(x: np.ndarray) -> np.ndarray:
    x = x - x.max(axis=1, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=1, keepdims=True)
