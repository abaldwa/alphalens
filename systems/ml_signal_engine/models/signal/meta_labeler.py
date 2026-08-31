"""
systems/ml_signal_engine/models/signal/meta_labeler.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-004
Owner: ml_signal_engine / signal
Consumers: systems/ml_signal_engine/inference/train_all_phase1.py

M-04: Meta-Labeler. "Should I act on this primary signal?" — a binary
filter trained on the SAME feature set as the primary signal model (per
the build prompt), labeled by whether the primary model's directional
call was profitable AFTER transaction costs (the critical difference from
the primary model's own triple-barrier label, which only checks
direction).

compute_labels() reuses backtest.costs.IndianTransactionCosts (built in
P1.4) for the round-trip cost threshold rather than hardcoding "~0.5%"
from 02_models.md's prose — the actual configured cost model is the
single source of truth (consistent with this project's "no hardcoded
constants" rule, SPEC-QUALITY-003).

NaN handling: train()/tune_threshold() originally did a blanket
frame.dropna() across every feature column plus the label — the same bug
already found and fixed in base_signal_model.py (P1.5), recurring here
because the fix wasn't propagated to this sibling file. With real
70-column technical features (252-day lookbacks, benchmark-dependent
columns, etc.) virtually no row has every column simultaneously
non-NaN, so the blanket dropna wiped 100% of rows even when
train_all_phase1.py's own >=10-non-NaN-label guard had already confirmed
there was enough labeled data. Caught via a live `python3 -m
systems.ml_signal_engine.inference.train_all_phase1 --quick` run that
crashed with "no valid (non-NaN) labeled rows" despite upstream logging
showing the run had cleared that guard. Fixed with the same median
SimpleImputer(keep_empty_features=True) pattern as BaseSignalModel,
fit on the training data only and reused (never refit) at predict/
tune_threshold time.
"""

import logging
from typing import Any, Dict, List, Optional, cast

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_score, recall_score

from backtest.costs import IndianTransactionCosts
from contracts.interfaces import IClassificationModel

logger = logging.getLogger(__name__)

# A representative liquid-stock trade for the round-trip cost threshold
# used by compute_labels()'s default — see that function's docstring.
_REPRESENTATIVE_PRICE = 1000.0
_REPRESENTATIVE_QUANTITY = 100

# SPEC-MODEL-004-style guard against a degenerate "predict nothing" optimum:
# precision-only threshold search is constrained to thresholds that still
# clear this minimum recall, otherwise "maximize precision" trivially picks
# the highest threshold with zero positive predictions (precision undefined
# -> 0 by zero_division, scanned past anyway, but this keeps the chosen
# threshold operationally useful).
MIN_RECALL_FLOOR = 0.05


class MetaLabeler(IClassificationModel):
    """M-04: binary Act(1)/Don't-Act(0) filter on top of a primary signal model's direction call."""

    def __init__(
        self, random_state: int = 42, lgbm_params: Optional[Dict[str, Any]] = None,
        use_class_weight: bool = True,
    ) -> None:
        self.random_state = random_state
        # use_class_weight defaults True: real Act/Don't-Act labels skew
        # toward the minority "Act" class (a strategy with e.g. a 25% win
        # rate has few profitable-after-cost calls to learn from) —
        # LightGBM's native "balanced" class_weight (inverse-frequency
        # re-weighting at fit time) fixes this without introducing a
        # separate resampling/SMOTE step that could leak synthetic
        # correlation across the train/val boundary. Only applied when the
        # caller hasn't already supplied their own lgbm_params (an explicit
        # lgbm_params dict is assumed to already encode the caller's choice).
        self._lgbm_params = lgbm_params or {
            "n_estimators": 200, "max_depth": 5, "learning_rate": 0.05,
            "random_state": random_state, "verbose": -1,
            "class_weight": "balanced" if use_class_weight else None,
        }
        self._lgbm: Optional[lgb.LGBMClassifier] = None
        self._feature_names: Optional[List[str]] = None
        self._imputer: Optional[SimpleImputer] = None
        self._threshold: float = 0.5
        self._trained_at: Optional[pd.Timestamp] = None
        self._training_samples: Optional[int] = None

    @staticmethod
    def compute_labels(
        direction: pd.Series,
        forward_return_pct: pd.Series,
        roundtrip_cost_pct: Optional[float] = None,
    ) -> pd.Series:
        """
        SPEC-MODEL-004-adjacent (02_models.md M-04): 1 if the primary
        model's directional call was profitable AFTER round-trip
        transaction costs, 0 otherwise. Hold (direction == 0) rows have
        no act decision to evaluate and are dropped (NaN), not labeled 0.

        Parameters
        ----------
        direction : pd.Series
            Primary model's called direction, values in {-1, 0, 1}
            (e.g. BaseSignalModel.predict(X)).
        forward_return_pct : pd.Series
            Realized forward return over the primary model's horizon, as
            a fraction (e.g. 0.03 = +3%), aligned to `direction`.
        roundtrip_cost_pct : float, optional
            Defaults to IndianTransactionCosts().compute_roundtrip_cost_pct()
            for a representative liquid trade (Rs 1000 x 100 shares) — the
            actually-configured cost model, not a hardcoded "~0.5%".

        Returns
        -------
        pd.Series
            1.0 / 0.0 for direction != 0 rows, NaN for direction == 0 rows.

        Raises
        ------
        ValueError
            If direction and forward_return_pct have mismatched index/length.
        """
        if len(direction) != len(forward_return_pct):
            raise ValueError("direction and forward_return_pct must be the same length")

        if roundtrip_cost_pct is None:
            roundtrip_cost_pct = IndianTransactionCosts().compute_roundtrip_cost_pct(
                _REPRESENTATIVE_PRICE, _REPRESENTATIVE_QUANTITY
            )

        net_return = direction * forward_return_pct
        label: pd.Series = pd.Series(np.nan, index=direction.index, dtype="float64")
        acted = direction != 0
        profitable = (net_return.loc[acted] > roundtrip_cost_pct).astype(float)
        label.loc[acted] = profitable
        return label

    def train(self, X: pd.DataFrame, y: pd.Series, sample_weight: Optional[pd.Series] = None) -> None:
        """
        Fit the LightGBM Act/Don't-Act classifier and tune its decision
        threshold to maximize precision (subject to MIN_RECALL_FLOOR) on
        the same data — for a held-out-fold-tuned threshold, call
        tune_threshold() separately with validation data.

        Parameters
        ----------
        X : pd.DataFrame
            Same feature columns as the primary signal model
            (features.matrix_builder.ALL_FEATURE_COLUMNS or a subset).
        y : pd.Series
            Output of compute_labels() — NaN (Hold) rows are dropped.

        Raises
        ------
        ValueError
            If X/y shapes mismatch, or no rows have a non-NaN label.
        """
        if len(X) != len(y):
            raise ValueError(f"X has {len(X)} rows, y has {len(y)} rows")

        valid = y.notna()
        if not valid.any():
            raise ValueError("no rows with a non-NaN label")

        self._feature_names = list(X.columns)
        X_valid, y_clean = X.loc[valid, self._feature_names], y.loc[valid].astype(int)
        X_clean = self._impute_fit(X_valid)

        self._lgbm = lgb.LGBMClassifier(**self._lgbm_params)
        self._lgbm.fit(X_clean, y_clean)
        self._threshold = _optimize_precision_threshold(self._lgbm.predict_proba(X_clean)[:, 1], y_clean)

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_clean)

    def tune_threshold(self, X_val: pd.DataFrame, y_val: pd.Series) -> float:
        """
        Re-tune the Act/Don't-Act decision threshold on a held-out
        validation fold (never the training fold or test fold) —
        SPEC-MODEL-004: threshold optimization, never 0.5 default.

        Returns
        -------
        float
            The tuned threshold (also stored on the instance).
        """
        if self._lgbm is None or self._feature_names is None:
            raise RuntimeError("tune_threshold called before train()")
        valid = y_val.notna()
        X_imputed = self._impute_transform(cast(pd.DataFrame, X_val.loc[valid, self._feature_names]))
        y_clean = y_val.loc[valid].astype(int)
        proba = self._lgbm.predict_proba(X_imputed)[:, 1]
        self._threshold = _optimize_precision_threshold(proba, y_clean)
        return self._threshold

    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        """Fit the median imputer on X (training data only) and return the imputed frame."""
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        imputed = self._imputer.fit_transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the already-fit imputer (never refit at predict time — that would leak statistics)."""
        if self._imputer is None:
            raise RuntimeError("predict called before train()")
        imputed = self._imputer.transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        if self._lgbm is None or self._feature_names is None:
            raise RuntimeError("predict_proba called before train()")
        X_imputed = self._impute_transform(cast(pd.DataFrame, X[self._feature_names]))
        proba = self._lgbm.predict_proba(X_imputed)
        return pd.DataFrame({"dont_act": proba[:, 0], "act": proba[:, 1]}, index=X.index)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        proba = self.predict_proba(X)["act"]
        return (proba >= self._threshold).astype(int).rename(None)

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame
            Columns: meta_label_act (bool), meta_label_prob (float).
        """
        proba = self.predict_proba(X)["act"]
        return pd.DataFrame({"meta_label_act": proba >= self._threshold, "meta_label_prob": proba}, index=X.index)

    def save(self, path: str) -> None:
        if self._lgbm is None:
            raise RuntimeError("save called before train()")
        joblib.dump(
            {
                "lgbm": self._lgbm, "feature_names": self._feature_names, "imputer": self._imputer,
                "threshold": self._threshold, "random_state": self.random_state,
                "trained_at": self._trained_at, "training_samples": self._training_samples,
            },
            path,
        )

    def load(self, path: str) -> None:
        payload = joblib.load(path)
        self._lgbm = payload["lgbm"]
        self._feature_names = payload["feature_names"]
        self._imputer = payload["imputer"]
        threshold = payload.get("threshold")
        if threshold is None:
            # config.settings.META_THRESHOLD fallback (item #7, user decision
            # 2026-07-04): a corrupted/incomplete artifact must still produce
            # a usable Act/Don't-Act cutoff rather than silently reverting to
            # the hardcoded 0.5 __init__ default this class explicitly warns
            # against (SPEC-MODEL-004: "threshold optimization, never 0.5
            # default").
            from config.settings import META_THRESHOLD

            logger.warning(
                "MetaLabeler.load(%s): saved payload has no tuned 'threshold' "
                "— falling back to config.settings.META_THRESHOLD=%s",
                path, META_THRESHOLD,
            )
            threshold = META_THRESHOLD
        self._threshold = threshold
        self.random_state = payload["random_state"]
        self._trained_at = payload["trained_at"]
        self._training_samples = payload["training_samples"]

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": "MetaLabeler",
            "version": "1.5.0",
            "created_at": self._trained_at,
            "features_count": len(self._feature_names) if self._feature_names else 0,
            "hyperparams": self._lgbm_params,
            "training_samples": self._training_samples,
            "threshold": self._threshold,
        }


def _optimize_precision_threshold(proba: np.ndarray, y_true: pd.Series) -> float:
    """SPEC-MODEL-004: maximize precision subject to a minimum recall floor (never 0.5 default)."""
    y_arr = y_true.to_numpy()
    grid = np.linspace(0.05, 0.95, 19)
    best_t, best_precision = 0.5, -1.0
    for t in grid:
        preds = (proba >= t).astype(int)
        recall = recall_score(y_arr, preds, zero_division=0)
        if recall < MIN_RECALL_FLOOR:
            continue
        precision = precision_score(y_arr, preds, zero_division=0)
        if precision > best_precision:
            best_precision, best_t = precision, t
    return float(best_t)
