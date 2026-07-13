"""
systems/ml_signal_engine/models/signal/base_signal_model.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-001, SPEC-MODEL-003, SPEC-MODEL-004, SPEC-SOLID-002, SPEC-SOLID-003
Owner: ml_signal_engine / signal
Consumers: systems/ml_signal_engine/models/signal/signal_5d.py,
           systems/ml_signal_engine/models/signal/signal_21d.py,
           systems/ml_signal_engine/models/signal/meta_labeler.py

BaseSignalModel: the shared "stacking ensemble + quantile regression"
architecture behind M-02 (Signal 5d) and M-03 (Signal 21d/63d) — per
02_models.md, M-03 is explicitly "Same as M-02 but with wider
triple-barrier thresholds", so the shared logic lives here once
(SPEC-SOLID-002: add new files/subclasses, don't duplicate) and
signal_5d.py/signal_21d.py are thin subclasses that only set
horizon-specific defaults.

Direction labels follow TripleBarrierLabeler's convention: -1 = Sell
(stop hit first), 0 = Hold (timeout), +1 = Buy (profit target hit first).

Architecture:
  1. Three base classifiers (LightGBM, CatBoost, XGBoost) each produce
     out-of-fold (OOF) 3-class probabilities on the training fold via
     cross-validation (never on the validation/test fold — that would
     leak the meta-learner's training signal).
  2. A LogisticRegression meta-learner trains on the stacked OOF
     probabilities (9 columns: 3 models x 3 classes) -> final calibrated
     3-class probability.
  3. Three independent LightGBM quantile regressors (alpha=0.10/0.50/0.90)
     predict the continuous forward return distribution — a SEPARATE
     target from the classification labels (02_models.md's "Quantile
     training (run 3 separate models)").
  4. Per-class probability thresholds are tuned on the validation fold by
     maximizing one-vs-rest F1 (never on test) — SPEC-MODEL-004:
     "Threshold optimization mandatory; never use 0.5 default."

NaN handling: LightGBM/CatBoost/XGBoost all tolerate missing feature
values natively (SPEC-FEAT-004's documented pattern — e.g. a 252-day
lookback feature that hasn't warmed up yet for a given ticker/date).
SMOTETomek does NOT (confirmed: imblearn raises "Input X contains NaN" —
it isn't one of the NaN-tolerant estimators scikit-learn documents). A
median SimpleImputer is fit once on the training fold and applied
everywhere downstream (resampling, all three base learners, the
meta-learner, and at predict time) so SMOTETomek never sees NaN, no row
is ever dropped just because one of 70+ feature columns hadn't warmed up
yet, and train/predict see consistently-imputed data.

[AS BUILT] "BaseModel" in 02_models.md's "Model Interface Standard" has a
4-argument train(X_train, y_train, X_val, y_val) signature, different
from contracts.interfaces.IModel's train(X, y, sample_weight=None) — same
reconciliation already applied to M-01 (HMM) and M-06 (P&D) in earlier
phases. This class implements IClassificationModel (train/predict/
predict_proba/save/load/metadata) for interface compliance, and adds
train_full(X_train, y_train, X_val, y_val, ...) as the real entry point
for the SPEC-MODEL-003/004-compliant HPO + threshold-tuned pipeline,
matching the doc's intent without breaking the project's actual
interface contract.
"""

import logging
from typing import Any, Dict, List, Optional

import catboost
import joblib
import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
import xgboost as xgb
from imblearn.combine import SMOTETomek
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict

from contracts.interfaces import IClassificationModel

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

DIRECTION_SELL, DIRECTION_HOLD, DIRECTION_BUY = -1, 0, 1
CLASS_ORDER = [DIRECTION_SELL, DIRECTION_HOLD, DIRECTION_BUY]
CLASS_NAMES = {DIRECTION_SELL: "sell", DIRECTION_HOLD: "hold", DIRECTION_BUY: "buy"}

SIGNAL_OUTPUT_COLUMNS = [
    "signal_buy_prob",
    "signal_hold_prob",
    "signal_sell_prob",
    "signal_q10",
    "signal_q50",
    "signal_q90",
]

N_STACKING_FOLDS = 5
DEFAULT_OPTUNA_TRIALS = 100


class BaseSignalModel(IClassificationModel):
    """
    Shared LightGBM+CatBoost+XGBoost stacking ensemble with LogisticRegression
    meta-learner, plus 3 independent LightGBM quantile regressors, for one
    triple-barrier horizon. Subclasses (Signal5DModel, Signal21DModel) only
    fix horizon_days/profit_multiplier/stop_multiplier.
    """

    def __init__(
        self,
        horizon_days: int,
        profit_multiplier: float = 2.0,
        stop_multiplier: float = 1.0,
        optuna_trials: int = DEFAULT_OPTUNA_TRIALS,
        random_state: int = 42,
        max_sampling_ratio: Optional[float] = None,
    ) -> None:
        if horizon_days <= 0:
            raise ValueError("horizon_days must be positive")
        self.horizon_days = horizon_days
        self.profit_multiplier = profit_multiplier
        self.stop_multiplier = stop_multiplier
        self.optuna_trials = optuna_trials
        self.random_state = random_state
        # ML21 (2026-07-10): optional cap on SMOTETomek's minority:majority
        # ratio (e.g. 0.5 = minority classes resampled to at most half the
        # majority class's count, not 1:1 'auto'). None preserves the
        # existing unbounded 'auto' behavior — this is opt-in, not a
        # default change, pending the before/after Sharpe comparison
        # FeatureBacklog.md's ML21 asks for before adoption.
        self.max_sampling_ratio = max_sampling_ratio

        self._feature_names: Optional[List[str]] = None
        self._imputer: Optional[SimpleImputer] = None
        self._lgbm_params: Dict[str, Any] = {
            "n_estimators": 200, "max_depth": 5, "learning_rate": 0.05,
            "random_state": random_state, "verbose": -1, "n_jobs": 2,
        }
        self._lgbm = None
        self._catboost = None
        self._xgboost = None
        self._present_classes: Optional[List[int]] = None
        self._meta: Optional[LogisticRegression] = None
        self._q10_model = None
        self._q50_model = None
        self._q90_model = None
        self._thresholds: Dict[int, float] = {c: 1.0 / len(CLASS_ORDER) for c in CLASS_ORDER}
        self._best_lgbm_params: Optional[Dict[str, Any]] = None
        self._trained_at = None
        self._training_samples: Optional[int] = None
        self._class_distribution: Optional[Dict[str, float]] = None

    # ===== Stacking ensemble internals =====
    def _make_base_models(self, params: Optional[Dict[str, Any]] = None) -> tuple:
        lgbm_params = {**self._lgbm_params, **(params or {})}
        lgbm = lgb.LGBMClassifier(**lgbm_params)
        cat = catboost.CatBoostClassifier(
            iterations=200, depth=5, learning_rate=0.05, random_state=self.random_state, verbose=False,
            thread_count=2,
        )
        xgboost_model = xgb.XGBClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.05,
            random_state=self.random_state, eval_metric="mlogloss", n_jobs=2,
        )
        return lgbm, cat, xgboost_model

    def _oof_stacked_features(
        self, X: pd.DataFrame, y_encoded: np.ndarray, lgbm_params: Optional[Dict] = None
    ) -> np.ndarray:
        """Out-of-fold probabilities from each base learner, stacked (n, 3*n_present_classes)."""
        lgbm, cat, xgboost_model = self._make_base_models(lgbm_params)
        n_splits = min(N_STACKING_FOLDS, int(np.min(np.bincount(y_encoded))))
        n_splits = max(n_splits, 2)
        cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=self.random_state)

        oof = []
        for model in (lgbm, cat, xgboost_model):
            proba = cross_val_predict(model, X, y_encoded, cv=cv, method="predict_proba")
            oof.append(proba)
        return np.hstack(oof)

    def _fit_base_and_meta(self, X: pd.DataFrame, y: pd.Series, lgbm_params: Optional[Dict] = None) -> None:
        # [2026-07-12 defect fix] dense (0-based, contiguous) encoding of only the
        # CLASS_ORDER values actually PRESENT in this fold — XGBoost's sklearn
        # wrapper hard-requires 0-based contiguous labels at fit time (verified:
        # it raises "Invalid classes inferred..." even with objective=
        # "multi:softprob", num_class=3 set explicitly), so the old fixed
        # 3-slot CLASS_ORDER offset (-1->0, 0->1, 1->2) crashes whenever a
        # fold is missing one class — e.g. a calendar-year fold with zero
        # triple-barrier sell hits in a strong bull market. Found while
        # training systems/ml_signal_engine_gainer/'s one-sided (HOLD/BUY-only)
        # gainer signal models, which hit this deterministically; applied
        # here too since production could hit the same fold composition.
        # self._present_classes records which CLASS_ORDER values were
        # actually seen (in CLASS_ORDER order) so predict_proba() can expand
        # back to the full, stable sell/hold/buy column contract every
        # downstream caller (_apply_thresholds, predict_signals, threshold
        # tuning) depends on.
        self._present_classes = [c for c in CLASS_ORDER if c in set(y.unique())]
        dense_map = {c: i for i, c in enumerate(self._present_classes)}
        y_encoded = y.map(dense_map).to_numpy()

        stacked = self._oof_stacked_features(X, y_encoded, lgbm_params)
        # multi_class param intentionally omitted: scikit-learn >= 1.5 deprecates the explicit
        # 'multinomial' value and always uses multinomial loss for a >2-class target anyway.
        self._meta = LogisticRegression(max_iter=1000, random_state=self.random_state)
        self._meta.fit(stacked, y_encoded)

        self._lgbm, self._catboost, self._xgboost = self._make_base_models(lgbm_params)
        self._lgbm.fit(X, y_encoded)
        self._catboost.fit(X, y_encoded)
        self._xgboost.fit(X, y_encoded)

    def _base_stacked_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self._lgbm is None:
            raise RuntimeError("predict called before train()/train_full()")
        X_imputed = self._impute_transform(X)
        probs = [m.predict_proba(X_imputed) for m in (self._lgbm, self._catboost, self._xgboost)]
        return np.hstack(probs)

    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        """Fit the median imputer on X (training data only) and return the imputed frame."""
        # keep_empty_features=True: a column that's entirely NaN in this particular training
        # fold (e.g. rs_vs_*/beta_63d/alpha_21d when no benchmark history exists yet for the
        # window in question) must still come out as a same-shape column (filled with 0, sklearn's
        # documented behavior for all-NaN columns under this flag) — SimpleImputer's default
        # (keep_empty_features=False) silently DROPS such columns instead, which desyncs the
        # output's column count from self._feature_names and breaks every downstream consumer
        # that indexes by feature name.
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        imputed = self._imputer.fit_transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the already-fit imputer (never refit at predict time — that would leak test-set statistics)."""
        if self._imputer is None:
            raise RuntimeError("predict called before train()/train_full()")
        imputed = self._imputer.transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    # ===== IModel / IClassificationModel =====
    def train(self, X: pd.DataFrame, y: pd.Series, sample_weight: Optional[pd.Series] = None) -> None:
        """
        IModel-compliant simple fit: stacking ensemble + meta-learner with
        default hyperparameters, no HPO, no SMOTETomek, no quantile
        regressors, no threshold tuning. Use train_full() for the complete
        SPEC-MODEL-003/004 pipeline this model is actually meant to run.

        Raises
        ------
        ValueError
            If X/y are empty or shapes mismatch, or no rows have a
            non-NaN label.
        """
        if len(X) != len(y):
            raise ValueError(f"X has {len(X)} rows, y has {len(y)} rows")
        valid = y.notna()
        if not valid.any():
            raise ValueError("no rows with a non-NaN label")

        self._feature_names = list(X.columns)
        X_valid, y_valid = X.loc[valid, self._feature_names], y.loc[valid]
        X_imputed = self._impute_fit(X_valid)
        self._fit_base_and_meta(X_imputed, y_valid)
        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Final class label per-row, using the threshold-tuned decision rule (see train_full)."""
        proba = self.predict_proba(X)
        return _apply_thresholds(proba, self._thresholds).rename(None)

    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        SPEC-MODEL-001 (IClassificationModel): per-class probabilities.

        Returns
        -------
        pd.DataFrame
            Columns 'sell', 'hold', 'buy' (CLASS_NAMES values, in
            CLASS_ORDER) — meta-learner's calibrated probabilities.
        """
        stacked = self._base_stacked_proba(X[self._feature_names])
        proba_narrow = self._meta.predict_proba(stacked)
        # Expand from "only the classes present at fit time" back to the fixed
        # sell/hold/buy contract (see _fit_base_and_meta's defect-fix comment) —
        # any CLASS_ORDER value never observed during training gets 0 probability.
        full = np.zeros((len(X), len(CLASS_ORDER)))
        present_classes = self._present_classes or CLASS_ORDER
        for dense_idx, class_value in enumerate(present_classes):
            full[:, CLASS_ORDER.index(class_value)] = proba_narrow[:, dense_idx]
        cols = [CLASS_NAMES[c] for c in CLASS_ORDER]
        return pd.DataFrame(full, columns=cols, index=X.index)

    def save(self, path: str) -> None:
        if self._lgbm is None:
            raise RuntimeError("save called before train()/train_full()")
        payload = {
            "lgbm": self._lgbm, "catboost": self._catboost, "xgboost": self._xgboost, "meta": self._meta,
            "q10": self._q10_model, "q50": self._q50_model, "q90": self._q90_model,
            "thresholds": self._thresholds, "feature_names": self._feature_names, "imputer": self._imputer,
            "horizon_days": self.horizon_days, "profit_multiplier": self.profit_multiplier,
            "stop_multiplier": self.stop_multiplier, "random_state": self.random_state,
            "best_lgbm_params": self._best_lgbm_params, "trained_at": self._trained_at,
            "training_samples": self._training_samples, "class_distribution": self._class_distribution,
            "present_classes": self._present_classes,
        }
        joblib.dump(payload, path)

    def load(self, path: str) -> None:
        payload = joblib.load(path)
        self._lgbm, self._catboost, self._xgboost = payload["lgbm"], payload["catboost"], payload["xgboost"]
        self._meta = payload["meta"]
        self._q10_model, self._q50_model, self._q90_model = payload["q10"], payload["q50"], payload["q90"]
        loaded_thresholds = payload.get("thresholds")
        if not loaded_thresholds:
            # config.settings.SIGNAL_THRESHOLD fallback (item #7, user decision
            # 2026-07-04): a corrupted/incomplete artifact or a bootstrap
            # model saved before train_full()'s F1-optimized threshold tuning
            # ran must still produce a usable, non-degenerate decision rule
            # rather than silently falling back to the 1/3 equal-share
            # default baked into __init__ — that default was never meant to
            # reach inference, only to exist before the first train() call.
            from config.settings import SIGNAL_THRESHOLD

            logger.warning(
                "%s.load(%s): saved payload has no tuned 'thresholds' — "
                "falling back to config.settings.SIGNAL_THRESHOLD=%s for "
                "buy/sell classes",
                type(self).__name__, path, SIGNAL_THRESHOLD,
            )
            loaded_thresholds = {
                c: (SIGNAL_THRESHOLD if c != DIRECTION_HOLD else 1.0 / len(CLASS_ORDER))
                for c in CLASS_ORDER
            }
        self._thresholds = loaded_thresholds
        self._feature_names = payload["feature_names"]
        self._imputer = payload["imputer"]
        self.horizon_days = payload["horizon_days"]
        self.profit_multiplier = payload["profit_multiplier"]
        self.stop_multiplier = payload["stop_multiplier"]
        self.random_state = payload["random_state"]
        self._best_lgbm_params = payload["best_lgbm_params"]
        self._trained_at = payload["trained_at"]
        self._training_samples = payload["training_samples"]
        self._class_distribution = payload["class_distribution"]
        self._present_classes = payload.get("present_classes") or list(CLASS_ORDER)

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": type(self).__name__,
            "version": "1.5.0",
            "created_at": self._trained_at,
            "features_count": len(self._feature_names) if self._feature_names else 0,
            "horizon_days": self.horizon_days,
            "hyperparams": self._best_lgbm_params or self._lgbm_params,
            "training_samples": self._training_samples,
            "class_distribution": self._class_distribution,
            "thresholds": self._thresholds,
        }

    # ===== Full SPEC-MODEL-003/004 pipeline =====
    def train_full(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        returns_train: Optional[pd.Series] = None,
        returns_val: Optional[pd.Series] = None,
    ) -> Dict[str, Any]:
        """
        Full training pipeline: SMOTETomek (train only) -> Optuna HPO (100
        trials, validation fold only) -> stacking ensemble + meta-learner
        -> quantile regressors (if returns given) -> per-class F1-optimized
        thresholds (validation fold only).

        Parameters
        ----------
        X_train, y_train : pd.DataFrame, pd.Series
            Training fold. y_train values must be in {-1, 0, 1}.
        X_val, y_val : pd.DataFrame, pd.Series
            Validation fold — used for HPO and threshold tuning, NEVER for
            fitting (SPEC-MODEL-003: "No data from test year used in
            training, HPO, or threshold selection" — X_val here is the
            *validation* slice carved from the training fold by
            WalkForwardValidator.get_train_validation_split, distinct from
            the held-out test fold).
        returns_train, returns_val : pd.Series, optional
            Continuous forward returns (not the -1/0/1 labels) for
            quantile regression. If omitted, signal_q10/q50/q90 are NaN.

        Returns
        -------
        dict
            Diagnostics: class_ratio_before/after SMOTETomek, best Optuna
            params, per-class thresholds, validation F1 per class.

        Spec References
        ----------------
        SPEC-MODEL-003: HPO only on validation fold, never test.
        SPEC-MODEL-004: SMOTETomek train-only; threshold never 0.5 default.

        Raises
        ------
        ValueError
            If X_train/y_train (or X_val/y_val) shapes mismatch, or
            y_train/y_val contain values outside {-1, 0, 1}.
        """
        if len(X_train) != len(y_train) or len(X_val) != len(y_val):
            raise ValueError("X/y shape mismatch in train or validation fold")
        if not set(y_train.dropna().unique()).issubset(set(CLASS_ORDER)) or not set(
            y_val.dropna().unique()
        ).issubset(set(CLASS_ORDER)):
            raise ValueError(f"labels must be in {CLASS_ORDER}")

        self._feature_names = list(X_train.columns)
        train_valid = y_train.notna()
        val_valid = y_val.notna()
        X_train_clean = X_train.loc[train_valid, self._feature_names]
        y_train_clean = y_train.loc[train_valid]
        X_val_raw = X_val.loc[val_valid, self._feature_names]
        y_val_clean = y_val.loc[val_valid]

        # Fit the imputer on the training fold only (never on validation/test —
        # that would leak validation-fold statistics into the training pipeline).
        X_train_imputed = self._impute_fit(X_train_clean)
        X_val_imputed = self._impute_transform(X_val_raw)

        ratio_before = y_train_clean.value_counts(normalize=True).to_dict()
        X_res, y_res = self._resample(X_train_imputed, y_train_clean, max_sampling_ratio=self.max_sampling_ratio)
        ratio_after = pd.Series(y_res).value_counts(normalize=True).to_dict()

        best_params = self._optuna_search(X_res, y_res, X_val_imputed, y_val_clean)
        self._best_lgbm_params = best_params

        self._fit_base_and_meta(X_res, pd.Series(y_res), best_params)

        if returns_train is not None:
            returns_train_clean = returns_train.loc[train_valid]
            self._fit_quantile_models(X_train_imputed, returns_train_clean)

        val_proba = self.predict_proba(X_val_raw)
        self._thresholds, val_f1 = _optimize_thresholds(val_proba, y_val_clean)

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_res)
        self._class_distribution = {str(k): v for k, v in ratio_after.items()}

        return {
            "class_ratio_before": {str(k): v for k, v in ratio_before.items()},
            "class_ratio_after": {str(k): v for k, v in ratio_after.items()},
            "best_lgbm_params": best_params,
            "thresholds": self._thresholds,
            "val_f1_per_class": val_f1,
        }

    @staticmethod
    def _resample(
        X: pd.DataFrame, y: pd.Series, random_state: int = 42, max_sampling_ratio: Optional[float] = None,
    ) -> tuple:
        """SPEC-MODEL-004: SMOTETomek on training data only.

        max_sampling_ratio (ML21, 2026-07-10): when set, caps each minority
        class's post-resample count at `max_sampling_ratio * majority_count`
        instead of imblearn's default 'auto' (1:1 with the majority class).
        This directly bounds the oversample-driven matrix blowup that caused
        the 2026-07-09 signal_63d OOM (49.5% buy / 42.2% hold / 8.3% sell —
        'auto' multiplies the 8.3% class up to the 49.5% class's count).
        None (default) preserves the original 'auto' behavior unchanged.
        max_sampling_ratio <= 0 (ML24, 2026-07-11) disables SMOTETomek
        entirely — the classifier trains on the true, unresampled class
        prior (same target distribution the quantile heads already see).
        """
        counts = y.value_counts()
        if len(counts) < 2 or counts.min() < 2:
            logger.warning("Training fold has <2 samples in some class — skipping SMOTETomek")
            return X, y
        if max_sampling_ratio is not None and max_sampling_ratio <= 0:
            logger.info("max_sampling_ratio<=0 — skipping SMOTETomek, training on true class prior")
            return X, y
        sampling_strategy = "auto"
        if max_sampling_ratio is not None:
            majority_count = int(counts.max())
            target = max(int(majority_count * max_sampling_ratio), int(counts.min()))
            sampling_strategy = {
                cls: max(target, int(cnt)) for cls, cnt in counts.items() if cnt < majority_count
            }
            if not sampling_strategy:
                sampling_strategy = "auto"
        smote_tomek = SMOTETomek(random_state=random_state, sampling_strategy=sampling_strategy)
        return smote_tomek.fit_resample(X, y)

    def _optuna_search(self, X_train: pd.DataFrame, y_train, X_val: pd.DataFrame, y_val: pd.Series) -> Dict[str, Any]:
        """
        SPEC-MODEL-003: Optuna HPO tunes the primary (LightGBM) learner
        only — CatBoost/XGBoost stay at fixed reasonable defaults (full
        3-way HPO would triple search cost for marginal Phase-1 gain) —
        evaluated on X_val/y_val (validation fold), never on test.
        """
        y_train_encoded = pd.Series(y_train).map({c: i for i, c in enumerate(CLASS_ORDER)}).to_numpy()
        y_val_encoded = y_val.map({c: i for i, c in enumerate(CLASS_ORDER)}).to_numpy()

        def objective(trial: optuna.Trial) -> float:
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 50, 300),
                "max_depth": trial.suggest_int("max_depth", 3, 8),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 15, 63),
                "random_state": self.random_state,
                "verbose": -1,
                "n_jobs": 2,
            }
            model = lgb.LGBMClassifier(**params)
            model.fit(X_train, y_train_encoded)
            preds = model.predict(X_val)
            return f1_score(y_val_encoded, preds, average="macro", zero_division=0)

        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=self.random_state))
        study.optimize(objective, n_trials=self.optuna_trials, show_progress_bar=False)
        best = dict(study.best_params)
        best["random_state"] = self.random_state
        best["verbose"] = -1
        return best

    def _fit_quantile_models(self, X: pd.DataFrame, returns: pd.Series) -> None:
        """02_models.md: 3 independent LightGBM quantile regressors on continuous forward return."""
        valid = returns.notna()
        X_valid, y_valid = X.loc[valid], returns.loc[valid]
        if X_valid.empty:
            logger.warning("No valid forward-return rows — skipping quantile model training")
            return
        for alpha, attr in ((0.10, "_q10_model"), (0.50, "_q50_model"), (0.90, "_q90_model")):
            model = lgb.LGBMRegressor(
                objective="quantile", alpha=alpha, n_estimators=200, max_depth=5,
                learning_rate=0.05, random_state=self.random_state, verbose=-1, n_jobs=2,
            )
            model.fit(X_valid, y_valid)
            setattr(self, attr, model)

    def predict_signals(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        The build prompt's required output contract.

        Parameters
        ----------
        X : pd.DataFrame
            Feature matrix (features.matrix_builder.ALL_FEATURE_COLUMNS,
            or any superset containing this model's trained feature set).

        Returns
        -------
        pd.DataFrame
            Columns: signal_buy_prob, signal_hold_prob, signal_sell_prob
            (sum to 1.0 per row, NaN rows excepted),
            signal_q10, signal_q50, signal_q90 (NaN if no quantile models
            were trained — see train_full's returns_train parameter).

        Raises
        ------
        RuntimeError
            If called before train()/train_full().
        """
        proba = self.predict_proba(X)
        out = pd.DataFrame(index=X.index)
        out["signal_buy_prob"] = proba["buy"]
        out["signal_hold_prob"] = proba["hold"]
        out["signal_sell_prob"] = proba["sell"]

        quantile_attrs = ("_q10_model", "_q50_model", "_q90_model")
        has_quantile_models = any(getattr(self, attr) is not None for attr in quantile_attrs)
        X_imputed = self._impute_transform(X[self._feature_names]) if has_quantile_models else None
        quantile_columns = ("signal_q10", "signal_q50", "signal_q90")
        for attr, col in zip(quantile_attrs, quantile_columns):
            model = getattr(self, attr)
            out[col] = model.predict(X_imputed) if model is not None else np.nan

        return out[SIGNAL_OUTPUT_COLUMNS]


def _apply_thresholds(proba: pd.DataFrame, thresholds: Dict[int, float]) -> pd.Series:
    """
    Per-class threshold decision rule (SPEC-MODEL-004: never 0.5 default).
    A class is "called" if its probability exceeds its own tuned
    threshold; among called classes, the one with the largest
    margin-over-threshold wins. Hold wins if no class clears its threshold
    (the conservative default — "don't act" rather than force a Buy/Sell).
    """
    margins = pd.DataFrame(index=proba.index)
    for c in CLASS_ORDER:
        name = CLASS_NAMES[c]
        margins[c] = proba[name] - thresholds.get(c, 1.0 / len(CLASS_ORDER))

    cleared = margins[margins > 0]
    result = pd.Series(DIRECTION_HOLD, index=proba.index, dtype=int)
    any_cleared = cleared.notna().any(axis=1)
    result.loc[any_cleared] = cleared.loc[any_cleared].idxmax(axis=1)
    return result


def _optimize_thresholds(proba: pd.DataFrame, y_val: pd.Series) -> tuple:
    """
    SPEC-MODEL-004: per-class threshold maximizing one-vs-rest F1 on the
    validation fold, scanned over a fine grid — never the 0.5 default.
    """
    thresholds: Dict[int, float] = {}
    f1_per_class: Dict[str, float] = {}
    grid = np.linspace(0.05, 0.95, 19)

    for c in CLASS_ORDER:
        name = CLASS_NAMES[c]
        y_binary = (y_val == c).astype(int).to_numpy()
        scores = proba[name].to_numpy()
        best_t, best_f1 = 1.0 / len(CLASS_ORDER), -1.0
        for t in grid:
            preds = (scores >= t).astype(int)
            f1 = f1_score(y_binary, preds, zero_division=0)
            if f1 > best_f1:
                best_f1, best_t = f1, t
        thresholds[c] = float(best_t)
        f1_per_class[name] = float(best_f1)

    return thresholds, f1_per_class
