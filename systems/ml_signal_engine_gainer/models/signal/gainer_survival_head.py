"""
systems/ml_signal_engine_gainer/models/signal/gainer_survival_head.py

GAINER EXPERIMENT (development phase, FeatureBacklog.md ML33, 2026-07-13
user-authorized) — a small RandomSurvivalForest "first_touch_day"
survival-curve head, bolted onto the 21d/63d gainer signal models only
(NOT signal_5d/6d — the backlog row's own feasibility note found the 6d
window too short for day-level timing to change a trading decision).
Mirrors systems/ml_signal_engine_gainer/models/multibagger/
multibagger_model.py's RandomSurvivalForest usage (same library, same
Surv.from_arrays (duration, event) shape, same median-impute-then-fit
pattern) but deliberately smaller/simpler: no checkpointing, no
negative-subsampling — the 21d/63d gainer targets have a ~26-35%
positive rate over a far smaller chunked dataset than multibagger's
crippling ~0.3%-over-629K-rows case that motivated those features, so a
single-shot fit here is expected to be cheap (minutes, not days per the
backlog row's own cost assessment).

Labels come from systems/ml_signal_engine_gainer/training/labeling.py's
compute_fixed_pct_labels: event = label (1 if the forward path touched
+target_pct within horizon_days, else 0/right-censored at horizon_days),
duration = first_touch_day for event==1 rows, horizon_days (the
observation window's own length, i.e. right-censoring point) for
event==0 rows — same "duration is the touch day if it happened, else
the censoring point" convention multibagger's build_binary_labels uses
for duration_months.

Does NOT touch systems/ml_signal_engine/ (production), backtest/, or
datastore/models/ registry files — self-contained under
ml_signal_engine_gainer/.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sksurv.ensemble import RandomSurvivalForest
from sksurv.util import Surv

logger = logging.getLogger(__name__)


class GainerSurvivalHead:
    """
    Small RSF head: fit(X, first_touch_day, event) -> survival-curve
    predictions. Deliberately minimal — no lambdarank/calibration stage
    (that's the existing GainerSignal*DModel classifier's job), this
    class only adds the timing/survival-curve output on top.
    """

    def __init__(self, random_state: int = 42, n_estimators: int = 100, min_samples_leaf: int = 5) -> None:
        self.random_state = random_state
        self.n_estimators = n_estimators
        self.min_samples_leaf = min_samples_leaf
        self._rsf: Optional[RandomSurvivalForest] = None
        self._imputer: Optional[SimpleImputer] = None
        self._feature_names: Optional[List[str]] = None
        self._trained_at: Optional[pd.Timestamp] = None
        self._training_samples: int = 0

    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        arr = self._imputer.fit_transform(X)
        return pd.DataFrame(arr, columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if self._imputer is None:
            raise RuntimeError("_impute_transform called before fit()")
        arr = self._imputer.transform(X)
        return pd.DataFrame(arr, columns=X.columns, index=X.index)

    def fit(
        self,
        X: pd.DataFrame,
        first_touch_day: pd.Series,
        event: pd.Series,
        horizon_days: int,
    ) -> Dict[str, Any]:
        """
        Parameters
        ----------
        X : pd.DataFrame
            Feature matrix (any columns — same "trains on whatever X has"
            convention as every other model in this project).
        first_touch_day : pd.Series
            compute_fixed_pct_labels' first_touch_day column — NaN for
            event==0 (censored) rows.
        event : pd.Series
            compute_fixed_pct_labels' label column (0/1), aligned to X.
        horizon_days : int
            The labeling horizon — used as the right-censoring duration
            for event==0 rows (they survived the whole observation
            window without touching target_pct).

        Returns
        -------
        dict
            Diagnostics: training_samples, event_rate, concordance_index
            (in-sample, a sanity check only — not a held-out metric).

        Raises
        ------
        ValueError
            If inputs are misaligned/empty or event has values outside {0, 1}.
        """
        lengths = {len(X), len(first_touch_day), len(event)}
        if len(lengths) != 1:
            raise ValueError("X/first_touch_day/event must all be the same length")
        if horizon_days <= 0:
            raise ValueError("horizon_days must be positive")

        valid = event.notna()
        if not valid.any():
            raise ValueError("no rows with a non-NaN event")
        if not set(event.dropna().unique()).issubset({0, 1}):
            raise ValueError("event must be binary {0, 1}")

        self._feature_names = list(X.columns)
        X_valid = X.loc[valid, self._feature_names]
        event_valid = event.loc[valid].astype(bool)
        # duration: the real touch day for events, else the full horizon
        # (right-censored) — never NaN once event is resolved.
        duration_valid = first_touch_day.loc[valid].where(event_valid, horizon_days).clip(lower=1).astype(float)

        X_imputed = self._impute_fit(X_valid)
        surv_y = Surv.from_arrays(event=event_valid.to_numpy(), time=duration_valid.to_numpy())

        self._rsf = RandomSurvivalForest(
            n_estimators=self.n_estimators,
            random_state=self.random_state,
            min_samples_leaf=self.min_samples_leaf,
            n_jobs=min(4, os.cpu_count() or 1),
        )
        self._rsf.fit(X_imputed, surv_y)
        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

        try:
            concordance = float(self._rsf.score(X_imputed, surv_y))
        except Exception:
            concordance = np.nan

        return {
            "training_samples": self._training_samples,
            "event_rate": float(event_valid.mean()),
            "concordance_index": concordance,
        }

    def predict_survival_at_days(self, X: pd.DataFrame, days: List[int]) -> pd.DataFrame:
        """
        Survival probability (probability of NOT having touched
        target_pct yet) at each of `days`, one column per day, for every
        row of X.

        Returns
        -------
        pd.DataFrame
            Columns "survival_d{d}" for each d in days, indexed like X.
        """
        if self._rsf is None:
            raise RuntimeError("predict_survival_at_days called before fit()")
        X_imputed = self._impute_transform(X[self._feature_names])
        surv_funcs = self._rsf.predict_survival_function(X_imputed)
        out = {}
        for d in days:
            out[f"survival_d{d}"] = [float(fn(d)) if d <= fn.x.max() else float(fn(fn.x.max())) for fn in surv_funcs]
        return pd.DataFrame(out, index=X.index)
