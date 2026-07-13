"""
systems/ml_signal_engine_gainer/models/signal/signal_ranker.py

GAINER EXPERIMENT: a lambdarank-based ranking model for the short-horizon
gainer targets (5%/6d, 10%/21d, 20%/63d), mirroring MultibaggerModel's
architecture (systems/ml_signal_engine_gainer/models/multibagger/
multibagger_model.py: LightGBM lambdarank + Platt-scaling calibration)
instead of BaseSignalModel's 3-class stacking classifier.

Why try this alongside the classifier: BaseSignalModel makes a hard
BUY/HOLD/SELL call per row via a tuned probability threshold — recall
came out very low (0.10-0.28 walk-forward across the 3 horizons, see
2026-07-12 evaluation report) because few rows clear the threshold.
MultiBagger's approach instead RANKS every row by probability and takes
a top-N list, which is how you'd actually use a short-term signal too
("today's top 20 candidates") — this may recover more of the recall the
threshold approach is leaving on the table, at the cost of not having a
calibrated "is this specific stock a buy" answer for an arbitrary
candidate outside the top-N.

One ranker per horizon; each lambdarank group = one as-of date (rows
sharing a date compete against each other), matching the daily
recommendation cadence rather than MultibaggerModel's weekly-snapshot
grouping.
"""

import logging
from typing import Any, Dict, List, Optional

import lightgbm as lgb
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

logger = logging.getLogger(__name__)

WATCHLIST_SIZE = 20


class SignalRankerModel:
    """Lambdarank ranker + Platt-scaling calibration for one short-horizon gainer target."""

    def __init__(self, horizon_days: int, target_pct: float, random_state: int = 42, n_estimators: int = 200) -> None:
        self.horizon_days = horizon_days
        self.target_pct = target_pct
        self.random_state = random_state
        self.n_estimators = n_estimators

        self._ranker: Optional[lgb.LGBMRanker] = None
        self._calibrator = None
        self._imputer: Optional[SimpleImputer] = None
        self._feature_names: Optional[List[str]] = None
        self._trained_at = None
        self._training_samples: Optional[int] = None

    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        return pd.DataFrame(self._imputer.fit_transform(X), columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if self._imputer is None:
            raise RuntimeError("predict called before train()")
        return pd.DataFrame(self._imputer.transform(X), columns=X.columns, index=X.index)

    def train(self, X: pd.DataFrame, y: pd.Series, dates: pd.Series) -> Dict[str, Any]:
        """
        X : feature matrix. y : binary label {0, 1} (FixedPercentLabeler's
        output). dates : same index as X/y — one lambdarank group per
        distinct date (rows sharing a date compete against each other,
        matching daily recommendation cadence).
        """
        self._feature_names = list(X.columns)
        X_imputed = self._impute_fit(X[self._feature_names])
        y_int = y.astype(int)

        # lambdarank requires rows sorted by group
        order = dates.sort_values().index
        X_sorted, y_sorted, dates_sorted = X_imputed.loc[order], y_int.loc[order], dates.loc[order]
        groups = dates_sorted.groupby(dates_sorted, sort=True).size().tolist()

        self._ranker = lgb.LGBMRanker(
            objective="lambdarank", metric="ndcg", ndcg_eval_at=[10, 20],
            n_estimators=self.n_estimators, max_depth=5, learning_rate=0.05,
            random_state=self.random_state, verbose=-1,
        )
        self._ranker.fit(X_sorted, y_sorted, group=groups)

        raw_scores = self._ranker.predict(X_sorted).reshape(-1, 1)
        if y_sorted.nunique() < 2:
            logger.warning("Only one class present — calibrator will be degenerate (constant probability)")
            self._calibrator = DummyClassifier(strategy="constant", constant=y_sorted.iloc[0])
        else:
            self._calibrator = LogisticRegression(random_state=self.random_state)
        self._calibrator.fit(raw_scores, y_sorted)

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_sorted)
        return {"training_samples": self._training_samples, "positive_rate": float(y_sorted.mean())}

    def predict_proba(self, X: pd.DataFrame) -> pd.Series:
        if self._ranker is None:
            raise RuntimeError("predict called before train()")
        X_imputed = self._impute_transform(X[self._feature_names])
        raw_scores = self._ranker.predict(X_imputed).reshape(-1, 1)
        proba_matrix = self._calibrator.predict_proba(raw_scores)
        if proba_matrix.shape[1] < 2:
            proba = proba_matrix[:, 0] if bool(self._calibrator.classes_[0]) else 1.0 - proba_matrix[:, 0]
        else:
            proba = proba_matrix[:, 1]
        return pd.Series(proba, index=X.index).clip(0, 1)

    def top_n_per_date(self, X: pd.DataFrame, dates: pd.Series, n: int = WATCHLIST_SIZE) -> pd.DataFrame:
        """Rank-based selection: top-N candidates per as-of date, mirroring
        MultibaggerModel's generate_weekly_watchlist but per-day instead of weekly."""
        proba = self.predict_proba(X)
        scored = pd.DataFrame({"date": dates, "proba": proba}, index=X.index)
        return scored.groupby("date", group_keys=False).apply(
            lambda g: g.sort_values("proba", ascending=False).head(n)
        )
