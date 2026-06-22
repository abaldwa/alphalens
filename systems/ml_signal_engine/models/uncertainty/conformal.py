"""
systems/ml_signal_engine/models/uncertainty/conformal.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-007
Owner: ml_signal_engine / uncertainty
Consumers: systems/ml_signal_engine/inference/train_all_phase1.py

M-05: Conformal Prediction. Calibrated prediction intervals with
guaranteed (target) coverage, wrapping any already-trained point/quantile
regressor (e.g. BaseSignalModel's Q50 model) — SPEC-MODEL-007 explicitly
requires the ACI (Adaptive Conformal Inference) variant, not standard
CQR, because financial time series violates the exchangeability
assumption standard conformal prediction relies on (regime changes,
temporal autocorrelation).

[AS BUILT] 02_models.md's example code imports `MapieQuantileRegressor`
with `method="quantile"` — that is actually Conformalized Quantile
Regression (CQR), which directly contradicts the same doc's own prose one
line above ("Use ACI variant (not standard CQR)"). It's also not
runnable: `mapie==1.3.0` (the pinned version, requirements/phase1.txt) has
no `MapieQuantileRegressor` class at all — MAPIE's API was restructured
across versions and that name doesn't exist in 1.3.0 (confirmed by
inspecting the installed package: `mapie.regression` exposes
`ConformalizedQuantileRegressor`, `SplitConformalRegressor`,
`CrossConformalRegressor`, `JackknifeAfterBootstrapRegressor`, and
`TimeSeriesRegressor`). `TimeSeriesRegressor(method="aci")` is the actual
ACI implementation in 1.3.0 (its docstring cites Zaffran et al.,
"Adaptive Conformal Predictions for Time Series" — the paper SPEC-MODEL-007
is describing) — used here instead of the doc's code sample, which the
doc's own prose requirement (ACI, not CQR) takes precedence over.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd
from mapie.regression import TimeSeriesRegressor
from sklearn.base import RegressorMixin

logger = logging.getLogger(__name__)

TARGET_COVERAGE = 0.90  # SPEC-MODEL-007: alpha = 0.10
MIN_ACCEPTABLE_COVERAGE = 0.85  # SPEC-MODEL-007: "alert if actual coverage < 85%"
NARROW_WIDTH_THRESHOLD_PCT = 0.04  # build prompt: "width < 4 percentage points = narrow"
DEFAULT_ACI_GAMMA = 0.05  # online ACI step size for adapt()


class ConformalPredictor:
    """
    Wraps an already-fitted scikit-learn-compatible regressor (e.g. a
    BaseSignalModel quantile model) with MAPIE's ACI conformal layer.

    Spec References
    ----------------
    SPEC-MODEL-007: 90% target coverage, ACI variant, monthly online
    recalibration via adapt(), alert if rolling coverage < 85%.
    """

    def __init__(
        self,
        estimator: RegressorMixin,
        target_coverage: float = TARGET_COVERAGE,
        gamma: float = DEFAULT_ACI_GAMMA,
    ) -> None:
        if not 0 < target_coverage < 1:
            raise ValueError("target_coverage must be in (0, 1)")
        self.target_coverage = target_coverage
        self.gamma = gamma
        self._mapie = TimeSeriesRegressor(estimator=estimator, method="aci", cv="prefit")
        self._calibrated = False

    def calibrate(self, X_cal: pd.DataFrame, y_cal: pd.Series) -> None:
        """
        Fit the conformal layer on a held-out calibration set (distinct
        from both the estimator's own training data and any test fold).

        Parameters
        ----------
        X_cal : pd.DataFrame
        y_cal : pd.Series
            Realized continuous target (e.g. forward return), aligned to X_cal.

        Raises
        ------
        ValueError
            If X_cal/y_cal are empty or mismatched in length.
        """
        if len(X_cal) != len(y_cal):
            raise ValueError("X_cal and y_cal must be the same length")
        if len(X_cal) == 0:
            raise ValueError("X_cal/y_cal must be non-empty")
        self._mapie.fit(X_cal.to_numpy(), y_cal.to_numpy())
        self._calibrated = True

    def predict_interval(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        X : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            Columns: conformal_point, conformal_lower, conformal_upper,
            conformal_width, conformal_narrow (bool — width below
            NARROW_WIDTH_THRESHOLD_PCT, the build prompt's "high
            conviction" heuristic).

        Raises
        ------
        RuntimeError
            If called before calibrate().
        """
        if not self._calibrated:
            raise RuntimeError("predict_interval called before calibrate()")
        point, intervals = self._mapie.predict(
            X.to_numpy(), confidence_level=self.target_coverage, allow_infinite_bounds=True
        )
        lower = intervals[:, 0, 0]
        upper = intervals[:, 1, 0]
        width = upper - lower
        return pd.DataFrame(
            {
                "conformal_point": point,
                "conformal_lower": lower,
                "conformal_upper": upper,
                "conformal_width": width,
                "conformal_narrow": width < NARROW_WIDTH_THRESHOLD_PCT,
            },
            index=X.index,
        )

    def adapt(self, X_new: pd.DataFrame, y_new: pd.Series, gamma: Optional[float] = None) -> None:
        """
        SPEC-MODEL-007: "monthly ACI online update" — adapts the
        conformal width using newly realized (X, y) pairs without a full
        recalibration/retrain.

        Raises
        ------
        RuntimeError
            If called before calibrate().
        """
        if not self._calibrated:
            raise RuntimeError("adapt called before calibrate()")
        self._mapie.adapt_conformal_inference(X_new.to_numpy(), y_new.to_numpy(), gamma=gamma or self.gamma)

    def evaluate_coverage(self, X: pd.DataFrame, y_true: pd.Series) -> float:
        """
        Empirical coverage on a labeled set: fraction of rows where
        y_true falls within [conformal_lower, conformal_upper].

        Spec References
        ----------------
        SPEC-MODEL-007: "Validate monthly on last 63 days; alert if
        actual coverage < 85%" — callers should compare this method's
        return value against MIN_ACCEPTABLE_COVERAGE.
        """
        intervals = self.predict_interval(X)
        covered = (y_true.to_numpy() >= intervals["conformal_lower"].to_numpy()) & (
            y_true.to_numpy() <= intervals["conformal_upper"].to_numpy()
        )
        coverage = float(np.mean(covered))
        if coverage < MIN_ACCEPTABLE_COVERAGE:
            logger.warning(
                f"Conformal coverage {coverage:.3f} below MIN_ACCEPTABLE_COVERAGE "
                f"({MIN_ACCEPTABLE_COVERAGE}) — recalibration recommended"
            )
        return coverage
