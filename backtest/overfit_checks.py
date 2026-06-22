"""
backtest/overfit_checks.py

Phase: 1.4 (Labeling + Backtesting Infrastructure)
Specs: SPEC-BT-001, SPEC-MODEL-003
Owner: Platform / Backtest
Consumers: backtest/integrity_checker.py (check_10_random_feature), backtest/engine.py (Phase 1.6)

Overfitting-detection utilities: the deflated Sharpe ratio (apply when
20+ configurations were tried — multiple-testing correction) and the
random-feature test (a model trained on shuffled features should score
~50% — anything durably above that is fitting noise, not signal). Both
are direct, tested ports of the pseudocode in alphalens_docs/
04_backtesting.md's "Overfitting Detection" section, adapted to this
codebase's contracts.interfaces.IModel (train/predict) instead of the
doc's generic `model_class()` sketch.
"""

import logging
from typing import List

import numpy as np
import pandas as pd
from scipy.stats import norm

from contracts.interfaces import IModel

logger = logging.getLogger(__name__)


def deflated_sharpe_ratio(sharpe: float, n_trials: int, n_obs: int) -> float:
    """
    Probability that the observed Sharpe ratio is genuine skill rather
    than the best of `n_trials` noisy configurations (SPEC-BT-001 rule 8:
    "apply DSR correction if testing 20+ configurations").

    Parameters
    ----------
    sharpe : float
        Observed (annualized) Sharpe ratio of the selected configuration.
    n_trials : int
        Number of configurations/strategies compared before selecting this one.
    n_obs : int
        Number of return observations the Sharpe ratio was computed over.

    Returns
    -------
    float
        DSR in [0, 1] — the probability the true Sharpe exceeds 0 after
        correcting for selection bias across n_trials attempts. Values
        below ~0.95 mean the result does not survive the multiple-testing
        correction.

    Spec References
    ----------------
    SPEC-BT-001: rule 8, Deflated Sharpe Ratio.

    Raises
    ------
    ValueError
        If n_trials < 1 or n_obs < 1.
    """
    if n_trials < 1 or n_obs < 1:
        raise ValueError("n_trials and n_obs must be >= 1")
    e_max = norm.ppf(1 - 1 / n_trials) if n_trials > 1 else 0.0
    dsr_stat = sharpe - e_max / np.sqrt(n_obs)
    return float(norm.cdf(dsr_stat))


def random_feature_test(
    model: IModel,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    feature_cols: List[str],
    n_repeats: int = 10,
    random_state: int = 42,
) -> float:
    """
    Train `model` on label-preserving but feature-shuffled training data
    and measure test accuracy — a model that scores durably above ~50%
    (binary) on permuted features is fitting noise in the original
    features, not real signal (SPEC-BT-001 rule: random feature test
    48-52% expected band).

    Parameters
    ----------
    model : IModel
        Any IModel implementation (uses .train(X, y) / .predict(X) only —
        SPEC-SOLID-003 Liskov substitution: works for any conforming model).
    X_train, y_train, X_test, y_test : pd.DataFrame / pd.Series
        Original (unshuffled) train/test split. y is compared against
        predictions on the (unshuffled) X_test — only the *training*
        features are permuted, never the test features or either target,
        otherwise this would measure something other than "does the model
        rely on real train-time feature/target structure."
    feature_cols : list of str
        Columns of X_train to permute (independently per column, per
        repeat — preserves each column's own marginal distribution while
        destroying its relationship to y_train and to every other column).
    n_repeats : int
        Number of independent shuffles to average over (default 10, per
        04_backtesting.md's reference implementation).
    random_state : int
        Base seed; each repeat uses a different derived seed for
        independence.

    Returns
    -------
    float
        Mean test accuracy across `n_repeats` shuffled-feature retrains.
        Expected near 0.50 for binary classification; durably > 0.55 is
        the doc's stated noise-fitting threshold.

    Spec References
    ----------------
    SPEC-BT-001: random feature test, 48-52% expected band.

    PIT Assumptions
    ----------------
    None beyond what the caller's X_train/X_test already guarantee — this
    function does not itself fetch or join any time-series data.

    Raises
    ------
    ValueError
        If feature_cols is empty or n_repeats < 1.
    """
    if not feature_cols:
        raise ValueError("feature_cols must be non-empty")
    if n_repeats < 1:
        raise ValueError("n_repeats must be >= 1")

    accuracies = []
    for i in range(n_repeats):
        rng = np.random.default_rng(random_state + i)
        shuffled = X_train.copy()
        for col in feature_cols:
            shuffled[col] = rng.permutation(shuffled[col].to_numpy())

        model.train(shuffled, y_train)
        predictions = model.predict(X_test)
        accuracy = float((predictions.to_numpy() == y_test.to_numpy()).mean())
        accuracies.append(accuracy)

    mean_accuracy = float(np.mean(accuracies))
    logger.info(f"Random feature test: mean accuracy {mean_accuracy:.4f} over {n_repeats} repeats")
    return mean_accuracy
