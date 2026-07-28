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
from typing import List, Optional

import numpy as np
import pandas as pd
from scipy.stats import norm

from contracts.interfaces import IModel

logger = logging.getLogger(__name__)

# Euler-Mascheroni constant, used in the expected-max-Sharpe-under-the-null
# approximation below (Bailey & Lopez de Prado 2014, eq. 10).
_EULER_MASCHERONI = 0.5772156649015329


def deflated_sharpe_ratio(
    sharpe: float, n_trials: int, n_obs: int, returns: Optional[pd.Series] = None
) -> float:
    """
    Probability that the observed Sharpe ratio is genuine skill rather
    than the best of `n_trials` noisy configurations (SPEC-BT-001 rule 8:
    "apply DSR correction if testing 20+ configurations").

    Full Bailey & Lopez de Prado (2014) formula: "The Deflated Sharpe
    Ratio: Correcting for Selection Bias, Backtest Overfitting, and
    Non-Normality". Two components a simplified version previously
    omitted (2026-07-19 full-codebase-review Fix B4):

    1. Expected max Sharpe under the null across n_trials configurations
       uses the full two-term approximation (eq. 10) — E[max SR] =
       sqrt(V[SR]) * [(1-gamma)*Phi^-1(1-1/N) + gamma*Phi^-1(1-1/(N*e))],
       gamma = Euler-Mascheroni constant — not just the single
       Phi^-1(1-1/N) term.
    2. The Sharpe ratio's standard error accounts for the return series'
       skewness (gamma3) and excess kurtosis (gamma4) (eq. 8):
       sigma(SR) = sqrt((1 - gamma3*SR + (gamma4/4)*SR^2) / (n_obs - 1)).
       A fat-tailed or negatively-skewed strategy (the common case for
       momentum-style strategies, which tend to have negative skew from
       occasional sharp reversals) has a WIDER standard error than the
       normal-distribution assumption implies, meaning a previously
       "passing" Sharpe ratio can fail once the real return distribution's
       skew/kurtosis is accounted for.

    Parameters
    ----------
    sharpe : float
        Observed PER-PERIOD (e.g. daily) Sharpe ratio of the selected
        configuration — NOT annualized. [BUG FIX, 5th fundamental-
        strategies review, item 6] this docstring previously said
        "annualized", which is wrong and was itself the direct cause of a
        real bug: 3 of 4 production call sites (backtest/
        run_strategy_queue.py, backtest/backfill_dsr.py, backtest/
        iterative_retrain.py) fed this an ANNUALIZED Sharpe (backtest/
        core/metrics.py::sharpe_ratio's output), inflating the statistic
        by ~sqrt(TRADING_DAYS_PER_YEAR) and saturating DSR near 1.0 —
        silently defeating the multiple-comparisons gate this function
        exists to enforce. The null-distribution correction terms below
        (expected-max-Sharpe-under-the-null, the skewness/kurtosis-
        adjusted standard error) are derived in Bailey & Lopez de Prado
        (2014) entirely in PER-PERIOD units; annualizing sharpe before
        calling this function is not a harmless unit conversion, it
        changes the statistic's meaning. If a caller only has an
        annualized Sharpe on hand, de-annualize it first: raw_sharpe =
        sharpe_annualized / sqrt(TRADING_DAYS_PER_YEAR) (exact, since
        sharpe_annualized = sharpe_daily * sqrt(TRADING_DAYS_PER_YEAR) by
        construction) — see any of the 4 fixed call sites above for the
        working pattern. Do not trust a stale docstring over those.
    n_trials : int
        Number of configurations/strategies compared before selecting this one.
    n_obs : int
        Number of return observations the Sharpe ratio was computed over.
    returns : pd.Series, optional
        The actual per-period return series the Sharpe ratio was computed
        from. When provided, its sample skewness/excess-kurtosis feed the
        standard-error correction above. When omitted (None, default —
        preserves callers that only have a scalar Sharpe, e.g. when
        returns aren't available at the call site), skewness/excess
        kurtosis are assumed 0 (i.e. normal), the same assumption the
        prior simplified version made implicitly.

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

    if n_trials > 1:
        e_max = (
            (1.0 - _EULER_MASCHERONI) * norm.ppf(1 - 1.0 / n_trials)
            + _EULER_MASCHERONI * norm.ppf(1 - 1.0 / (n_trials * np.e))
        )
    else:
        e_max = 0.0

    skewness, excess_kurtosis = 0.0, 0.0
    if returns is not None and len(returns) >= 3:
        s, k = float(returns.skew()), float(returns.kurt())  # pandas .kurt() is already excess (normal = 0)
        if np.isfinite(s):
            skewness = s
        if np.isfinite(k):
            excess_kurtosis = k

    variance_term = 1.0 - skewness * sharpe + (excess_kurtosis / 4.0) * sharpe**2
    sr_std_error = np.sqrt(max(variance_term, 1e-12) / max(n_obs - 1, 1))

    dsr_stat = (sharpe - e_max / np.sqrt(n_obs)) / sr_std_error
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
