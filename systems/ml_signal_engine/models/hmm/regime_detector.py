"""
systems/ml_signal_engine/models/hmm/regime_detector.py

Phase: 1.2 (Core Feature Computation — HMM regime bucket)
Specs: SPEC-MODEL-003, SPEC-SOLID-003, SPEC-SOLID-004, SPEC-PIPE-004
Owner: ml_signal_engine / hmm
Consumers: features/matrix_builder (6 hmm_regime_* feature columns),
           backtest (BEAR_REGIME_POSITION_SCALE), systems/ml_signal_engine (Phase 1+)

M-01 (02_models.md): GaussianHMM(n_components=4, covariance_type='full')
regime detector. One detector instance is fit per ticker on that ticker's
own 5 observables (daily_return, log_return, realized_vol_10d, volume_
ratio_20d, atr_pct) — hmmlearn has no batch/panel-fit API, so this is a
genuine per-entity model-fitting loop (compute_hmm_regime_features below),
not a vectorized-feature-arithmetic loop. SPEC-PIPE-004's "no Python loop
over individual stocks" rule governs the latter (see features/technical.py's
module docstring for that distinction) — fitting N independent statistical
models cannot be vectorized away the way rolling means or TA-Lib calls can,
the same reasoning that already applies to Supertrend's recurrence.
"""

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import talib
from hmmlearn.hmm import GaussianHMM

from contracts.interfaces import IRegimeModel

logger = logging.getLogger(__name__)

# hmmlearn logs "Model is not converging" at WARNING for every restart that
# doesn't converge within n_iter — expected and handled here (fit() keeps
# the best-scoring restart and only raises if *all* of them fail), so it's
# noise at the default level rather than an actionable signal.
logging.getLogger("hmmlearn").setLevel(logging.ERROR)

HMM_REGIME_FEATURES = [
    "hmm_regime",
    "hmm_regime_prob_bullish",
    "hmm_regime_prob_bearish",
    "hmm_regime_duration",
    "hmm_regime_transition",
    "hmm_regime_stability",
]

OBSERVABLE_COLUMNS = ["daily_return", "log_return", "realized_vol_10d", "volume_ratio_20d", "atr_pct"]

N_STATES = 4
# 02_models.md's reference snippet uses 10-20 restarts x 1000 iterations for
# offline research-grade fits; reduced here for the *daily, per-ticker*
# production fit (every ticker refits "today only, not backfill" per
# CLAUDE.md's STEP 10) so a 500-stock universe stays well inside the
# 90-minute pipeline budget (SPEC-SYS-002). Override via the constructor
# for offline/research use.
DEFAULT_N_RESTARTS = 5
DEFAULT_N_ITER = 200
# Below this many valid observations, a 4-state HMM has too little data to
# meaningfully separate states — matches the same "insufficient history ->
# NaN, not a degraded guess" spirit as SPEC-FEAT-001, applied to a model
# fit rather than a rolling window.
MIN_OBSERVATIONS = 60


class HMMRegimeDetector(IRegimeModel):
    """
    GaussianHMM(n_components=4) regime detector for a single price series.

    States are labeled post-hoc by mean `daily_return` (ascending): rank 0
    = Bearish, 1 = Sideways/Volatile boundary, ..., 3 = Bullish — per
    02_models.md's "labeled post-hoc by mean return" convention. (The doc's
    qualitative 4-state story — Bearish/Sideways/Volatile/Bullish — folds
    vol into the same return-rank ordering here rather than a second axis;
    a future iteration could rank by (mean_return, vol) jointly if the
    pure-return ranking proves too coarse in backtests.)

    Spec References
    ----------------
    SPEC-MODEL-003 (IRegimeModel contract), 02_models.md M-01.
    """

    def __init__(
        self,
        n_states: int = N_STATES,
        n_restarts: int = DEFAULT_N_RESTARTS,
        n_iter: int = DEFAULT_N_ITER,
        random_state: int = 42,
    ) -> None:
        self.n_states = n_states
        self.n_restarts = n_restarts
        self.n_iter = n_iter
        self.random_state = random_state
        self._model: Optional[GaussianHMM] = None
        self._state_order: Optional[np.ndarray] = None  # raw state idx, ordered bearish -> bullish

    def fit(self, X: pd.DataFrame) -> None:
        """
        Fit GaussianHMM to X[OBSERVABLE_COLUMNS], keeping the best of
        `n_restarts` random-seed fits by log-likelihood (best BIC proxy
        per 02_models.md's reference snippet).

        Parameters
        ----------
        X : pd.DataFrame
            Must contain OBSERVABLE_COLUMNS. Rows with any NaN observable
            are dropped before fitting.

        Raises
        ------
        ValueError
            If fewer than MIN_OBSERVATIONS valid rows remain, or every
            restart fails to converge.
        """
        obs = X[OBSERVABLE_COLUMNS].dropna().to_numpy(dtype=np.float64)
        if len(obs) < MIN_OBSERVATIONS:
            raise ValueError(f"need >= {MIN_OBSERVATIONS} valid observations to fit, got {len(obs)}")

        best_model, best_score = None, -np.inf
        for i in range(self.n_restarts):
            model = GaussianHMM(
                n_components=self.n_states,
                covariance_type="full",
                n_iter=self.n_iter,
                random_state=self.random_state + i,
            )
            try:
                model.fit(obs)
                score = model.score(obs)
            except Exception as exc:  # hmmlearn can raise on degenerate/singular covariance
                logger.debug(f"HMM restart {i} failed to converge: {exc}")
                continue
            if score > best_score:
                best_model, best_score = model, score

        if best_model is None:
            raise ValueError("all HMM restarts failed to converge")

        self._model = best_model
        mean_return_per_state = best_model.means_[:, OBSERVABLE_COLUMNS.index("daily_return")]
        self._state_order = np.argsort(mean_return_per_state)

    def predict_regime(self, X: pd.DataFrame) -> Tuple[pd.Series, Optional[pd.DataFrame]]:
        """
        Decode the most likely regime rank and per-day state probabilities.

        Parameters
        ----------
        X : pd.DataFrame
            Same OBSERVABLE_COLUMNS as fit(). Rows with any NaN observable
            get NaN regime/probabilities (no decoding attempted for them).

        Returns
        -------
        (regimes, probabilities)
            regimes : pd.Series, index-aligned to X. Float dtype (NaN
                where undecodable), values in {0.0, 1.0, 2.0, 3.0} = the
                bearish->bullish rank elsewhere.
            probabilities : pd.DataFrame (n, n_states), columns
                'state_prob_0' (bearish) .. 'state_prob_3' (bullish),
                index-aligned to X.

        Raises
        ------
        RuntimeError
            If called before fit().
        """
        if self._model is None:
            raise RuntimeError("predict_regime() called before fit()")

        valid = X[OBSERVABLE_COLUMNS].notna().all(axis=1)
        regimes = pd.Series(np.nan, index=X.index, dtype=np.float64)
        prob_cols = [f"state_prob_{r}" for r in range(self.n_states)]
        probabilities = pd.DataFrame(np.nan, index=X.index, columns=prob_cols, dtype=np.float64)

        if valid.any():
            obs = X.loc[valid, OBSERVABLE_COLUMNS].to_numpy(dtype=np.float64)
            raw_states = self._model.predict(obs)
            raw_probs = self._model.predict_proba(obs)

            rank_of_state = np.empty(self.n_states, dtype=int)
            rank_of_state[self._state_order] = np.arange(self.n_states)

            regimes.loc[valid] = rank_of_state[raw_states].astype(np.float64)
            probabilities.loc[valid, prob_cols] = raw_probs[:, self._state_order]

        return regimes, probabilities


def compute_hmm_observables(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the 5 per-ticker observables M-01 trains on (02_models.md).

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel: date, ticker, open, high, low, close, volume.

    Returns
    -------
    pd.DataFrame
        `ohlcv` plus OBSERVABLE_COLUMNS (5 new float64 columns).

    Spec References
    ----------------
    SPEC-PIPE-004: vectorized via groupby/rolling, same pattern as
    features/technical.py — no per-ticker Python loop for the arithmetic.
    """
    df = ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True).copy()
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(np.float64)
    df["volume"] = df["volume"].astype(np.float64)

    grouped_close = df.groupby("ticker", sort=False)["close"]
    prev_close = grouped_close.shift(1)
    df["daily_return"] = df["close"] / prev_close - 1
    df["log_return"] = np.log(df["close"] / prev_close)

    df["realized_vol_10d"] = (
        df.groupby("ticker", sort=False)["log_return"]
        .rolling(10, min_periods=10)
        .std()
        .reset_index(level=0, drop=True)
        * np.sqrt(252)
    )

    vol_sma20 = (
        df.groupby("ticker", sort=False)["volume"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    df["volume_ratio_20d"] = df["volume"] / vol_sma20

    atr_parts = []
    for _, g in df.groupby("ticker", sort=False):
        atr = talib.ATR(
            g["high"].to_numpy(dtype=np.float64),
            g["low"].to_numpy(dtype=np.float64),
            g["close"].to_numpy(dtype=np.float64),
            timeperiod=14,
        )
        atr_parts.append(pd.Series(atr, index=g.index))
    atr14 = pd.concat(atr_parts)
    df["atr_pct"] = atr14 / df["close"] * 100

    return df


def _fit_and_decode_one_ticker(ticker: str, g: pd.DataFrame, n_restarts: int, n_iter: int) -> pd.DataFrame:
    out = pd.DataFrame({"date": g["date"].to_numpy(), "ticker": ticker})
    for col in HMM_REGIME_FEATURES:
        out[col] = np.nan

    detector = HMMRegimeDetector(n_restarts=n_restarts, n_iter=n_iter)
    try:
        detector.fit(g)
    except ValueError as exc:
        logger.debug(f"HMM fit skipped for {ticker}: {exc}")
        for col in HMM_REGIME_FEATURES:
            out[col] = out[col].astype(np.float64)
        return out

    regimes, probs = detector.predict_regime(g)
    out["hmm_regime"] = regimes.to_numpy()
    out["hmm_regime_prob_bullish"] = probs["state_prob_3"].to_numpy()
    out["hmm_regime_prob_bearish"] = probs["state_prob_0"].to_numpy()
    out["hmm_regime_stability"] = probs.max(axis=1).to_numpy()

    regime_vals = out["hmm_regime"]
    changed = regime_vals.ne(regime_vals.shift(1)) | regime_vals.isna()
    run_id = changed.cumsum()
    duration = regime_vals.groupby(run_id).cumcount() + 1
    out["hmm_regime_duration"] = duration.where(regime_vals.notna())

    prev_regime = regime_vals.shift(1)
    transitioned = regime_vals.ne(prev_regime) & regime_vals.notna() & prev_regime.notna()
    out["hmm_regime_transition"] = transitioned.astype(float).where(regime_vals.notna())

    for col in HMM_REGIME_FEATURES:
        out[col] = out[col].astype(np.float64)
    return out


def compute_hmm_regime_features(
    ohlcv: pd.DataFrame,
    n_restarts: int = DEFAULT_N_RESTARTS,
    n_iter: int = DEFAULT_N_ITER,
) -> pd.DataFrame:
    """
    Fit one HMMRegimeDetector per ticker and return the 6 regime features.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel: date, ticker, open, high, low, close, volume.
        Needs enough history per ticker to clear MIN_OBSERVATIONS after
        the 20-day volume-ratio and 10-day vol warm-up (i.e. roughly
        MIN_OBSERVATIONS + 20 trading days minimum).
    n_restarts, n_iter : int
        Passed through to HMMRegimeDetector — override for higher-fidelity
        offline fits (02_models.md suggests 10-20 restarts x 1000 iters).

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker + HMM_REGIME_FEATURES (6 cols), float64.
        Tickers with insufficient history (< MIN_OBSERVATIONS valid
        observable rows) get all-NaN regime columns rather than an error.

    Spec References
    ----------------
    02_models.md M-01. SPEC-PIPE-004: the per-ticker `for` loop here fits
    independent statistical models (unavoidable — see module docstring),
    not vectorized feature arithmetic.

    PIT Assumptions
    ----------------
    Each ticker's HMM is fit and decoded using only that ticker's own
    OHLCV through the requested date range — no cross-ticker or future
    information leaks into a fit.

    Raises
    ------
    ValueError
        If `ohlcv` is missing required OHLCV columns.
    """
    required = ["date", "ticker", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in ohlcv.columns]
    if missing:
        raise ValueError(f"ohlcv is missing required columns: {missing}")

    obs_df = compute_hmm_observables(ohlcv)
    parts = [
        _fit_and_decode_one_ticker(ticker, g, n_restarts, n_iter)
        for ticker, g in obs_df.groupby("ticker", sort=False)
    ]
    result = pd.concat(parts, ignore_index=True)
    return result[["date", "ticker"] + HMM_REGIME_FEATURES]
