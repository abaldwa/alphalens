"""
systems/ml_signal_engine/models/hmm/regime_detector.py

Phase: 2.1 (HMM Redesign — 3-state, standardized, anchored)
Specs: SPEC-MODEL-003, SPEC-SOLID-003, SPEC-SOLID-004, SPEC-PIPE-004
Owner: ml_signal_engine / hmm
Consumers: features/matrix_builder (hmm_regime_* feature columns),
           backtest (BEAR_REGIME_POSITION_SCALE), systems/ml_signal_engine (Phase 1+),
           screener templates R1-R4

Redesigned HMM regime detector (2026-08-07) based on 6-agent model review:
  - 3 observables (de-redundant): daily_return, realized_vol_10d, volume_ratio_20d
  - 3 states: bearish (rank 0), sideways (rank 1), bullish (rank 2)
  - Z-score standardization before EM (fixes variance dominance)
  - Label anchoring via real-space mean return ranking (fixes permutation invariance)
  - MIN_OBSERVATIONS raised to 80 (safe for 3-state, 3-observable full covariance)

Previous design (5 observables, 4 states, no standardization, no anchoring) was
found untrustworthy: labels were non-reproducible (34% seed agreement on INFY),
assigned by return-rank but states separated on ATR/volume (99.6% variance in 2 dims),
and "volatile" (rank 2) was mislabeled as a volatility state when it was actually
the second-highest-mean-return state.

One detector instance is fit per ticker on that ticker's own 3 observables —
hmmlearn has no batch/panel-fit API, so this is a genuine per-entity model-fitting
loop (compute_hmm_regime_features below), not a vectorized-feature-arithmetic loop.
"""

import logging
from typing import Optional, Tuple, cast

import numpy as np
import pandas as pd
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

# 3 de-redundant observables (redesigned 2026-08-07 per 6-agent review):
#   daily_return     — primary return signal (log_return dropped: r=0.9998 with daily_return)
#   realized_vol_10d — 10-day realized volatility (atr_pct dropped: redundant, degenerate under circuit caps)
#   volume_ratio_20d — volume relative to 20-day SMA (retained as regime-quality signal)
OBSERVABLE_COLUMNS = ["daily_return", "realized_vol_10d", "volume_ratio_20d"]

N_STATES = 3  # bearish (rank 0), sideways (rank 1), bullish (rank 2)
REGIME_RANK_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "bullish"}

# 02_models.md's reference snippet uses 10-20 restarts x 1000 iterations for
# offline research-grade fits; reduced here for the *daily, per-ticker*
# production fit (every ticker refits "today only, not backfill" per
# CLAUDE.md's STEP 10) so a 500-stock universe stays well inside the
# 90-minute pipeline budget (SPEC-SYS-002). Override via the constructor
# for offline/research use.
DEFAULT_N_RESTARTS = 5
DEFAULT_N_ITER = 200
# 3-state, 3-observable full covariance: ~35 free params (3×9 mean+cov + 6 transitions + 2 init).
# Floor at 80 valid observations ≈ 2.3× params, safe for stable EM convergence.
MIN_OBSERVATIONS = 80


class HMMRegimeDetector(IRegimeModel):
    """
    GaussianHMM(n_components=3) regime detector for a single price series.

    Redesigned 2026-08-07 per 6-agent model review:
    - 3 observables (de-redundant): daily_return, realized_vol_10d, volume_ratio_20d
    - 3 states: bearish (rank 0), sideways (rank 1), bullish (rank 2)
    - Z-score standardization before EM (fixes variance dominance)
    - Label anchoring via real-space mean return ranking (fixes permutation invariance)

    States are labeled post-hoc by mean `daily_return` (ascending): rank 0
    = Bearish, 1 = Sideways, 2 = Bullish. The anchoring uses real-space
    (unscaled) mean returns so the ranking is stable across refits —
    "bullish" always corresponds to the physical state with the highest
    mean daily return, regardless of EM's internal permutation.

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
        self._mu: Optional[np.ndarray] = None   # z-score mean (per feature)
        self._sigma: Optional[np.ndarray] = None  # z-score std (per feature)

    def fit(self, X: pd.DataFrame) -> None:
        """
        Fit GaussianHMM to X[OBSERVABLE_COLUMNS], keeping the best of
        `n_restarts` random-seed fits by log-likelihood (best BIC proxy
        per 02_models.md's reference snippet).

        Features are z-score standardized before fitting to prevent
        variance dominance (fixes the 2800× ATR-over-return issue found
        in the 2026-08-07 model review). The scaler is fit on the window
        and stored for predict_regime to use the same transformation.

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

        # Z-score standardization — critical fix from model review.
        # Without this, ATR/variance dominates EM's likelihood and states
        # separate on volatility, not returns — mislabeling the regime.
        mu = obs.mean(axis=0)
        sigma = obs.std(axis=0)
        sigma[sigma < 1e-8] = 1.0  # guard against zero-variance (constant series)
        obs_scaled = (obs - mu) / sigma

        best_model, best_score = None, -np.inf
        for i in range(self.n_restarts):
            model = GaussianHMM(
                n_components=self.n_states,
                covariance_type="full",
                n_iter=self.n_iter,
                random_state=self.random_state + i,
            )
            try:
                model.fit(obs_scaled)
                score = model.score(obs_scaled)
            except Exception as exc:  # hmmlearn can raise on degenerate/singular covariance
                logger.debug(f"HMM restart {i} failed to converge: {exc}")
                continue
            if score > best_score:
                best_model, best_score = model, score

        if best_model is None:
            raise ValueError("all HMM restarts failed to converge")

        self._model = best_model
        self._mu = mu
        self._sigma = sigma

        # Label anchoring: rank states by REAL-SPACE (unscaled) mean daily return
        # so "bullish" always = highest mean return state, regardless of EM permutation.
        # This is the critical fix for the non-reproducible label problem.
        daily_return_idx = OBSERVABLE_COLUMNS.index("daily_return")
        real_means = best_model.means_[:, daily_return_idx] * sigma[daily_return_idx] + mu[daily_return_idx]
        self._state_order = np.argsort(real_means)

    def predict_regime(self, X: pd.DataFrame) -> Tuple[pd.Series, pd.DataFrame]:
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
                where undecodable), values in {0.0, 1.0, 2.0} = the
                bearish->bullish rank.
            probabilities : pd.DataFrame (n, n_states), columns
                'state_prob_0' (bearish) .. 'state_prob_2' (bullish),
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
            # Apply the SAME z-score standardization used during fit()
            obs_scaled = (obs - self._mu) / self._sigma

            raw_states = self._model.predict(obs_scaled)
            raw_probs = self._model.predict_proba(obs_scaled)

            rank_of_state = np.empty(self.n_states, dtype=int)
            rank_of_state[self._state_order] = np.arange(self.n_states)

            regimes.loc[valid] = rank_of_state[raw_states].astype(np.float64)
            probabilities.loc[valid, prob_cols] = raw_probs[:, self._state_order]

        return regimes, probabilities


def compute_hmm_observables(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the 3 per-ticker observables for the HMM regime detector.

    Redesigned 2026-08-07: dropped log_return (r=0.9998 with daily_return)
    and atr_pct (redundant with realized_vol; degenerate under circuit caps).

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel: date, ticker, open, high, low, close, volume.

    Returns
    -------
    pd.DataFrame
        `ohlcv` plus OBSERVABLE_COLUMNS (3 new float64 columns).

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

    # realized_vol_10d: 10-day rolling std of daily_return (annualized)
    df["realized_vol_10d"] = (
        df.groupby("ticker", sort=False)["daily_return"]
        .rolling(10, min_periods=10)
        .std()
        .reset_index(level=0, drop=True)
        * np.sqrt(252)
    )

    # volume_ratio_20d: volume relative to 20-day SMA
    vol_sma20 = (
        df.groupby("ticker", sort=False)["volume"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    df["volume_ratio_20d"] = df["volume"] / vol_sma20

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
    out["hmm_regime_prob_bullish"] = probs["state_prob_2"].to_numpy()  # rank 2 = bullish (3-state)
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


def _fit_and_decode_one_ticker_star(args: Tuple[str, pd.DataFrame, int, int]) -> pd.DataFrame:
    """Pickled-args shim for Pool.imap — spawn workers can't use starmap kwargs cleanly."""
    return _fit_and_decode_one_ticker(*args)


def compute_hmm_regime_features(
    ohlcv: pd.DataFrame,
    n_restarts: int = DEFAULT_N_RESTARTS,
    n_iter: int = DEFAULT_N_ITER,
    n_workers: int = 1,
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
    n_workers : int
        1 (default) keeps the original single-process loop, unchanged for
        every existing caller/test. >1 fits tickers concurrently via a
        spawn-context multiprocessing.Pool — spawn (not fork) so each
        worker starts fresh rather than inheriting the parent's RSS; each
        task ships only that ticker's own observable slice. Keep this
        conservative: this exact per-ticker-fit parallelization pattern
        (scripts/feature_backfill_hybrid.py) OOM-killed this machine twice
        (2026-06-26, confirmed via journalctl) at n_workers=10 against the
        501-ticker universe; the following day's run against the full
        ~2,644-ticker universe used n_workers=3 with no OOM. Default stays
        1 (opt-in) — callers on this machine should pass 3, not 10.

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
    groups = list(obs_df.groupby("ticker", sort=False))

    if n_workers <= 1 or len(groups) <= 1:
        parts = [
            _fit_and_decode_one_ticker(cast(str, ticker), g, n_restarts, n_iter)
            for ticker, g in groups
        ]
    else:
        import multiprocessing

        # CRITICAL: numpy/scipy (via hmmlearn's EM) already parallelize each
        # single fit internally through OpenBLAS/MKL -- confirmed empirically
        # (a "sequential" single-process fit here showed 55 OS threads and
        # ~350% average CPU on this machine, not the ~100% a naive single-
        # threaded loop would suggest). Stacking n_workers processes on top
        # without capping that inner thread pool causes severe oversubscription
        # (n_workers x BLAS's own thread count, all fighting for the same
        # cores) that made a real production run 3x SLOWER than the original
        # sequential loop despite the extra processes (measured: 20-ticker
        # benchmark went 331s sequential -> 257s parallel-unpinned -> 19.8s
        # parallel-pinned). Spawned children inherit os.environ at process
        # creation, before their own numpy import initializes BLAS, so
        # setting these here (before Pool creation) reliably caps each
        # worker to one BLAS thread and lets the outer process-level
        # parallelism actually win. Restored after the pool exits so this
        # doesn't leak into unrelated code running later in this process.
        _blas_env_vars = (
            "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS",
        )
        import os
        _prev_env = {var: os.environ.get(var) for var in _blas_env_vars}
        try:
            for var in _blas_env_vars:
                os.environ[var] = "1"

            worker_args = [(cast(str, ticker), g, n_restarts, n_iter) for ticker, g in groups]
            ctx = multiprocessing.get_context("spawn")
            with ctx.Pool(processes=n_workers) as pool:
                parts = list(pool.imap(_fit_and_decode_one_ticker_star, worker_args))
        finally:
            for var, val in _prev_env.items():
                if val is None:
                    os.environ.pop(var, None)
                else:
                    os.environ[var] = val

    result = pd.concat(parts, ignore_index=True)
    return result[["date", "ticker"] + HMM_REGIME_FEATURES]
