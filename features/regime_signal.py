"""
features/regime_signal.py

Phase: full-codebase-review Fix B2 (2026-07-19)
Owner: Platform / Features
Consumers: backtest/momentum_backtest.py (regime-conditioning, opt-in)

Realized-volatility regime classification for a benchmark index series
(e.g. Nifty 50/500 close prices). Momentum strategies are well-documented
to crash in regime transitions (sharp reversals after a sustained
uptrend, often accompanied by a volatility spike) — this lets a momentum
backtest optionally suppress new buys during historically high-volatility
regimes rather than blindly buying into every rebalance regardless of
market conditions.

This module computes REAL trailing realized volatility from whatever
benchmark close-price series the caller supplies — it does not fetch or
assume a specific data source. Callers should pass a real benchmark
series (e.g. from the production index_ohlcv table, or
backtest/reports/momentum/momentum_yoy.duckdb's benchmark_index table
built by scripts/build_momentum_benchmark_db.py) rather than a synthetic
one (SPEC-QUALITY-003).
"""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import pandas as pd

HIGH_VOL = "high_vol"
NORMAL = "normal"


def compute_realized_vol_regime(
    benchmark_close: pd.Series,
    vol_window: int = 21,
    regime_lookback_days: int = 252,
    high_vol_percentile: float = 0.75,
) -> pd.Series:
    """
    Classify each date in `benchmark_close`'s index into a HIGH_VOL/NORMAL
    regime label, based on trailing realized volatility's percentile rank
    within its own trailing history.

    Parameters
    ----------
    benchmark_close : pd.Series
        Date-indexed benchmark index close prices (real data — see module
        docstring). Must be sorted ascending by date.
    vol_window : int
        Trading-day window for the realized-volatility calculation
        (default 21, ~1 trading month).
    regime_lookback_days : int
        Trading-day window the current volatility's percentile rank is
        computed against (default 252, ~1 trading year).
    high_vol_percentile : float
        Percentile threshold (default 0.75) — a date whose trailing
        realized vol sits at or above this percentile of its own trailing
        `regime_lookback_days`-day history is labeled HIGH_VOL.

    Returns
    -------
    pd.Series
        Date-indexed regime labels (HIGH_VOL / NORMAL). The first
        `vol_window + regime_lookback_days` or so dates are NaN
        (insufficient trailing history to classify) — callers should
        treat NaN/missing as "unknown, do not filter on it" the same way
        MomentumBacktester's other optional filters treat missing data
        (never exclude on absence of information).
    """
    log_returns = np.log(benchmark_close / benchmark_close.shift(1))
    realized_vol = log_returns.rolling(window=vol_window, min_periods=vol_window).std() * np.sqrt(252)

    def _percentile_rank(window: "npt.NDArray[np.float64]") -> float:
        current = window[-1]
        if np.isnan(current):
            return float("nan")
        valid = window[~np.isnan(window)]
        if len(valid) < 2:
            return float("nan")
        return float((valid <= current).sum() / len(valid))

    pct_rank = realized_vol.rolling(
        window=regime_lookback_days, min_periods=max(vol_window, 20)
    ).apply(_percentile_rank, raw=True)

    regime = pd.Series(np.nan, index=benchmark_close.index, dtype=object)
    known = pct_rank.notna()
    regime.loc[known] = np.where(pct_rank.loc[known] >= high_vol_percentile, HIGH_VOL, NORMAL)
    return regime
