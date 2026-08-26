"""
features/volatility_weighting_features.py

Phase: R0 (traditional momentum + volatility-scaled position weighting)
Owner: Platform / Features
Consumers: backtest/adapters/momentum_adapter.py (weight_method sizing),
           systems/ml_signal_engine (raw risk features)

Stage-1 (per-ticker, no cross-ticker dependency) realized-volatility
features: annualized realized vol, realized variance, and downside
(semi-deviation) vol over a rolling window, plus the four raw scoring
inputs (inv_vol_score, inv_variance_score, downside_inv_vol_score,
target_vol_score) that feed R0's per-ticker basket weighting.

These are deliberately the RAW, basket-independent per-ticker values —
not the basket-normalized size_multiplier that
features/volatility_scaling.py's WEIGHT_DISPATCH produces at each
rebalance (that normalization depends on which tickers are in the basket
that day, so it isn't a reusable per-ticker feature; this module is).
Both share the same underlying realized-vol computation so backtest
sizing and this ML feature can never silently drift apart — the rolling
math here is the vectorized, whole-history equivalent of
features/momentum_signal.py::_daily_return_volatility's single-date
version.
"""

import logging
from typing import List

import numpy as np
import pandas as pd

from features._vector_utils import grouped_rolling

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252
VOLATILITY_WEIGHTING_LOOKBACK_DAYS = 126
TARGET_VOL_DEFAULT = 0.15

VOLATILITY_WEIGHTING_FEATURES: List[str] = [
    "realized_vol_126d",
    "realized_variance_126d",
    "downside_vol_126d",
    "inv_vol_score",
    "inv_variance_score",
    "downside_inv_vol_score",
    "target_vol_score",
]


def compute_volatility_weighting_features(
    ohlcv: pd.DataFrame,
    lookback_days: int = VOLATILITY_WEIGHTING_LOOKBACK_DAYS,
    target_vol: float = TARGET_VOL_DEFAULT,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel with columns: date, ticker, close. One or more
        tickers; each ticker's rows must be sorted ascending by date
        (Stage-1 contract — see features/hybrid_compute.py).
    lookback_days : int
        Rolling window for the realized-vol computation (default 126,
        ~6 months — matches R8/R9's default and R0's weight_lookback_days
        default in backtest/adapters/momentum_adapter.py).
    target_vol : float
        Target annualized volatility for target_vol_score (default 0.15,
        matching the R8/R0 default).

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker + VOLATILITY_WEIGHTING_FEATURES (7 cols),
        float64. All-NaN until `lookback_days` trading days of history
        exist for that ticker (no fabricated early-history values, per
        the no-mock-data policy) — same NaN-until-ready contract as every
        other Stage-1 module.
    """
    if ohlcv.empty:
        return pd.DataFrame(columns=["date", "ticker"] + VOLATILITY_WEIGHTING_FEATURES)

    df = ohlcv[["date", "ticker", "close"]].copy()
    log_return = np.log(df["close"] / df.groupby("ticker", sort=False)["close"].shift(1))
    df["_log_return"] = log_return
    df["_neg_return"] = df["_log_return"].clip(upper=0.0)

    realized_vol_daily = grouped_rolling(df, "_log_return", lookback_days, "std")
    downside_vol_daily = grouped_rolling(df, "_neg_return", lookback_days, "std")

    realized_vol = realized_vol_daily * (TRADING_DAYS_PER_YEAR ** 0.5)
    downside_vol = downside_vol_daily * (TRADING_DAYS_PER_YEAR ** 0.5)
    realized_variance = realized_vol ** 2

    # 0.0 vol (e.g. a circuit-locked/no-move window) → NaN rather than
    # divide-by-zero, same convention as features/volatility_scaling.py.
    realized_vol_safe = realized_vol.replace(0.0, np.nan)
    realized_variance_safe = realized_variance.replace(0.0, np.nan)
    downside_vol_safe = downside_vol.replace(0.0, np.nan)

    out = pd.DataFrame({
        "date": df["date"].values,
        "ticker": df["ticker"].values,
        "realized_vol_126d": realized_vol.values,
        "realized_variance_126d": realized_variance.values,
        "downside_vol_126d": downside_vol.values,
        "inv_vol_score": (1.0 / realized_vol_safe).values,
        "inv_variance_score": (1.0 / realized_variance_safe).values,
        "downside_inv_vol_score": (1.0 / downside_vol_safe).values,
        "target_vol_score": (target_vol / realized_vol_safe).values,
    })
    return out
