"""
features/_vector_utils.py

Small shared vectorized helpers used across multiple feature-computation
modules (technical.py, pnd_features.py, multibagger.py). Extracted to
avoid maintaining three copies of the same helpers (SPEC-PIPE-004
code-reuse guidance) — no behavior change from the original per-module
copies.
"""

from typing import Any, Callable, List, Optional, TypeVar

import numpy as np
import pandas as pd

# apply_per_ticker preserves whatever pandas object fn returns (Series or
# DataFrame); a TypeVar keeps that link instead of widening to Any.
_FrameT = TypeVar("_FrameT", pd.Series, pd.DataFrame)


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Elementwise division that yields NaN (not a RuntimeWarning/inf) on 0/0 or x/0."""
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator.to_numpy(dtype=np.float64) / denominator.to_numpy(dtype=np.float64)
    return pd.Series(result, index=numerator.index).replace([np.inf, -np.inf], np.nan)


def grouped_rolling(df: pd.DataFrame, col: str, window: int, how: str, min_periods: Optional[int] = None) -> pd.Series:
    """Per-ticker rolling aggregate, full window required by default (SPEC-FEAT-001 NaN-until-ready)."""
    grouped = df.groupby("ticker", sort=False)[col].rolling(window, min_periods=min_periods or window)
    return getattr(grouped, how)().reset_index(level=0, drop=True)


def grouped_shift(df: pd.DataFrame, col: str, periods: int) -> pd.Series:
    return df.groupby("ticker", sort=False)[col].shift(periods)


def apply_per_ticker(df: pd.DataFrame, fn: Callable[[pd.DataFrame], _FrameT]) -> _FrameT:
    """
    Apply fn(ticker_group) -> Series/DataFrame per ticker, concatenating results.

    Deliberately avoids `df.groupby('ticker').apply(fn)`: when the input
    has exactly one ticker and fn returns a Series, pandas' apply silently
    reshapes the result into a single wide row instead of concatenating it
    as a per-row Series (a long-standing pandas footgun, not a bug in fn).
    Caught by tests/unit/test_features_technical.py's single-ticker
    minimum-history test. A plain per-group loop + pd.concat sidesteps it
    while remaining the same "per-ticker dispatch, not per-stock Python
    feature-math loop" pattern used throughout this module (SPEC-PIPE-004).
    """
    parts = [fn(g) for _, g in df.groupby("ticker", sort=False)]
    out: _FrameT = pd.concat(parts)
    return out


def grouped_talib_single(
    df: pd.DataFrame, cols: List[str], fn: Callable[..., Any], **kwargs: Any
) -> pd.Series:
    """Apply a single-output TA-Lib function per ticker (vectorized C call per group)."""

    def _one(g: pd.DataFrame) -> pd.Series:
        arrays = [g[c].to_numpy(dtype=np.float64) for c in cols]
        return pd.Series(fn(*arrays, **kwargs), index=g.index)

    return apply_per_ticker(df, _one)


def grouped_talib_multi(
    df: pd.DataFrame,
    cols: List[str],
    fn: Callable[..., Any],
    out_names: List[str],
    **kwargs: Any,
) -> pd.DataFrame:
    """Apply a multi-output TA-Lib function (e.g. MACD, STOCH, BBANDS) per ticker."""

    def _one(g: pd.DataFrame) -> pd.DataFrame:
        arrays = [g[c].to_numpy(dtype=np.float64) for c in cols]
        outs = fn(*arrays, **kwargs)
        return pd.DataFrame({name: arr for name, arr in zip(out_names, outs)}, index=g.index)

    return apply_per_ticker(df, _one)
