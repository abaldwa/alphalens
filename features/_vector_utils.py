"""
features/_vector_utils.py

Small shared vectorized helpers used across multiple feature-computation
modules (technical.py, pnd_features.py, multibagger.py). Extracted to
avoid maintaining three copies of the same elementwise-division helper
(SPEC-PIPE-004 code-reuse guidance) — no behavior change from the
original per-module copies.
"""

import numpy as np
import pandas as pd


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Elementwise division that yields NaN (not a RuntimeWarning/inf) on 0/0 or x/0."""
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator.to_numpy(dtype=np.float64) / denominator.to_numpy(dtype=np.float64)
    return pd.Series(result, index=numerator.index).replace([np.inf, -np.inf], np.nan)
