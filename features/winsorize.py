"""
features/winsorize.py

Winsorization utilities for momentum signal processing.

Winsorization caps extreme values at specified percentiles, reducing the
influence of outliers while preserving the rank order of non-extreme values.
Used for momentum signals (section 8.4 of spec) and other factors where
extreme scores can distort rankings.
"""

from typing import Tuple

import pandas as pd


def winsorize_series(
    series: pd.Series,
    lower_pct: float = 0.05,
    upper_pct: float = 0.95,
) -> Tuple[pd.Series, int, int, int]:
    """Winsorize a Series at specified percentiles.

    Winsorization clips extreme values:
    - Values below the lower percentile are set to the lower percentile value.
    - Values above the upper percentile are set to the upper percentile value.
    - Values within the range are unchanged.

    Args:
        series: Input Series (typically momentum scores or volatility).
        lower_pct: Lower percentile cutoff, e.g., 0.05 for 5th percentile.
        upper_pct: Upper percentile cutoff, e.g., 0.95 for 95th percentile.

    Returns:
        Tuple of:
        - winsorized: Series with extreme values clipped.
        - n_lower_clipped: Count of values set to the lower bound.
        - n_upper_clipped: Count of values set to the upper bound.
        - n_total: Total non-NaN values in input.
    """
    # Drop NaN to compute percentiles cleanly.
    clean = series.dropna()
    n_total = len(clean)

    if n_total == 0:
        return series.copy(), 0, 0, 0

    # Compute bounds from the clean data.
    lower_bound = clean.quantile(lower_pct)
    upper_bound = clean.quantile(upper_pct)

    # Count how many will be clipped before clipping.
    n_lower_clipped = (clean < lower_bound).sum()
    n_upper_clipped = (clean > upper_bound).sum()

    # Apply clipping, preserving NaN positions.
    winsorized = series.clip(lower=lower_bound, upper=upper_bound)

    return winsorized, int(n_lower_clipped), int(n_upper_clipped), n_total
