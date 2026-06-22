"""
features/intraday.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-FEAT-001, SPEC-PIPE-004
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine

Computes intraday OHLC-pattern features (`01_features.md` Category 12 /
`02_models.md`'s "8 intraday" bucket in the Signal 5d input formula:
"76 core technical + 8 intraday + 7 calendar + 6 HMM + 14 macro = 111").

Column-overlap note (read before adding callers): Category 12's 8 named
features are `gap_up_pct`, `gap_down_pct`, `intraday_reversal_score`,
`upper_shadow_pct`, `lower_shadow_pct`, `body_to_range_ratio`, `close_
position_in_range`, `opening_drive_strength`. The original P1.1 build
prompt's "Category 11 (Derived/Engineered)" already merged 5 of those 8
names into `features/technical.py` (`gap_up_pct`, `gap_down_pct`,
`intraday_reversal_score`, `close_position_in_range`, `body_to_range_
ratio`) under different category bookkeeping. Recomputing them here too
would produce duplicate column names when `features/matrix_builder.py`
concatenates module outputs. Rather than silently duplicate or risk
regressing the already-tested `technical.py`, this module exposes only the
3 genuinely net-new names — `upper_shadow_pct`, `lower_shadow_pct`,
`opening_drive_strength` — and documents the other 5 as already owned by
`technical.py`. Net result: 70 (technical) + 3 (intraday) + 7 (calendar)
+ 14 (macro) + 6 (HMM) = 100 total feature columns, not the doc's literal
111 — see BuildLog.md "P1.2" for the full accounting of every count
discrepancy across `01_features.md` / `02_models.md` / `CLAUDE.md`.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close"]

INTRADAY_FEATURES = ["upper_shadow_pct", "lower_shadow_pct", "opening_drive_strength"]


def compute_intraday_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the 3 net-new intraday OHLC-pattern features (see module docstring).

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel with columns: date, ticker, open, high, low, close.
        One row per (ticker, date); no lookback history is required (every
        feature here is computed from that single day's own OHLC).

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker + INTRADAY_FEATURES (3 cols), float64, no
        infinities (zero-range days, e.g. circuit-locked stocks, yield NaN
        rather than a divide-by-zero).

    Spec References
    ----------------
    SPEC-PIPE-004: fully vectorized — pure elementwise numpy/pandas
    arithmetic over the whole panel at once, no per-ticker iteration at all
    (unlike technical.py, nothing here needs a rolling window or per-ticker
    recurrence).

    PIT Assumptions
    ----------------
    None — same-day OHLC is always contemporaneously knowable (PITRule.NONE).

    Raises
    ------
    ValueError
        If `ohlcv` is missing any of REQUIRED_OHLCV_COLUMNS.
    """
    missing = [c for c in REQUIRED_OHLCV_COLUMNS if c not in ohlcv.columns]
    if missing:
        raise ValueError(f"ohlcv is missing required columns: {missing}")

    df = ohlcv.copy()
    rng = (df["high"] - df["low"]).astype(np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        upper_shadow = (df["high"] - np.maximum(df["open"], df["close"])) / rng
        lower_shadow = (np.minimum(df["open"], df["close"]) - df["low"]) / rng

        # "Opening drive" proxy (01_features.md gives no closed-form formula,
        # only "Proxy from OHLC (see features/intraday.py)"): direction of
        # the move away from the open, scaled down by how much of that move
        # was given back by the close. A close sitting at the day's extreme
        # in the open's breakout direction scores near +/-1 (a clean drive,
        # no reversal); a close that round-trips back toward/through the
        # open scores near 0 (no sustained drive).
        direction = np.sign(df["close"] - df["open"])
        extreme_in_direction = np.where(direction >= 0, df["high"], df["low"])
        giveback = np.abs(extreme_in_direction - df["close"])
        opening_drive = direction * (1 - giveback / rng)

    out = pd.DataFrame(
        {
            "date": df["date"],
            "ticker": df["ticker"],
            "upper_shadow_pct": upper_shadow,
            "lower_shadow_pct": lower_shadow,
            "opening_drive_strength": opening_drive,
        }
    )
    for col in INTRADAY_FEATURES:
        out[col] = out[col].astype(np.float64).replace([np.inf, -np.inf], np.nan)

    return out
