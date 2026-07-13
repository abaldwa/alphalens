"""
systems/ml_signal_engine/training/labeling.py

Phase: 1
Specs: SPEC-MODEL-002, SPEC-MODEL-006, SPEC-LIB-004
Owner: ml_signal_engine / training
Consumers: systems/ml_signal_engine/models/signal/* (M-02, M-03),
           systems/ml_signal_engine/models/multibagger/multibagger_model.py (M-08)

Native triple-barrier label construction (SPEC-MODEL-002). mlfinlab is
intentionally NOT a dependency: it is unavailable on PyPI under any version
(the vendor, Hudson & Thames, made it commercial/private and pulled the
public package). The triple-barrier method itself is the only requirement —
not the package — and is reimplemented here directly, fully vectorized.

Phase 1.4 added TripleBarrierLabeler, a class wrapper around
compute_triple_barrier_labels (kept below, unmodified — SPEC-SOLID-002:
add, don't modify) carrying the SPEC-MODEL-002 default barriers
(profit_multiplier=2.0, stop_multiplier=1.0, max_holding=21), a
multi-ticker panel entry point, validation, and the class-distribution
report.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_triple_barrier_labels(
    close: pd.Series,
    atr: pd.Series,
    horizon_days: int,
    profit_multiplier: float,
    stop_multiplier: float,
    vertical_barrier_days: int,
    pnd_block: pd.Series | None = None,
) -> pd.Series:
    """
    Compute triple-barrier labels for one stock's close-price path.

    For each date t, an upper barrier (close[t] + profit_multiplier * atr[t])
    and a lower barrier (close[t] - stop_multiplier * atr[t]) are checked
    against the forward close path over the next `vertical_barrier_days`
    trading days. The label reflects whichever barrier is touched first.

    Parameters
    ----------
    close : pd.Series
        Daily close prices, point-in-time / corporate-action-adjusted,
        sorted ascending by date, with a date-like index.
    atr : pd.Series
        Average True Range aligned to the same index as `close`. Sets the
        barrier width: upper = close + profit_multiplier * atr,
        lower = close - stop_multiplier * atr.
    horizon_days : int
        Label horizon in exact trading days (5, 21, or 63 per
        SPEC-MODEL-002). Kept as a distinct argument from
        `vertical_barrier_days` for callers that want them decoupled; for
        M-02/M-03 the two are equal.
    profit_multiplier : float
        ATR multiplier for the upper (profit) barrier.
    stop_multiplier : float
        ATR multiplier for the lower (stop) barrier.
    vertical_barrier_days : int
        Number of trading days to look forward before the vertical (time)
        barrier is hit.
    pnd_block : pd.Series, optional
        Boolean series aligned to `close`, True where the entry date is
        pump-and-dump blocked (pnd_score > 60, SPEC-MODEL-006). Where True,
        a would-be +1 label is downgraded to 0 — P&D episodes are excluded
        from positive labels.

    Returns
    -------
    pd.Series
        Float64 labels in {-1.0, 0.0, 1.0} indexed like `close`. The final
        `vertical_barrier_days` rows are NaN — there is not enough forward
        history to resolve a label for them, so no label is invented.

    Spec References
    ----------------
    SPEC-MODEL-002: Triple-barrier label construction — exact horizon,
        ATR-multiple barriers, {-1, 0, +1} labels.
    SPEC-MODEL-006: P&D episodes excluded from positive labels.
    SPEC-LIB-004: mlfinlab not used (unavailable on PyPI); reimplemented
        natively since the algorithm, not the package, is the requirement.

    PIT Assumptions
    ----------------
    This function is purely retrospective over the path the caller supplies
    — it does not itself enforce point-in-time correctness. The caller MUST
    pass an already PIT-correct, corporate-action-adjusted `close` series
    (SPEC-PIPE-002, SPEC-PIPE-003). No row's label uses price data beyond
    `vertical_barrier_days` trading days after that row's date.

    Raises
    ------
    ValueError
        If `close`/`atr`/`pnd_block` are not aligned on the same index, or
        if horizon/multiplier arguments are not positive.
    """
    if horizon_days <= 0 or vertical_barrier_days <= 0:
        raise ValueError("horizon_days and vertical_barrier_days must be positive")
    if profit_multiplier <= 0 or stop_multiplier <= 0:
        raise ValueError("profit_multiplier and stop_multiplier must be positive")
    if not close.index.equals(atr.index):
        raise ValueError("close and atr must share the same index")
    if pnd_block is not None and not close.index.equals(pnd_block.index):
        raise ValueError("close and pnd_block must share the same index")

    n = len(close)
    labels = pd.Series(np.nan, index=close.index, dtype="float64")

    if n <= vertical_barrier_days:
        return labels

    close_vals = close.to_numpy(dtype="float64")
    atr_vals = atr.to_numpy(dtype="float64")

    upper = close_vals + profit_multiplier * atr_vals
    lower = close_vals - stop_multiplier * atr_vals

    n_valid = n - vertical_barrier_days
    # forward[i, k] = close[i + 1 + k] for k in [0, vertical_barrier_days)
    forward = np.lib.stride_tricks.sliding_window_view(
        close_vals[1:], vertical_barrier_days
    )[:n_valid]

    upper_hit = forward > upper[:n_valid, None]
    lower_hit = forward < lower[:n_valid, None]

    any_upper = upper_hit.any(axis=1)
    any_lower = lower_hit.any(axis=1)
    # argmax on a boolean array returns the first True index; the
    # np.where guards against the all-False case, where argmax would
    # otherwise (meaninglessly) return 0.
    first_upper = np.where(any_upper, upper_hit.argmax(axis=1), vertical_barrier_days)
    first_lower = np.where(any_lower, lower_hit.argmax(axis=1), vertical_barrier_days)

    # A same-day tie (first_upper == first_lower on an actual hit) is
    # impossible: upper > lower always, so one forward close cannot be
    # simultaneously above upper and below lower. Ties only occur when
    # neither barrier is hit (both sentinels equal vertical_barrier_days),
    # which correctly resolves to 0 below.
    out = np.zeros(n_valid, dtype="float64")
    out[first_upper < first_lower] = 1.0
    out[first_lower < first_upper] = -1.0

    if pnd_block is not None:
        block_vals = pnd_block.to_numpy(dtype=bool)[:n_valid]
        out[(out == 1.0) & block_vals] = 0.0

    labels.iloc[:n_valid] = out
    return labels


class TripleBarrierLabeler:
    """
    SPEC-MODEL-002 class wrapper: carries the default barrier
    configuration (profit_multiplier=2.0, stop_multiplier=1.0,
    max_holding=21 trading days) so callers don't have to repeat these at
    every call site, adds explicit post-hoc validation, multi-ticker panel
    labeling, and the class-distribution report. All actual label
    computation is delegated to compute_triple_barrier_labels — this
    class does not duplicate that logic.
    """

    def __init__(self, profit_multiplier: float = 2.0, stop_multiplier: float = 1.0, max_holding: int = 21) -> None:
        if profit_multiplier <= 0 or stop_multiplier <= 0:
            raise ValueError("profit_multiplier and stop_multiplier must be positive")
        if max_holding <= 0:
            raise ValueError("max_holding must be positive")
        self.profit_multiplier = profit_multiplier
        self.stop_multiplier = stop_multiplier
        self.max_holding = max_holding

    def label(
        self,
        close: pd.Series,
        atr: pd.Series,
        horizon_days: Optional[int] = None,
        pnd_block: Optional[pd.Series] = None,
    ) -> pd.Series:
        """
        Label one stock's close-price path using this labeler's barrier
        configuration.

        Parameters
        ----------
        close, atr, pnd_block : see compute_triple_barrier_labels.
        horizon_days : int, optional
            Defaults to self.max_holding when omitted — for M-02/M-03 the
            label horizon and the vertical barrier are the same value.

        Returns
        -------
        pd.Series
            Float64 labels in {-1.0, 0.0, 1.0}, NaN for the unresolvable tail.

        Raises
        ------
        ValueError
            Propagated from compute_triple_barrier_labels, or raised here
            if validate() finds a label outside {-1, 0, 1}.
        """
        horizon = horizon_days if horizon_days is not None else self.max_holding
        labels = compute_triple_barrier_labels(
            close, atr, horizon, self.profit_multiplier, self.stop_multiplier, self.max_holding, pnd_block
        )
        self.validate(labels)
        return labels

    def label_panel(
        self,
        df: pd.DataFrame,
        close_col: str = "close",
        atr_col: str = "atr_14",
        ticker_col: str = "ticker",
        pnd_block_col: Optional[str] = None,
        horizon_days: Optional[int] = None,
    ) -> pd.Series:
        """
        Label a multi-ticker long-format panel, one independent
        triple-barrier pass per ticker (each ticker's forward path is
        unrelated to every other ticker's — this is per-entity dispatch,
        not vectorized-across-tickers feature arithmetic, same SPEC-PIPE-004
        distinction made in features/technical.py for Supertrend/HMM).

        Parameters
        ----------
        df : pd.DataFrame
            Must contain ticker_col, close_col, atr_col (and pnd_block_col
            if given), sorted or not — sorted internally by
            (ticker_col, its row order) before grouping.
        close_col, atr_col, ticker_col, pnd_block_col : str
            Column names.
        horizon_days : int, optional
            Defaults to self.max_holding.

        Returns
        -------
        pd.Series
            Labels aligned to df's index (NaN for unresolvable tail rows
            of each ticker).

        Raises
        ------
        ValueError
            If required columns are missing.
        """
        required = [ticker_col, close_col, atr_col] + ([pnd_block_col] if pnd_block_col else [])
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"df is missing required columns: {missing}")

        parts = []
        for _, group in df.groupby(ticker_col, sort=False):
            pnd = group[pnd_block_col] if pnd_block_col else None
            labels = self.label(group[close_col], group[atr_col], horizon_days, pnd)
            parts.append(labels)
        return pd.concat(parts).reindex(df.index)

    def validate(self, labels: pd.Series) -> None:
        """
        SPEC-MODEL-002: labels must be in {-1, 0, 1} (NaN allowed only for
        the unresolvable tail).

        Raises
        ------
        ValueError
            If any non-NaN label falls outside {-1.0, 0.0, 1.0}.
        """
        non_nan = labels.dropna()
        if non_nan.empty:
            return
        invalid = ~non_nan.isin([-1.0, 0.0, 1.0])
        if invalid.any():
            bad_values = sorted(non_nan[invalid].unique().tolist())
            raise ValueError(f"labels contain values outside {{-1, 0, 1}}: {bad_values}")

    def class_distribution_report(self, labels: pd.Series, print_report: bool = True) -> Dict[float, float]:
        """
        SPEC-MODEL-004-style imbalance visibility: % of each class.

        Parameters
        ----------
        labels : pd.Series
            Output of label()/label_panel().
        print_report : bool
            If True (default), also prints a human-readable summary.

        Returns
        -------
        dict
            {label_value: percentage}, e.g. {1.0: 12.3, 0.0: 75.1, -1.0: 12.6}.
        """
        non_nan = labels.dropna()
        if non_nan.empty:
            if print_report:
                print("Class distribution: no resolved labels")
            return {}

        counts = non_nan.value_counts()
        pcts = (counts / counts.sum() * 100).to_dict()

        if print_report:
            total = int(counts.sum())
            nan_count = int(labels.isna().sum())
            print(f"Class distribution ({total} resolved labels, {nan_count} NaN/tail):")
            for label_value in (1.0, 0.0, -1.0):
                pct = pcts.get(label_value, 0.0)
                print(f"  {label_value:+.0f}: {pct:6.2f}%")

        return pcts


def compute_fixed_pct_labels(
    close: pd.Series,
    horizon_days: int,
    target_pct: float,
    pnd_block: pd.Series | None = None,
) -> pd.DataFrame:
    """
    GAINER EXPERIMENT (copy of labeling.py, not used by production):
    binary single-touch label — 1 if the forward close path over the next
    `horizon_days` trading days ever reaches +target_pct (e.g. 0.05 for
    5%) vs close[t], else 0. Unlike TripleBarrierLabeler this has no
    downside/stop barrier and no ATR scaling — a flat percentage target,
    matching the user's literal "5% gain over next 6 days" spec.

    Parameters
    ----------
    close : pd.Series
        Daily close prices, sorted ascending by date.
    horizon_days : int
        Forward window length in trading days.
    target_pct : float
        Fractional gain target, e.g. 0.05 for 5%.
    pnd_block : pd.Series, optional
        Boolean mask aligned to close's index; True downgrades a positive
        label to 0 (same convention as compute_triple_barrier_labels'
        pnd_block and multibagger_model.build_binary_labels' pnd exclusion
        — excludes P&D/upper-circuit-driven moves from positive labels).

    Returns
    -------
    pd.DataFrame
        Columns: label (0/1, NaN for unresolvable tail), max_return
        (float, forward max return actually realized over the window —
        used downstream for the near-miss-magnitude metric), first_touch_day
        (ML33, 2026-07-13: int day index (1..horizon_days) the forward path
        first reached +target_pct, or NaN if the label is 0/censored — i.e.
        it never touched target_pct within the horizon. Together with a
        censoring flag (event = label, since label==1 iff the touch
        actually happened within the window) this is the (duration, event)
        pair a survival model needs — first_touch_day is the "duration"
        for event==1 rows; event==0 rows are right-censored at
        horizon_days, same convention multibagger's build_binary_labels
        uses for its own (duration_months, event) pair.)
    """
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    if target_pct <= 0:
        raise ValueError("target_pct must be positive")

    values = close.to_numpy(dtype=np.float64)
    n = len(values)
    label = np.full(n, np.nan)
    max_return = np.full(n, np.nan)
    first_touch_day = np.full(n, np.nan)

    for i in range(n):
        fwd = values[i + 1:i + 1 + horizon_days]
        if len(fwd) < horizon_days:
            continue
        fwd_returns = fwd / values[i] - 1.0
        max_return[i] = np.nanmax(fwd_returns)
        label[i] = float(max_return[i] >= target_pct)
        if label[i] == 1.0:
            # First day (1-indexed) the forward path reached target_pct —
            # np.argmax on a boolean array returns the first True index.
            touched = fwd_returns >= target_pct
            first_touch_day[i] = float(np.argmax(touched) + 1)

    out = pd.DataFrame(
        {"label": label, "max_return": max_return, "first_touch_day": first_touch_day}, index=close.index
    )
    if pnd_block is not None:
        block_vals = pnd_block.reindex(close.index).fillna(False).to_numpy(dtype=bool)
        downgraded = (out["label"] == 1.0) & block_vals
        out.loc[downgraded, "label"] = 0.0
        # A downgraded label is no longer a real touch event for labeling
        # purposes (same P&D-exclusion convention as the label itself) —
        # its first_touch_day is dropped too so a survival model doesn't
        # treat a P&D-driven spike as a genuine event.
        out.loc[downgraded, "first_touch_day"] = np.nan
    return out


class FixedPercentLabeler:
    """
    GAINER EXPERIMENT: class wrapper around compute_fixed_pct_labels,
    mirroring TripleBarrierLabeler's panel API so it drops into the same
    training-dataset-building call sites.
    """

    def __init__(self, horizon_days: int, target_pct: float) -> None:
        self.horizon_days = horizon_days
        self.target_pct = target_pct

    def label_panel(
        self,
        df: pd.DataFrame,
        close_col: str = "close",
        ticker_col: str = "ticker",
        pnd_block_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Label a multi-ticker long-format panel, one independent pass per
        ticker. Returns a DataFrame (label, max_return) aligned to df's
        index — see compute_fixed_pct_labels.
        """
        required = [ticker_col, close_col] + ([pnd_block_col] if pnd_block_col else [])
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"df is missing required columns: {missing}")

        parts = []
        for _, group in df.groupby(ticker_col, sort=False):
            pnd = group[pnd_block_col] if pnd_block_col else None
            parts.append(compute_fixed_pct_labels(group[close_col], self.horizon_days, self.target_pct, pnd))
        return pd.concat(parts).reindex(df.index)
