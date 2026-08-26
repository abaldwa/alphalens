"""
features/pattern_scores.py

Phase: 3.1 (Advanced Technical Features — Pattern Recognition)
Specs: SPEC-FEAT-001, SPEC-PIPE-004
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine/models/signal

Computes 6 chart-pattern recognition scores using TA-Lib candlestick
functions and custom geometric scoring:
  head_shoulders_score, double_bottom_score, cup_handle_score,
  flag_pattern_score, wedge_score, base_breakout_score

All scores are in [0, 1] (0 = pattern absent, 1 = perfect pattern).
These are probabilistic formation scores, not binary detected/undetected flags.
TA-Lib's CDL functions return -100/0/+100; we use them as sub-signals inside
a composite scoring logic rather than as standalone outputs.

PIT Assumptions
---------------
All inputs are OHLCV prices (PITRule.NONE — always same-day knowable).
"""

import logging
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import talib
from scipy.signal import argrelextrema

logger = logging.getLogger(__name__)

PATTERN_FEATURES: List[str] = [
    "head_shoulders_score",
    "double_bottom_score",
    "cup_handle_score",
    "flag_pattern_score",
    "wedge_score",
    "base_breakout_score",
]


# ── Geometric helpers ─────────────────────────────────────────────────────────


def _peak_valley_idx(prices: np.ndarray[Any, Any], order: int = 3) -> Tuple[np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    """Return indices of local peaks and valleys within order-bars window."""
    peaks = argrelextrema(prices, np.greater_equal, order=order)[0]
    valleys = argrelextrema(prices, np.less_equal, order=order)[0]
    return peaks, valleys


def _head_shoulders_score(highs: np.ndarray[Any, Any], lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any]) -> float:
    """
    Head-and-shoulders: three peaks where middle peak (head) > the two shoulders,
    shoulders are roughly equal, and neckline is horizontal.

    Score ∈ [0, 1]: measures symmetry of shoulders + relative height of head.
    """
    n = len(highs)
    if n < 20:
        return 0.0
    try:
        peaks, _ = _peak_valley_idx(highs)
        if len(peaks) < 3:
            return 0.0
        # Consider the last three peaks
        p1, p2, p3 = peaks[-3], peaks[-2], peaks[-1]
        h1, h2, h3 = highs[p1], highs[p2], highs[p3]
        # Head must be the tallest
        if not (h2 > h1 and h2 > h3):
            return 0.0
        # Shoulder symmetry: |h1 - h3| / h2
        shoulder_symmetry = 1.0 - min(1.0, abs(h1 - h3) / (h2 + 1e-10))
        # Head prominence: how much taller is head vs shoulders
        head_prominence = min(1.0, (h2 - max(h1, h3)) / (h2 + 1e-10) * 5)
        score = shoulder_symmetry * 0.5 + head_prominence * 0.5

        # Penalty if close is still near the head (pattern hasn't completed)
        price_pct_from_head = (h2 - closes[-1]) / (h2 + 1e-10)
        completion_bonus = min(1.0, max(0.0, price_pct_from_head * 10))
        score *= (0.5 + 0.5 * completion_bonus)

        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


def _double_bottom_score(lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any]) -> float:
    """
    Double bottom: two roughly equal troughs separated by a peak, with price
    recovering from the second trough.

    Score ∈ [0, 1].
    """
    n = len(lows)
    if n < 15:
        return 0.0
    try:
        _, valleys = _peak_valley_idx(lows)
        if len(valleys) < 2:
            return 0.0
        v1, v2 = valleys[-2], valleys[-1]
        l1, l2 = lows[v1], lows[v2]
        # Troughs should be similar depth
        depth_symmetry = 1.0 - min(1.0, abs(l1 - l2) / (max(l1, l2) + 1e-10) * 5)
        # Price must be recovering from v2
        recovery = (closes[-1] - l2) / (l2 + 1e-10)
        recovery_score = min(1.0, max(0.0, recovery * 10))
        # Both troughs should be at multi-week lows
        low_of_window = lows.min()
        depth_from_window = 1.0 - (l2 - low_of_window) / (lows.max() - low_of_window + 1e-10)
        score = depth_symmetry * 0.4 + recovery_score * 0.4 + depth_from_window * 0.2
        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


def _cup_handle_score(highs: np.ndarray[Any, Any], lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any]) -> float:
    """
    Cup-and-handle: U-shaped consolidation followed by brief pullback handle near
    prior high.

    Scores the smoothness of the cup bottom and the tightness of the handle.
    """
    n = len(closes)
    if n < 30:
        return 0.0
    try:
        # Split into cup phase (first 70%) and handle phase (last 30%)
        cup_end = int(n * 0.7)
        cup = closes[:cup_end]
        handle = closes[cup_end:]

        # Cup: prior high, trough, recovery. Score = R² of parabola fit
        x = np.arange(len(cup))
        p = np.polyfit(x, cup, 2)
        if p[0] <= 0:  # concave up (bowl shape) required
            return 0.0
        fit = np.polyval(p, x)
        ss_res: float = np.sum((cup - fit) ** 2)
        ss_tot = np.sum((cup - cup.mean()) ** 2) + 1e-10
        r2_cup = max(0.0, 1 - ss_res / ss_tot)

        # Handle: shallow pullback < 1/3 of cup depth, tight consolidation
        cup_depth = highs[:cup_end].max() - lows[:cup_end].min() + 1e-10
        handle_decline = max(0.0, handle.max() - handle.min())
        handle_tightness = max(0.0, 1.0 - handle_decline / cup_depth * 3)

        # Recovery: current close near prior high
        prior_high = highs[:cup_end].max()
        recovery = min(1.0, max(0.0, 1.0 - (prior_high - closes[-1]) / (prior_high + 1e-10) * 10))

        score = r2_cup * 0.4 + handle_tightness * 0.3 + recovery * 0.3
        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


def _flag_pattern_score(highs: np.ndarray[Any, Any], lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any],
                        volumes: np.ndarray[Any, Any]) -> float:
    """
    Bull flag: sharp upward pole + consolidation channel with declining volume.

    Score ∈ [0, 1].
    """
    n = len(closes)
    if n < 20:
        return 0.0
    try:
        # Pole: first 1/3 of window has strong positive return
        pole_end = max(5, n // 3)
        pole_return = (closes[pole_end] - closes[0]) / (closes[0] + 1e-10)
        if pole_return < 0.05:  # need at least 5% move for a pole
            return 0.0
        pole_strength = min(1.0, pole_return / 0.15)

        # Flag: last 2/3 consolidates in a channel
        flag = closes[pole_end:]
        if len(flag) < 5:
            return 0.0
        flag_range = (flag.max() - flag.min()) / (flag.mean() + 1e-10)
        channel_tightness = max(0.0, 1.0 - flag_range * 5)

        # Volume should be declining in flag
        flag_volumes = volumes[pole_end:]
        if len(flag_volumes) > 2:
            vol_trend = float(np.polyfit(np.arange(len(flag_volumes)), flag_volumes, 1)[0])
            vol_decline = max(0.0, min(1.0, -vol_trend / (volumes.mean() + 1e-10) * 10))
        else:
            vol_decline = 0.5  # neutral

        score = pole_strength * 0.4 + channel_tightness * 0.4 + vol_decline * 0.2
        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


def _wedge_score(highs: np.ndarray[Any, Any], lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any]) -> float:
    """
    Rising or falling wedge: converging trendlines (high slope ≠ low slope,
    lines converging toward each other).

    Score ∈ [0, 1] where 1 = tight converging wedge near breakout.
    """
    n = len(closes)
    if n < 15:
        return 0.0
    try:
        x = np.arange(n, dtype=float)
        slope_h = float(np.polyfit(x, highs, 1)[0])
        slope_l = float(np.polyfit(x, lows, 1)[0])
        # Wedge: slopes must have opposite sign or converge (|slope_h - slope_l| > 0)
        convergence = abs(slope_h - slope_l)
        # Normalise by price scale
        price_scale = closes.mean() + 1e-10
        convergence_norm = min(1.0, convergence / price_scale * n * 0.5)

        # Width at end vs beginning: should be narrower at end
        initial_width = highs[0] - lows[0]
        final_width = highs[-1] - lows[-1]
        if initial_width <= 0:
            return 0.0
        width_compression = max(0.0, 1.0 - final_width / initial_width)

        score = convergence_norm * 0.5 + width_compression * 0.5
        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


def _base_breakout_score(highs: np.ndarray[Any, Any], lows: np.ndarray[Any, Any], closes: np.ndarray[Any, Any],
                         volumes: np.ndarray[Any, Any]) -> float:
    """
    Base breakout: extended consolidation base followed by volume-expansion breakout.

    Score ∈ [0, 1].
    """
    n = len(closes)
    if n < 20:
        return 0.0
    try:
        base_end = int(n * 0.8)
        base = closes[:base_end]
        breakout = closes[base_end:]
        if len(breakout) < 2:
            return 0.0

        base_range = (base.max() - base.min()) / (base.mean() + 1e-10)
        tightness = max(0.0, 1.0 - base_range * 5)

        # Length of base — longer bases are more significant
        base_duration = min(1.0, base_end / 40)

        # Breakout above base high
        base_high = highs[:base_end].max()
        current_close = closes[-1]
        above_base = min(1.0, max(0.0, (current_close - base_high) / (base_high + 1e-10) * 20))

        # Volume expansion on breakout
        vol_base_avg = volumes[:base_end].mean() + 1e-10
        vol_breakout_avg = volumes[base_end:].mean()
        vol_expansion = min(1.0, max(0.0, (vol_breakout_avg / vol_base_avg - 1.0) * 0.5))

        score = tightness * 0.25 + base_duration * 0.15 + above_base * 0.35 + vol_expansion * 0.25
        return float(np.clip(score, 0.0, 1.0))
    except Exception:
        return 0.0


# ── Per-ticker computation ────────────────────────────────────────────────────


def _compute_row_pattern_scores(
    op: np.ndarray[Any, Any], hi: np.ndarray[Any, Any], lo: np.ndarray[Any, Any], cl: np.ndarray[Any, Any], vol: np.ndarray[Any, Any], end: int,
) -> Dict[str, float]:
    """
    Compute the 6 pattern scores "as of" bar index `end - 1`, using only
    `[: end]` of each array — PITRule.NONE-safe for any `end`, not just
    the final bar. Extracted from the old `_compute_patterns_for_ticker`
    (which only ever called this logic once, for the final row).
    """
    n = end
    w = min(40, n)
    hi_w, lo_w, cl_w, vol_w = hi[end - w:end], lo[end - w:end], cl[end - w:end], vol[end - w:end]

    hs = _head_shoulders_score(hi_w, lo_w, cl_w)
    db = _double_bottom_score(lo_w, cl_w)
    ch = _cup_handle_score(hi_w, lo_w, cl_w)
    fp = _flag_pattern_score(hi_w, lo_w, cl_w, vol_w)
    wg = _wedge_score(hi_w, lo_w, cl_w)
    bb = _base_breakout_score(hi_w, lo_w, cl_w, vol_w)

    op_e, hi_e, lo_e, cl_e = op[:end], hi[:end], lo[:end], cl[:end]
    # Supplement HS score with TA-Lib evening star / bearish engulfing if signal present
    try:
        cdl_es = talib.CDLEVENINGSTAR(op_e, hi_e, lo_e, cl_e, penetration=0)
        if cdl_es[-1] != 0:
            hs = min(1.0, hs + 0.15)
    except Exception:
        pass

    # Supplement double-bottom with TA-Lib morning star
    try:
        cdl_ms = talib.CDLMORNINGSTAR(op_e, hi_e, lo_e, cl_e, penetration=0)
        if cdl_ms[-1] != 0:
            db = min(1.0, db + 0.15)
    except Exception:
        pass

    return {
        "head_shoulders_score": hs, "double_bottom_score": db, "cup_handle_score": ch,
        "flag_pattern_score": fp, "wedge_score": wg, "base_breakout_score": bb,
    }


def _compute_patterns_for_ticker(grp: pd.DataFrame, all_rows: bool = False) -> pd.DataFrame:
    """
    Compute 6 pattern scores for a single ticker panel.

    all_rows : bool
        [2026-08-01] False (default, live daily pipeline's only use):
        fills only the last row — original behavior. True (only
        `features/panel_staging.py`'s batch-backfill path): fills every
        row with >= 20 bars of trailing history — see
        `features/advanced_technical.py::_compute_for_ticker`'s `all_rows`
        docstring for the identical bug this mirrors/fixes:
        `compute_full_range_chunk_panels` calls this once per ticker over
        a multi-year panel, and the old always-last-row-only behavior left
        every date but the single most recent one NaN.
    """
    grp = grp.sort_values("date").reset_index(drop=True)
    n = len(grp)

    result = grp[["date", "ticker"]].copy()
    for col in PATTERN_FEATURES:
        result[col] = np.nan

    if n < 20:
        return result

    op = grp["open"].to_numpy(dtype=float)
    hi = grp["high"].to_numpy(dtype=float)
    lo = grp["low"].to_numpy(dtype=float)
    cl = grp["close"].to_numpy(dtype=float)
    vol = grp["volume"].to_numpy(dtype=float)

    row_positions = range(19, n) if all_rows else [n - 1]
    for i in row_positions:
        scores = _compute_row_pattern_scores(op, hi, lo, cl, vol, end=i + 1)
        for col, val in scores.items():
            result.loc[result.index[i], col] = val

    return result


# ── Public API ────────────────────────────────────────────────────────────────


def compute_pattern_scores(ohlcv_panel: pd.DataFrame, all_rows: bool = False) -> pd.DataFrame:
    """
    Compute 6 chart-pattern probability scores for the full OHLCV panel.

    Parameters
    ----------
    ohlcv_panel : pd.DataFrame
        Columns: date, ticker, open, high, low, close, volume.
    all_rows : bool
        [2026-08-01] Default False preserves the original, live-daily-
        pipeline behavior: only the last row per ticker gets computed. Set
        True ONLY for batch backfill staging (`features/panel_staging.py`)
        — see `_compute_patterns_for_ticker`'s `all_rows` docstring for the
        bug this fixes. Meaningfully more expensive with all_rows=True
        (~n_rows/ticker calls instead of 1) — only pass True when you
        actually need per-date historical values.

    Returns
    -------
    pd.DataFrame
        One row per (date, ticker); columns: date, ticker + PATTERN_FEATURES.
        All scores in [0, 1]. With all_rows=False (default), only the most
        recent bar per ticker has non-NaN values; with all_rows=True,
        every bar with >= 20 bars of trailing history does.

    Spec References
    ---------------
    SPEC-FEAT-001: NaN returned for tickers with insufficient history.
    SPEC-PIPE-004: vectorized via groupby (no per-stock Python for-loop).
    """
    required = {"date", "ticker", "open", "high", "low", "close", "volume"}
    missing = required - set(ohlcv_panel.columns)
    if missing:
        raise ValueError(f"ohlcv_panel missing columns: {missing}")

    result = ohlcv_panel.groupby("ticker", group_keys=False).apply(
        lambda grp: _compute_patterns_for_ticker(grp, all_rows=all_rows)
    )
    return result[["date", "ticker"] + PATTERN_FEATURES].reset_index(drop=True)
