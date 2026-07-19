"""
ingestion/adjust/rights_adjuster.py

Phase: full-codebase-review Fix 2 (2026-07-19)
Owner: Platform / Ingestion
Consumers: scripts/apply_rights_adjustment_fyers.py (on-demand job)

Why this exists
----------------
ingestion/adjust/price_adjuster.py's `_action_factors()` deliberately
returns a no-op (1.0, 1.0) for RIGHTS actions — a rights issue's price
impact depends on the subscription price and take-up rate, not just the
entitlement ratio, so it cannot be derived locally from `ratio` alone the
way SPLIT/BONUS can (see that function's docstring). The only prior
handling was a one-off manual patch
(scripts/validate_corporate_actions_fyers.py's ratio_pre/ratio_post
comparison, applied by hand to specific tickers) — any newly-ingested
RIGHTS action got zero adjustment until someone noticed and re-ran that
process.

Per 2026-07-19 user decision, this is an **on-demand job**, not a live
call inside the normal ingestion pipeline — `_action_factors()` is left
untouched; an operator runs scripts/apply_rights_adjustment_fyers.py
explicitly when they want RIGHTS actions corrected. Per the same
decision, Fyers is the source of truth for the adjustment factor (not a
locally recomputed formula) — this module formalizes
validate_corporate_actions_fyers.py's empirical ratio_pre/ratio_post
comparison into a reusable, directly-testable function instead of a
copy-pasted inline computation.

Methodology (identical to validate_corporate_actions_fyers.py):
Fyers' `history` endpoint returns split/bonus/rights-ADJUSTED continuous
prices, so a raw pre/post price jump around ex_date in Fyers data is NOT
expected — the whole point of adjustment is that Fyers' series has no
jump there. Our own adj_close divided by Fyers' close should be a roughly
constant ratio across the window if our adjustment already matches; if
our RIGHTS action has NO adjustment applied (the current, documented
gap), that ratio itself jumps across ex_date, and ratio_post/ratio_pre is
the empirical correction factor Fyers implies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 10


@dataclass
class RightsAdjustmentFactor:
    """
    Result of comparing our adjusted prices against Fyers' around a
    RIGHTS action's ex_date.

    Attributes
    ----------
    ticker : str
    ex_date : str
    ratio_pre : float
        Median(our_close / fyers_close) for rows before ex_date.
    ratio_post : float
        Median(our_close / fyers_close) for rows on/after ex_date.
    price_factor : float
        ratio_post / ratio_pre — the empirical multiplicative correction
        that should be applied to every row before ex_date so our series
        matches Fyers' continuous (already-adjusted) series. 1.0 means our
        existing adjustment already matches Fyers (no correction needed).
    n_pre : int
        Number of matched (date-aligned) rows before ex_date used.
    n_post : int
        Number of matched rows on/after ex_date used.
    """

    ticker: str
    ex_date: str
    ratio_pre: float
    ratio_post: float
    price_factor: float
    n_pre: int
    n_post: int


def compute_rights_adjustment_factor(
    conn,
    fyers_client,
    ticker: str,
    ex_date: str,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> Optional[RightsAdjustmentFactor]:
    """
    Compute the Fyers-derived price adjustment factor for a single RIGHTS
    corporate action, live — this IS the "pull from Fyers API rather than
    recompute" feature (2026-07-19 user decision): the factor comes from
    comparing our stored prices against Fyers' own (correctly adjusted)
    series, not from a locally-derived formula based on the rights ratio
    (which price_adjuster.py's _action_factors() docstring explains can't
    work — subscription price and take-up rate aren't in `ratio` alone).

    Parameters
    ----------
    conn : DuckDB connection
        Read access to ohlcv_adjusted for `ticker`.
    fyers_client : ingestion.scrapers.fyers_backfill.FYERSBackfill
        Caller-supplied client (dependency injection — tests pass a stub
        exposing the same download_history(ticker, from_date, to_date)
        signature, avoiding a real Fyers auth dependency in unit tests).
    ticker : str
        Bare NSE symbol (e.g. 'RELIANCE').
    ex_date : str
        ISO date (YYYY-MM-DD) of the RIGHTS action's ex-date.
    window_days : int
        Trading-day window on each side of ex_date to compare (default 10,
        matching validate_corporate_actions_fyers.py's default).

    Returns
    -------
    RightsAdjustmentFactor, or None if there isn't enough overlapping
    data on both sides of ex_date to compute a reliable factor (never
    fabricates a factor from partial/one-sided data).
    """
    ex_ts = pd.Timestamp(ex_date)
    win_from = (ex_ts - timedelta(days=window_days)).strftime("%Y-%m-%d")
    win_to = (ex_ts + timedelta(days=window_days)).strftime("%Y-%m-%d")

    hist = fyers_client.download_history(ticker, win_from, win_to)
    if hist is None or hist.empty or len(hist) < 2:
        logger.warning(f"{ticker}: no usable Fyers history around {ex_date}")
        return None
    hist = hist.sort_values("date").copy()
    hist["date"] = pd.to_datetime(hist["date"])

    ours = conn.execute(
        """SELECT date, close FROM ohlcv_adjusted
           WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date""",
        [ticker, win_from, win_to],
    ).fetchdf()
    if ours.empty:
        logger.warning(f"{ticker}: no ohlcv_adjusted rows around {ex_date}")
        return None
    ours["date"] = pd.to_datetime(ours["date"])

    merged = pd.merge(ours, hist[["date", "close"]], on="date", suffixes=("_ours", "_fyers"))
    merged = merged[merged["close_fyers"] > 0]
    if merged.empty:
        return None

    merged["ratio"] = merged["close_ours"] / merged["close_fyers"]
    before = merged[merged["date"] < ex_ts]
    after = merged[merged["date"] >= ex_ts]

    if before.empty or after.empty:
        logger.warning(
            f"{ticker}: insufficient window around {ex_date} "
            f"(n_pre={len(before)}, n_post={len(after)}) — cannot compute factor"
        )
        return None

    ratio_pre = float(before["ratio"].median())
    ratio_post = float(after["ratio"].median())
    if ratio_pre <= 0:
        return None

    return RightsAdjustmentFactor(
        ticker=ticker,
        ex_date=ex_date,
        ratio_pre=ratio_pre,
        ratio_post=ratio_post,
        price_factor=ratio_post / ratio_pre,
        n_pre=len(before),
        n_post=len(after),
    )
