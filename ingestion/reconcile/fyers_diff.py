"""
ingestion/reconcile/fyers_diff.py

Phase: 0.5 (FYERS Historical Backfill / Daily Cutover)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: scripts/fyers_staged_backfill.py,
    ingestion/scheduler/daily_pipeline.py (step_download_fyers_daily)

Diffs a staged FYERS OHLCV batch against the current production
`ohlcv_adjusted` table, classifying each (ticker, date) as:
  - "new"      — no existing row at all (a genuine gap being filled).
  - "changed"  — an existing row whose close or volume differs from
                 FYERS' value by more than the tolerance below (this is
                 the primary target: rows corrupted by the backward
                 adjustment bug documented in memory
                 project_ohlcv_adjfactor_discontinuities_20260802).
  - "unchanged" — matches within tolerance; no downstream recompute needed.

Only "new" and "changed" rows need feature/parquet recompute — this
module's whole purpose is to avoid a full recompute on every backfilled
year when most rows won't actually have moved.
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

# Relative tolerance for price comparison, absolute tolerance for volume
# (volume is an integer count, not a magnitude that scales with price).
PRICE_RELATIVE_TOLERANCE = 1e-4
VOLUME_ABSOLUTE_TOLERANCE = 1

DIFF_COLUMNS = ["ticker", "date", "change_type"]


def diff_fyers_vs_prod(
    conn,
    fyers_df: pd.DataFrame,
    tickers: List[str],
    start_date,
    end_date,
) -> pd.DataFrame:
    """
    Compare `fyers_df` (columns: ticker, date, open, high, low, close,
    volume — FYERSBackfill.download_history's output schema) against
    production `ohlcv_adjusted` for the same tickers/date range.

    Parameters
    ----------
    conn : Any
        Open DuckDB connection to the normalised-schema DB (must have
        read access to `ohlcv_adjusted`; a write connection is fine too,
        this function only reads).
    fyers_df : pd.DataFrame
        The staged FYERS batch (e.g. `staging.ohlcv_fyers`, already
        loaded into a DataFrame, or the raw batch_download output before
        staging).
    tickers : List[str]
        Tickers to diff. Must match fyers_df's ticker coverage (used to
        scope the production-side query — tickers not in this list are
        ignored even if present in fyers_df).
    start_date, end_date : date
        Inclusive production-side query range.

    Returns
    -------
    pd.DataFrame
        Columns: ticker, date, change_type ("new" | "changed" |
        "unchanged"), one row per (ticker, date) present in fyers_df.
        Empty DataFrame (same columns) if fyers_df is empty or tickers
        is empty.

    Spec References
    ----------------
    SPEC-PIPE-001.
    """
    if fyers_df.empty or not tickers:
        return pd.DataFrame(columns=DIFF_COLUMNS)

    placeholders = ",".join("?" for _ in tickers)
    prod_df = conn.execute(
        f"""
        SELECT ticker, date, close, volume
        FROM ohlcv_adjusted
        WHERE ticker IN ({placeholders}) AND date BETWEEN ? AND ?
        """,
        tickers + [start_date, end_date],
    ).df()

    # Normalize both sides to the same date dtype before merging — DuckDB's
    # .df() returns `date` as datetime64[us], while fyers_df (built from
    # FYERSBackfill.download_history, which uses plain python date objects)
    # has an object-dtype date column. Left as a bare merge, pandas raises
    # ValueError ("merge on object and datetime64 columns") rather than
    # silently mismatching, but normalizing here is the actual fix.
    fyers_side = fyers_df[["ticker", "date", "close", "volume"]].copy()
    fyers_side["date"] = pd.to_datetime(fyers_side["date"])
    prod_df["date"] = pd.to_datetime(prod_df["date"])

    merged = fyers_side.merge(
        prod_df,
        on=["ticker", "date"],
        how="left",
        suffixes=("_fyers", "_prod"),
    )

    is_new = merged["close_prod"].isna()

    price_diff = (merged["close_fyers"] - merged["close_prod"]).abs()
    price_changed = price_diff > (merged["close_prod"].abs() * PRICE_RELATIVE_TOLERANCE)

    volume_diff = (merged["volume_fyers"] - merged["volume_prod"]).abs()
    volume_changed = volume_diff > VOLUME_ABSOLUTE_TOLERANCE

    is_changed = ~is_new & (price_changed | volume_changed)

    change_type = pd.Series("unchanged", index=merged.index)
    change_type[is_new] = "new"
    change_type[is_changed] = "changed"

    result = merged[["ticker", "date"]].copy()
    result["change_type"] = change_type

    counts = change_type.value_counts().to_dict()
    logger.info(
        "fyers_diff: %d new, %d changed, %d unchanged (of %d rows, %s..%s)",
        counts.get("new", 0),
        counts.get("changed", 0),
        counts.get("unchanged", 0),
        len(result),
        start_date,
        end_date,
    )

    return result[DIFF_COLUMNS]


def recompute_targets(diff_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter a diff_fyers_vs_prod() result down to just the rows needing
    downstream feature/parquet recompute ("new" and "changed").

    Parameters
    ----------
    diff_df : pd.DataFrame
        Output of diff_fyers_vs_prod.

    Returns
    -------
    pd.DataFrame
        Columns: ticker, date. Empty if nothing needs recompute.
    """
    if diff_df.empty:
        return pd.DataFrame(columns=["ticker", "date"])
    targets = diff_df[diff_df["change_type"].isin(["new", "changed"])][["ticker", "date"]]
    return targets.reset_index(drop=True)
