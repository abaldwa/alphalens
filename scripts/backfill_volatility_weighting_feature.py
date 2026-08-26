"""
scripts/backfill_volatility_weighting_feature.py

Phase: R0 (traditional momentum + volatility-scaled position weighting)
Owner: Platform / Features

Adds features/volatility_weighting_features.py's 7 columns
(VOLATILITY_WEIGHTING_FEATURES) to the existing hybrid feature store
without re-running the full, expensive Stage-1 pipeline. The new
columns are computed purely from each ticker's own cached `close`
series (already present in every staging parquet), so this is a plain
merge — seconds per ticker instead of the ~0.5 min/ticker a full Stage-1
recompute costs across the whole universe.

Two targets, both updated:
  1. datastore/features/staging/<STAGING_DIR>/<TICKER>.parquet — Stage-1
     per-ticker cache (so a future --rebuild-daily rebuild already has
     these columns without recomputing them again).
  2. datastore/features/daily/<DATE>.parquet — the Stage-2 assembled,
     date-partitioned store that ML training actually reads.

Idempotent: safe to re-run — existing volatility-weighting columns (if
any) are dropped and recomputed before merging back in.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd

from features.volatility_weighting_features import (
    VOLATILITY_WEIGHTING_FEATURES,
    compute_volatility_weighting_features,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STAGING_DIR = Path("datastore/features/staging/reengineer_full_20260706")
DAILY_DIR = Path("datastore/features/daily")


def _backfill_staging() -> pd.DataFrame:
    """Merge the new columns into every per-ticker staging parquet.

    Returns the concatenated (date, ticker, +7 cols) long frame for the
    whole universe, reused by _backfill_daily so close prices are read
    from disk only once.
    """
    parts: List[pd.DataFrame] = []
    files = sorted(STAGING_DIR.glob("*.parquet"))
    logger.info(f"staging backfill: {len(files)} ticker parquets in {STAGING_DIR}")
    for i, path in enumerate(files):
        df = pd.read_parquet(path)
        df = df.sort_values("date").reset_index(drop=True)
        new_cols = compute_volatility_weighting_features(df[["date", "ticker", "close"]])
        # Idempotent: drop any stale columns from a prior run before merging.
        df = df.drop(columns=VOLATILITY_WEIGHTING_FEATURES, errors="ignore")
        merged = df.merge(new_cols, on=["date", "ticker"], how="left")
        merged.to_parquet(path, index=False)
        parts.append(new_cols)
        if (i + 1) % 250 == 0:
            logger.info(f"staging backfill: {i + 1}/{len(files)} tickers done")
    logger.info(f"staging backfill: {len(files)}/{len(files)} tickers done")
    return pd.concat(parts, ignore_index=True)


def _backfill_daily(all_new_cols: pd.DataFrame) -> None:
    """Merge the new columns into every date-partitioned daily parquet."""
    by_date: Dict[pd.Timestamp, pd.DataFrame] = dict(tuple(all_new_cols.groupby("date")))
    files = sorted(DAILY_DIR.glob("*.parquet"))
    logger.info(f"daily backfill: {len(files)} date parquets in {DAILY_DIR}")
    n_updated = 0
    for i, path in enumerate(files):
        date_ts = pd.Timestamp(path.stem)
        day_new_cols = by_date.get(date_ts)
        df = pd.read_parquet(path)
        df = df.drop(columns=VOLATILITY_WEIGHTING_FEATURES, errors="ignore")
        if day_new_cols is not None:
            merged = df.merge(day_new_cols.drop(columns=["date"]), on="ticker", how="left")
        else:
            merged = df
            for col in VOLATILITY_WEIGHTING_FEATURES:
                merged[col] = pd.NA
        merged.to_parquet(path, index=False)
        n_updated += 1
        if (i + 1) % 500 == 0:
            logger.info(f"daily backfill: {i + 1}/{len(files)} dates done")
    logger.info(f"daily backfill: {n_updated}/{len(files)} dates done")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-daily", action="store_true", help="Only update staging, not the daily store")
    args = parser.parse_args()

    all_new_cols = _backfill_staging()
    if not args.skip_daily:
        _backfill_daily(all_new_cols)
    logger.info("volatility-weighting feature backfill complete")


if __name__ == "__main__":
    main()
