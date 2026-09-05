"""
scripts/fyers_multiday_backfill.py

Multi-day OHLCV backfill for specific date ranges (catch-up scenarios).

When more than 1 day of data is missing (e.g., 2026-08-14 to 2026-09-04),
this script efficiently batches multiple days into SINGLE API CALLS per ticker,
avoiding the performance penalty of per-day downloads.

Key improvements over fyers_staged_backfill.py:
1. Date-range focused, not year-focused
2. Multi-day batching: fetches (start_date, end_date) in ONE call, not day-by-day
3. Parallel ticker downloads with bounded concurrency
4. Resumable at ticker granularity (cached tickers skipped on restart)
5. Automatic partitioning into 365-day chunks (Fyers API limit)

Usage
-----
    # Backfill missing 2026-08-14 to 2026-09-04 for all tickers
    python -m scripts.fyers_multiday_backfill \\
        --start-date 2026-08-14 \\
        --end-date 2026-09-04

    # Backfill entire 2025 for specific tickers
    python -m scripts.fyers_multiday_backfill \\
        --start-date 2025-01-01 \\
        --end-date 2025-12-31 \\
        --tickers SBIN TCS INFY

    # Dry run (fetch & stage, no publish)
    python -m scripts.fyers_multiday_backfill \\
        --start-date 2026-08-14 \\
        --end-date 2026-09-04 \\
        --dry-run
"""

from __future__ import annotations

import argparse
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from config.settings import DUCKDB_PATH, FYERS_RAW_DIR
from config.universe import (
    clip_to_listing_window,
    get_listing_windows,
    get_tickers_for_feature_engineering,
    get_top_adtv_tickers,
)
from datastore.api.db import get_duckdb_connection
from datastore.staging.gate import drop_staging_table, stage_dataframe
from datastore.staging.publish import publish_run_lock
from ingestion.reconcile.fyers_diff import diff_fyers_vs_prod, recompute_targets
from ingestion.scrapers.fyers_backfill import FYERSBackfill
from ingestion.scrapers.fyers_symbol_master import fetch_valid_nse_eq_tickers

logger = logging.getLogger(__name__)

# Checkpoint file for multi-day backfill tracking
MULTIDAY_CHECKPOINT_PATH = FYERS_RAW_DIR / "multiday_backfill_completed_ranges.txt"
TICKER_CACHE_DIR = FYERS_RAW_DIR / "multiday_backfill_cache"
RECOMPUTE_TARGETS_DIR = FYERS_RAW_DIR / "multiday_backfill_recompute_targets"

# Parallel fetch concurrency (conservative: 6 req/sec safe ceiling)
MAX_PARALLEL_FETCHES = 6

# Fyers API limitation: max 365 days per request
MAX_DAYS_PER_REQUEST = 365


def _partition_date_range(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """
    Split a date range into chunks of max MAX_DAYS_PER_REQUEST.

    Returns list of (chunk_start, chunk_end) tuples.
    """
    chunks = []
    current = start_date

    while current <= end_date:
        chunk_end = min(current + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    return chunks


def _make_cache_key(start_date: date, end_date: date) -> str:
    """Generate checkpoint key for a date range."""
    return f"{start_date.isoformat()}_{end_date.isoformat()}"


def _load_completed_ranges() -> "set[str]":
    """Load set of completed (start_date, end_date) ranges."""
    if not MULTIDAY_CHECKPOINT_PATH.exists():
        return set()
    return {line.strip() for line in MULTIDAY_CHECKPOINT_PATH.read_text().splitlines() if line.strip()}


def _mark_range_complete(start_date: date, end_date: date) -> None:
    """Mark a date range as completed."""
    MULTIDAY_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    key = _make_cache_key(start_date, end_date)
    with MULTIDAY_CHECKPOINT_PATH.open("a") as f:
        f.write(f"{key}\n")


def _ticker_cache_path(ticker: str, start_date: date, end_date: date) -> Path:
    """Path for ticker cache within a date range."""
    date_key = _make_cache_key(start_date, end_date)
    return TICKER_CACHE_DIR / date_key / f"{ticker}.parquet"


def clear_range_cache(start_date: date, end_date: date) -> None:
    """Delete cached ticker data for a completed date range."""
    date_key = _make_cache_key(start_date, end_date)
    cache_dir = TICKER_CACHE_DIR / date_key
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def _fetch_and_cache_ticker(
    fb: FYERSBackfill,
    ticker: str,
    start_date: date,
    end_date: date,
) -> str:
    """
    Fetch one ticker's history for [start_date, end_date] in a SINGLE API call
    (or multiple calls if range > 365 days), cache immediately.

    Returns
    -------
    str
        "cached", "fetched", "empty", or "failed"
    """
    cache_path = _ticker_cache_path(ticker, start_date, end_date)
    if cache_path.exists():
        return "cached"

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Partition into 365-day chunks if needed
        chunks = _partition_date_range(start_date, end_date)
        dfs = []

        for chunk_start, chunk_end in chunks:
            logger.debug(
                f"Fetching {ticker} for {chunk_start.isoformat()} to {chunk_end.isoformat()} "
                f"({(chunk_end - chunk_start).days + 1} days, single API call)"
            )
            df = fb.download_history(ticker, chunk_start.isoformat(), chunk_end.isoformat())
            if not df.empty:
                dfs.append(df)

        if not dfs:
            # No data for any chunk
            result_df = pd.DataFrame()
        else:
            result_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["date", "ticker"])

    except Exception as exc:
        logger.warning(
            f"fyers_multiday_backfill: {ticker} failed for {start_date.isoformat()}-{end_date.isoformat()} ({exc})"
        )
        return "failed"

    # Normalize date column to datetime64 for consistent Parquet schema
    if not result_df.empty:
        result_df = result_df.copy()
        result_df["date"] = pd.to_datetime(result_df["date"])
        result_df.to_parquet(cache_path, index=False)
        return "fetched"
    else:
        # Write empty frame so it's not retried
        result_df = pd.DataFrame()
        result_df.to_parquet(cache_path, index=False)
        return "empty"


def backfill_date_range(
    conn: Any,
    fb: FYERSBackfill,
    start_date: date,
    end_date: date,
    ticker_filter: Optional[list[str]] = None,
    dry_run: bool = False,
) -> "dict[str, Any]":
    """
    Backfill OHLCV for all (or filtered) tickers across [start_date, end_date].

    Parameters
    ----------
    ticker_filter : list[str], optional
        If provided, only fetch these tickers (else all from universe).
    """
    # Resolve universe
    stock_universe = set(get_tickers_for_feature_engineering())
    if ticker_filter:
        tickers = [t for t in ticker_filter if t in stock_universe]
    else:
        tickers = [t for t in get_top_adtv_tickers(n=len(stock_universe)) if t in stock_universe]

    windows = get_listing_windows(conn, tickers)

    per_ticker_range = {}
    for ticker in tickers:
        listing_date, delisting_date = windows.get(ticker, (None, None))
        clipped = clip_to_listing_window(listing_date, delisting_date, start_date, end_date)
        if clipped is not None:
            per_ticker_range[ticker] = clipped

    if not per_ticker_range:
        logger.info(
            f"backfill_date_range: no tickers in range {start_date.isoformat()}-{end_date.isoformat()}"
        )
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tickers": 0,
            "staged_rows": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
        }

    # Filter against FYERS symbol master
    valid_fyers_tickers = fetch_valid_nse_eq_tickers()
    if valid_fyers_tickers:
        not_on_fyers = [t for t in per_ticker_range if t not in valid_fyers_tickers]
        if not_on_fyers:
            logger.info(
                f"backfill_date_range: {len(not_on_fyers)}/{len(per_ticker_range)} tickers not in FYERS symbol master, skipping"
            )
        per_ticker_range = {t: r for t, r in per_ticker_range.items() if t in valid_fyers_tickers}

    if not per_ticker_range:
        logger.info(
            f"backfill_date_range: no valid FYERS tickers in range {start_date.isoformat()}-{end_date.isoformat()}"
        )
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tickers": 0,
            "staged_rows": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
        }

    logger.info(
        f"backfill_date_range: {len(per_ticker_range)} tickers for "
        f"{start_date.isoformat()}-{end_date.isoformat()} "
        f"(up to {MAX_PARALLEL_FETCHES} in parallel, multi-day batching)"
    )

    # Parallel fetch with multi-day batching per ticker
    statuses = {"cached": 0, "fetched": 0, "empty": 0, "failed": 0}
    failed_tickers = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
        future_to_ticker = {
            executor.submit(_fetch_and_cache_ticker, fb, ticker, start_date, end_date): ticker
            for ticker in per_ticker_range
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            status = future.result()
            statuses[status] = statuses.get(status, 0) + 1
            if status == "failed":
                failed_tickers.append(ticker)

    logger.info(f"backfill_date_range: fetch complete — {statuses}")
    if failed_tickers:
        logger.warning(
            f"backfill_date_range: {len(failed_tickers)}/{len(per_ticker_range)} tickers failed: "
            f"{failed_tickers[:20]}"
        )

    # Load cached data
    date_key = _make_cache_key(start_date, end_date)
    cache_glob = str(TICKER_CACHE_DIR / date_key / "*.parquet")
    ticker_list = list(per_ticker_range)
    placeholders = ",".join("?" for _ in ticker_list)

    try:
        range_df = conn.execute(
            f"SELECT * FROM read_parquet(?, union_by_name=true) WHERE ticker IN ({placeholders})",
            [cache_glob] + ticker_list,
        ).df()
    except Exception as exc:
        logger.warning(f"backfill_date_range: failed to load cached Parquet: {exc}")
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tickers": len(per_ticker_range),
            "staged_rows": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
        }

    if range_df.empty:
        logger.info("backfill_date_range: cached data yielded 0 rows")
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tickers": len(per_ticker_range),
            "staged_rows": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
        }

    # Validate and stage
    def _no_op_validator(df: pd.DataFrame) -> "tuple[pd.DataFrame, pd.DataFrame]":
        valid_mask = (
            df["open"].notna()
            & df["high"].notna()
            & df["low"].notna()
            & df["close"].notna()
            & (df["high"] >= df["low"])
            & (df["low"] >= 0)
        )
        passed = df[valid_mask].copy()
        rejected = df[~valid_mask].copy()
        if not rejected.empty:
            rejected["reason"] = "invalid OHLC"
        return passed, rejected

    stage_result = stage_dataframe(conn, "ohlcv_fyers", range_df, validators=[_no_op_validator])
    if not stage_result.ok:
        logger.warning("backfill_date_range: validation failed")
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tickers": len(per_ticker_range),
            "staged_rows": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
        }

    # Diff and prepare targets
    staged_df = conn.execute("SELECT * FROM staging.ohlcv_fyers").df()
    diff_df = diff_fyers_vs_prod(conn, staged_df, ticker_list, start_date, end_date)
    targets = recompute_targets(diff_df)

    counts = diff_df["change_type"].value_counts().to_dict()
    summary = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "tickers": len(per_ticker_range),
        "staged_rows": stage_result.staged_rows,
        "new": counts.get("new", 0),
        "changed": counts.get("changed", 0),
        "unchanged": counts.get("unchanged", 0),
    }

    if dry_run:
        logger.info(f"backfill_date_range: DRY RUN — {summary} (not published)")
        drop_staging_table(conn, "ohlcv_fyers")
        return summary

    # Publish
    if not targets.empty:
        with publish_run_lock() as acquired:
            if not acquired:
                raise RuntimeError("backfill_date_range: could not acquire publish_run_lock")

            conn.execute("ALTER TABLE staging.ohlcv_fyers ADD COLUMN IF NOT EXISTS adj_factor DOUBLE DEFAULT 1.0")
            conn.execute("ALTER TABLE staging.ohlcv_fyers ADD COLUMN IF NOT EXISTS vol_adj_factor DOUBLE DEFAULT 1.0")
            conn.execute("ALTER TABLE staging.ohlcv_fyers ALTER COLUMN date TYPE DATE")

            conn.execute(
                """
                DELETE FROM ohlcv_adjusted
                WHERE EXISTS (
                    SELECT 1 FROM staging.ohlcv_fyers s
                    WHERE s.ticker = ohlcv_adjusted.ticker AND s.date = ohlcv_adjusted.date
                )
                """
            )

            conn.execute(
                """
                INSERT INTO ohlcv_adjusted
                    (date, ticker, open, high, low, close, volume, adj_factor, vol_adj_factor, source)
                SELECT date, ticker, open, high, low, close, volume, 1.0, 1.0, 'fyers'
                FROM staging.ohlcv_fyers
                """
            )

        # Record recompute targets
        RECOMPUTE_TARGETS_DIR.mkdir(parents=True, exist_ok=True)
        target_file = RECOMPUTE_TARGETS_DIR / f"{start_date.isoformat()}_{end_date.isoformat()}.parquet"
        targets[["ticker", "date"]].to_parquet(target_file, index=False)

        logger.info(
            f"backfill_date_range: published {len(targets)} changed/new ticker-days "
            f"({targets['date'].min().isoformat()} to {targets['date'].max().isoformat()})"
        )
    else:
        logger.info("backfill_date_range: no changes detected")

    drop_staging_table(conn, "ohlcv_fyers")
    return summary


def run(
    start_date: date,
    end_date: date,
    ticker_filter: Optional[list[str]] = None,
    dry_run: bool = False,
) -> None:
    """Run multi-day backfill for [start_date, end_date]."""
    completed = _load_completed_ranges()
    range_key = _make_cache_key(start_date, end_date)

    if range_key in completed:
        logger.info(f"fyers_multiday_backfill: range {start_date.isoformat()}-{end_date.isoformat()} already completed")
        return

    fb = FYERSBackfill(non_interactive=True)

    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        summary = backfill_date_range(conn, fb, start_date, end_date, ticker_filter, dry_run=dry_run)
        logger.info(f"fyers_multiday_backfill: {start_date.isoformat()}-{end_date.isoformat()} done — {summary}")

        if not dry_run:
            _mark_range_complete(start_date, end_date)
            clear_range_cache(start_date, end_date)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multi-day OHLCV backfill for specific date ranges (catch-up scenarios)",
    )
    p.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
    p.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="Specific tickers to backfill (else all from universe)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Stage and diff but never publish",
    )
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    run(start_date, end_date, ticker_filter=args.tickers, dry_run=args.dry_run)
