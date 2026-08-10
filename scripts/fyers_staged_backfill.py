"""
scripts/fyers_staged_backfill.py

Phase: 0.5 (FYERS Historical Backfill / Daily Cutover)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: manual invocation; eventually
    ingestion/scheduler/pipeline_scheduler.py for a one-time scheduled run.

Multi-year FYERS OHLCV backfill, one year at a time, most-recent-complete
year first, walking back to --start-year (default 2017):

  1. Resolve the stock-only, ADTV-ranked ticker universe
     (config.universe.get_tickers_for_feature_engineering +
     get_top_adtv_tickers), clipped per-ticker to
     [listing_date, delisting_date] ∩ [year_start, year_end]
     (config.universe.get_listing_windows / clip_to_listing_window) — so
     no ticker is ever requested before it listed or after it delisted.
     Further filtered against FYERS' own symbol master
     (ingestion.scrapers.fyers_symbol_master) to skip tickers FYERS
     doesn't recognize under the NSE:<ticker>-EQ format at all — avoids
     ~10-12% of the universe otherwise failing with "Invalid symbol
     provided" on every single run (live-confirmed 2026-08-04).
  2. Download the year's OHLCV in parallel (ThreadPoolExecutor, bounded
     concurrency — see MAX_PARALLEL_FETCHES) via the existing,
     already-tested ingestion.scrapers.fyers_backfill.FYERSBackfill.
     Each ticker's result is cached to its own Parquet file immediately
     on fetch (TICKER_CACHE_DIR) rather than accumulated in a Python
     list for the whole year — this is what makes the step resumable
     (a crash mid-year only loses whatever hadn't been cached yet, not
     the whole year — see _fetch_and_cache_ticker) and keeps peak memory
     bounded (each ticker's DataFrame is written to disk and released,
     not held for the ~2300-ticker duration of the fetch loop).
  3. Load the year's cached Parquet files into staging.ohlcv_fyers
     (datastore.staging.gate.stage_dataframe) via DuckDB's native
     read_parquet — a single bounded materialization at the end, not a
     running Python-side accumulation.
  4. Diff staged vs. production (ingestion.reconcile.fyers_diff) to find
     exactly which (ticker, date) rows are new or changed.
  5. Publish the staged year into ohlcv_adjusted with adj_factor=1.0
     (under publish_run_lock) — Fyers data is already corporate-action-
     adjusted, so no price_adjuster.py pass runs on these rows.
  6. [2026-08-04, user decision] Feature/parquet recompute is deferred to
     the END, once every year has been published — NOT triggered per-year
     during the loop. Each year's exact (ticker, date) targets are
     persisted to RECOMPUTE_TARGETS_DIR/<year>.parquet
     (_record_recompute_targets) for a later, separate, explicit
     consolidated recompute run to consume.
  7. Drop staging.ohlcv_fyers and this year's ticker cache, move to the
     previous year.

Resumable at TWO granularities: YEAR_CHECKPOINT_PATH (a whole completed
year is skipped entirely on restart) AND, within an in-progress year,
TICKER_CACHE_DIR (a ticker already cached is never re-fetched — a crash
mid-year costs only the not-yet-cached tickers, not the whole year).

Non-stock instruments (ETFs etc.) are entirely untouched by this script —
they keep coming from NSE Bhavcopy as before.

Usage
-----
    python -m scripts.fyers_staged_backfill --end-year 2026 --start-year 2017
    python -m scripts.fyers_staged_backfill --end-year 2026 --start-year 2017 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

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

YEAR_CHECKPOINT_PATH = FYERS_RAW_DIR / "staged_backfill_completed_years.txt"
TICKER_CACHE_DIR = FYERS_RAW_DIR / "staged_backfill_cache"
# One Parquet file per year: exact (ticker, date) pairs needing feature/
# parquet recompute, for a later consolidated pass — see
# _record_recompute_targets. Deliberately NOT cleared by clear_year_cache
# (that only clears the raw OHLCV ticker cache) — these must survive
# until the eventual recompute actually consumes them.
RECOMPUTE_TARGETS_DIR = FYERS_RAW_DIR / "staged_backfill_recompute_targets"

# [2026-08-04, live-confirmed] 8 concurrent FYERS history() calls completed
# cleanly (~0.8s total for 8 tickers); 16 concurrent hit 3x "429 request
# limit reached" (FYERS' documented ~10 req/sec ceiling, community docs).
# 6 gives a real safety margin under sustained load (each call ~0.7-0.8s,
# so steady-state throughput ≈ 6/0.8s ≈ 7.5 req/sec) rather than sitting
# right at the edge like the 8-worker test did.
MAX_PARALLEL_FETCHES = 6


def _no_op_validator(df: pd.DataFrame) -> tuple:
    """Placeholder validator slot for datastore.staging.gate.stage_dataframe.

    Basic sanity (no null OHLC, high>=low>=0) — FYERS itself is the
    trusted source here (see module docstring: no corporate-action
    reprocessing), so this is a shape/sanity check, not a business-rule
    gate.
    """
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
        rejected["reason"] = "invalid OHLC (null or high<low or negative)"
    return passed, rejected


def _load_completed_years() -> set:
    if not YEAR_CHECKPOINT_PATH.exists():
        return set()
    return {int(line) for line in YEAR_CHECKPOINT_PATH.read_text().splitlines() if line.strip()}


def _mark_year_complete(year: int) -> None:
    YEAR_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with YEAR_CHECKPOINT_PATH.open("a") as f:
        f.write(f"{year}\n")


def _year_cache_dir(year: int) -> Path:
    return TICKER_CACHE_DIR / str(year)


def _record_recompute_targets(year: int, targets: pd.DataFrame) -> None:
    """
    Persist the exact (ticker, date) pairs that need feature/parquet
    recompute for this year — one Parquet file per year under
    RECOMPUTE_TARGETS_DIR, so a later consolidated recompute run can read
    RECOMPUTE_TARGETS_DIR/*.parquet and act on precisely these cells,
    never a broader "recompute everything in this date range" sweep.
    """
    RECOMPUTE_TARGETS_DIR.mkdir(parents=True, exist_ok=True)
    targets[["ticker", "date"]].to_parquet(RECOMPUTE_TARGETS_DIR / f"{year}.parquet", index=False)


def _ticker_cache_path(year: int, ticker: str) -> Path:
    return _year_cache_dir(year) / f"{ticker}.parquet"


def clear_year_cache(year: int) -> None:
    """Delete the per-ticker Parquet cache for a fully-completed year."""
    year_dir = _year_cache_dir(year)
    if year_dir.exists():
        shutil.rmtree(year_dir)


def _fetch_and_cache_ticker(fb: FYERSBackfill, ticker: str, start: date, end: date, year: int) -> str:
    """
    Fetch one ticker's history and cache it to disk immediately, unless
    already cached (resume). Never raises — failures are caught and
    reported via the returned status so one bad ticker can't take down
    the whole ThreadPoolExecutor batch.

    Returns
    -------
    str
        "cached" (already had it from a prior run), "fetched" (new data
        written), "empty" (FYERS returned nothing, e.g. ticker delisted
        before this year — cached as an empty Parquet so it's not
        retried every resume), or "failed" (download_history raised).
    """
    cache_path = _ticker_cache_path(year, ticker)
    if cache_path.exists():
        return "cached"

    try:
        df = fb.download_history(ticker, start.isoformat(), end.isoformat())
    except Exception as exc:
        logger.warning(f"fyers_staged_backfill: {ticker} failed for {year} ({exc}) — skipping")
        return "failed"

    # [2026-08-04, live-confirmed] download_history()'s "date" column holds
    # plain python date objects (pandas object dtype). Writing that
    # straight to Parquet per-ticker, across ~2300 separate files, lets
    # pyarrow's schema inference drift file-to-file — DuckDB's read_parquet
    # glob scan over the resulting files then hit a real internal crash
    # ("ExpressionExecutor::Execute called with a result vector of type
    # VARCHAR that does not match expression type TIMESTAMP") the first
    # time a query touched the unified column. Normalizing to a proper
    # datetime64 dtype before writing makes every file's Parquet schema
    # for this column identical, which is what read_parquet's glob mode
    # actually requires.
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    return "fetched" if not df.empty else "empty"


def backfill_year(conn, fb: FYERSBackfill, year: int, dry_run: bool = False) -> dict:
    """
    Run the full fetch -> stage -> diff -> publish -> recompute -> drop
    cycle for one calendar year.

    Returns
    -------
    dict
        Summary: {"year", "tickers", "staged_rows", "new", "changed", "unchanged"}.
    """
    year_start = date(year, 1, 1)
    year_end = min(date(year, 12, 31), date.today())

    stock_universe = set(get_tickers_for_feature_engineering())
    tickers = [t for t in get_top_adtv_tickers(n=len(stock_universe)) if t in stock_universe]
    windows = get_listing_windows(conn, tickers)

    per_ticker_range = {}
    for ticker in tickers:
        listing_date, delisting_date = windows.get(ticker, (None, None))
        clipped = clip_to_listing_window(listing_date, delisting_date, year_start, year_end)
        if clipped is not None:
            per_ticker_range[ticker] = clipped

    if not per_ticker_range:
        logger.info("fyers_staged_backfill: no tickers traded in %d, skipping", year)
        return {"year": year, "tickers": 0, "staged_rows": 0, "new": 0, "changed": 0, "unchanged": 0}

    valid_fyers_tickers = fetch_valid_nse_eq_tickers()
    if valid_fyers_tickers:
        not_on_fyers = [t for t in per_ticker_range if t not in valid_fyers_tickers]
        if not_on_fyers:
            logger.info(
                "fyers_staged_backfill: year %d — %d/%d tickers not in FYERS' symbol master, "
                "skipping without an API call: %s",
                year, len(not_on_fyers), len(per_ticker_range),
                not_on_fyers[:20] if len(not_on_fyers) > 20 else not_on_fyers,
            )
        per_ticker_range = {t: r for t, r in per_ticker_range.items() if t in valid_fyers_tickers}
    else:
        logger.warning(
            "fyers_staged_backfill: could not load FYERS symbol master — proceeding without "
            "pre-filtering (invalid symbols will still be caught and skipped per-ticker)"
        )

    if not per_ticker_range:
        logger.info("fyers_staged_backfill: no valid FYERS tickers left to fetch for %d", year)
        return {"year": year, "tickers": 0, "staged_rows": 0, "new": 0, "changed": 0, "unchanged": 0}

    logger.info(
        "fyers_staged_backfill: year %d — %d tickers to download (up to %d in parallel)",
        year, len(per_ticker_range), MAX_PARALLEL_FETCHES,
    )

    statuses = {"cached": 0, "fetched": 0, "empty": 0, "failed": 0}
    failed_tickers = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
        future_to_ticker = {
            executor.submit(_fetch_and_cache_ticker, fb, ticker, start, end, year): ticker
            for ticker, (start, end) in per_ticker_range.items()
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            status = future.result()
            statuses[status] = statuses.get(status, 0) + 1
            if status == "failed":
                failed_tickers.append(ticker)

    logger.info("fyers_staged_backfill: year %d fetch complete — %s", year, statuses)
    if failed_tickers:
        logger.warning(
            "fyers_staged_backfill: year %d — %d/%d tickers failed: %s",
            year, len(failed_tickers), len(per_ticker_range),
            failed_tickers[:20] if len(failed_tickers) > 20 else failed_tickers,
        )

    cache_files = list(_year_cache_dir(year).glob("*.parquet"))
    if not cache_files:
        logger.info("fyers_staged_backfill: year %d — FYERS returned no data at all", year)
        return {"year": year, "tickers": len(per_ticker_range), "staged_rows": 0, "new": 0, "changed": 0, "unchanged": 0}

    # Single bounded materialization via DuckDB's native Parquet scan —
    # not a Python-side accumulation across the whole fetch loop (see
    # module docstring). Filtered to this run's ticker set in case a
    # prior run's cache dir has extra tickers this run's universe/symbol
    # filter excluded.
    cache_glob = str(_year_cache_dir(year) / "*.parquet")
    ticker_list = list(per_ticker_range)
    placeholders = ",".join("?" for _ in ticker_list)
    year_df = conn.execute(
        f"SELECT * FROM read_parquet(?) WHERE ticker IN ({placeholders})",
        [cache_glob] + ticker_list,
    ).df()

    if year_df.empty:
        logger.info("fyers_staged_backfill: year %d — cached data yielded 0 rows", year)
        return {"year": year, "tickers": len(per_ticker_range), "staged_rows": 0, "new": 0, "changed": 0, "unchanged": 0}

    stage_result = stage_dataframe(conn, "ohlcv_fyers", year_df, validators=[_no_op_validator])
    if not stage_result.ok:
        logger.warning("fyers_staged_backfill: year %d — nothing passed staging validation", year)
        return {"year": year, "tickers": len(per_ticker_range), "staged_rows": 0, "new": 0, "changed": 0, "unchanged": 0}

    staged_df = conn.execute("SELECT * FROM staging.ohlcv_fyers").df()
    diff_df = diff_fyers_vs_prod(conn, staged_df, list(per_ticker_range), year_start, year_end)
    targets = recompute_targets(diff_df)

    counts = diff_df["change_type"].value_counts().to_dict()
    summary = {
        "year": year,
        "tickers": len(per_ticker_range),
        "staged_rows": stage_result.staged_rows,
        "new": counts.get("new", 0),
        "changed": counts.get("changed", 0),
        "unchanged": counts.get("unchanged", 0),
    }

    if dry_run:
        logger.info("fyers_staged_backfill: DRY RUN year %d — %s (not published)", year, summary)
        drop_staging_table(conn, "ohlcv_fyers")
        return summary

    if not targets.empty:
        with publish_run_lock() as acquired:
            if not acquired:
                raise RuntimeError(
                    "fyers_staged_backfill: could not acquire publish_run_lock — "
                    "another staging/publish operation is in progress."
                )
            conn.execute(
                "ALTER TABLE staging.ohlcv_fyers ADD COLUMN IF NOT EXISTS adj_factor DOUBLE DEFAULT 1.0"
            )
            conn.execute(
                "ALTER TABLE staging.ohlcv_fyers ADD COLUMN IF NOT EXISTS vol_adj_factor DOUBLE DEFAULT 1.0"
            )
            # staging's "date" column loads as TIMESTAMP-family (pandas has
            # no true date-only dtype) — cast to DATE so the INSERT below
            # writes proper DATE values into ohlcv_adjusted.date, matching
            # its native column type.
            conn.execute("ALTER TABLE staging.ohlcv_fyers ALTER COLUMN date TYPE DATE")
            # [2026-08-04, live-confirmed] The `(ticker, date) IN (SELECT
            # ticker, date FROM ...)` row/tuple-comparison pattern hits a
            # genuine DuckDB 1.2.0 internal engine crash ("ExpressionExecutor
            # ::Execute called with a result vector of type VARCHAR that does
            # not match expression type ...") — reproduced identically across
            # THREE different declared types for staging's date column
            # (TIMESTAMP, TIMESTAMP_NS, and DATE after an explicit CAST), so
            # this is not a type-mismatch bug at all; it's the tuple-IN
            # pattern's codegen itself. Rewritten below as a standard
            # correlated EXISTS join (column-wise equality, no row/tuple
            # comparison) — a far more common, well-supported query shape.
            conn.execute(
                """
                DELETE FROM ohlcv_adjusted
                WHERE EXISTS (
                    SELECT 1 FROM staging.ohlcv_fyers s
                    WHERE s.ticker = ohlcv_adjusted.ticker AND s.date = ohlcv_adjusted.date
                )
                """
            )
            # Explicit column list — ohlcv_adjusted also has delivery_qty/
            # delivery_pct (NULL for FYERS-sourced rows, FYERS doesn't
            # report delivery data) which staging.ohlcv_fyers doesn't have;
            # a bare `SELECT *` would map columns positionally and either
            # fail or silently misalign.
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted
                    (date, ticker, open, high, low, close, volume, adj_factor, vol_adj_factor, source)
                -- [2026-08-10] Literal 1.0, not staging's adj_factor: a FYERS
                -- row is already adjusted at source, so our factors must
                -- always be 1.0. Same rule as daily_pipeline.py's
                -- step_download_fyers_daily UPSERT — see the note there.
                SELECT date, ticker, open, high, low, close, volume, 1.0, 1.0, 'fyers'
                FROM staging.ohlcv_fyers
                """
            )

        # [2026-08-04, user decision] Feature recompute is NOT triggered
        # per-year during the backfill — deferred to the end, across the
        # whole 2017->present range, once every year's OHLCV has been
        # published. The exact (ticker, date) pairs are persisted (not
        # just a covering date range) so the eventual recompute can be
        # precisely targeted instead of recomputing every ticker for
        # every date in between — see RECOMPUTE_TARGETS_DIR.
        _record_recompute_targets(year, targets)
        logger.info(
            "fyers_staged_backfill: year %d — %d changed/new ticker-days recorded for "
            "later feature recompute (%s..%s)",
            year, len(targets), targets["date"].min().strftime("%Y-%m-%d"),
            targets["date"].max().strftime("%Y-%m-%d"),
        )
    else:
        logger.info("fyers_staged_backfill: year %d — no changes, nothing to recompute", year)

    drop_staging_table(conn, "ohlcv_fyers")
    return summary


def run(start_year: int, end_year: int, dry_run: bool = False) -> None:
    completed = _load_completed_years()
    fb = FYERSBackfill(non_interactive=True)

    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        for year in range(end_year, start_year - 1, -1):
            if year in completed:
                logger.info("fyers_staged_backfill: year %d already completed, skipping", year)
                continue
            summary = backfill_year(conn, fb, year, dry_run=dry_run)
            logger.info("fyers_staged_backfill: year %d done — %s", year, summary)
            if not dry_run:
                _mark_year_complete(year)
                clear_year_cache(year)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Staged multi-year FYERS OHLCV backfill")
    p.add_argument("--start-year", type=int, default=2017)
    p.add_argument("--end-year", type=int, default=date.today().year)
    p.add_argument(
        "--dry-run", action="store_true",
        help="Stage and diff each year but never publish or recompute — inspect counts only.",
    )
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    run(args.start_year, args.end_year, dry_run=args.dry_run)
