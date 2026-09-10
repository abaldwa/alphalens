"""
scripts/persist_trendlyne_cache.py

[2026-09-06] Phase 2 of two-phase Trendlyne backfill.

Reads cached Trendlyne fundamentals (from ingestion/scrapers/trendlyne_cache.py)
and persists to DuckDB in one bulk transaction. Holds DuckDB lock only briefly.

Usage:
    # Persist whatever data is cached
    .venv/bin/python3 scripts/persist_trendlyne_cache.py

    # Dry-run (show what would be written, don't write)
    .venv/bin/python3 scripts/persist_trendlyne_cache.py --dry-run

    # Clear old cache files (>30 days)
    .venv/bin/python3 scripts/persist_trendlyne_cache.py --clear-cache 30
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.fundamental_quality_gate import validate_and_annotate
from features.fundamental_source_priority import (
    SOURCE_PRIORITY,
)
from ingestion.scrapers.trendlyne_cache import TrendlyneScraperCache, TRENDLYNE_CACHE_DIR
from scripts.backfill_fundamentals_trendlyne import (
    _extract_annual_patch,
    _extract_quarterly_rows,
    _merge_annual,
)

# [2026-09-10 fix] parse_fundamentals_from_cache() below originally assumed
# a cache_data["quarterly"]/["annual"] schema that never matched what
# ingestion.scrapers.trendlyne_cache.py actually caches — real responses
# are Trendlyne's own {"head": ..., "body": {...}} shape (quarterlyOrder/
# quarterlyDataDump/annualOrder/annualDataDump keys), which is why every
# real persist attempt logged "No valid rows parsed from cache" despite
# the cache files genuinely containing data. scripts/backfill_fundamentals_
# trendlyne.py already has proven, tested extraction logic for this exact
# shape (percent-to-fraction conversion, gross_profit/interest_coverage/
# total_debt derivation, annual-into-quarterly merge) — reusing it here
# rather than writing a second, independently-drifting copy.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def parse_fundamentals_from_cache(ticker: str, cache_data: Dict) -> Optional[List[Dict]]:
    """
    Parse raw Trendlyne JSON response from cache into fundamentals rows.
    Returns a list of row dicts, or None if parsing fails / no data.

    cache_data is the real Trendlyne get-fundamental_results-v2 response
    shape: {"head": {...}, "body": {quarterlyOrder, quarterlyDataDump,
    annualOrder, annualDataDump, ...}} — see
    scripts.backfill_fundamentals_trendlyne's extraction functions, reused
    here rather than duplicated.
    """
    if not cache_data or not isinstance(cache_data, dict):
        return None

    body = cache_data.get("body")
    if not body:
        logger.debug(f"{ticker}: no body in cache")
        return None

    q_rows = _extract_quarterly_rows(ticker, body)
    annual_patches = _extract_annual_patch(body)
    merged = _merge_annual(q_rows, annual_patches)

    if not merged:
        logger.debug(f"{ticker}: no quarterly/annual data in cache")
        return None

    for row in merged:
        row["fundamentals_source"] = "trendlyne"
        row["fundamentals_source_priority"] = SOURCE_PRIORITY["trendlyne"]
        validate_and_annotate(row)

    return merged


def persist_cache_to_db(dry_run: bool = False) -> int:
    """
    Read all cached Trendlyne data, validate, and bulk-write to DuckDB.
    Returns count of rows written.
    """
    scraper = TrendlyneScraperCache("", "")  # Just for cache access
    cached_tickers = scraper.get_cached_tickers()

    if not cached_tickers:
        logger.info("No cached data to persist")
        return 0

    logger.info(f"Persisting {len(cached_tickers)} cached tickers...")

    all_rows = []
    for ticker in cached_tickers:
        cache_file = TRENDLYNE_CACHE_DIR / f"{ticker}.json"
        try:
            with open(cache_file) as f:
                cache_data = json.load(f)
            rows = parse_fundamentals_from_cache(ticker, cache_data)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.warning(f"Error parsing {ticker}: {e}")

    if not all_rows:
        logger.info("No valid rows parsed from cache")
        return 0

    logger.info(f"Parsed {len(all_rows)} rows from {len(cached_tickers)} tickers")

    if dry_run:
        logger.info("[dry-run] Would write these rows:")
        for row in all_rows[:10]:
            logger.info(f"  {row['ticker']} FY{row['fiscal_year']}Q{row['quarter']}")
        if len(all_rows) > 10:
            logger.info(f"  ... and {len(all_rows) - 10} more")
        return 0

    # [2026-09-10 fix] The previous direct `INSERT ... ON CONFLICT (ticker,
    # fiscal_year, quarter)` cannot work — live-verified the fundamentals
    # table carries no UNIQUE/PRIMARY KEY constraint on those columns at
    # all (DuckDB raised BinderException the first time this ever actually
    # ran against real data). Switched to the same staged-publish pattern
    # backfill_fundamentals_nse_xbrl.py's staged mode already uses
    # successfully: read existing data (lock released immediately after),
    # merge in-memory, then acquire the lock only for the atomic
    # stage+publish. datastore/staging/merge.py::coalesce_merge's own
    # docstring names Trendlyne's quality_flag/quality_flag_reason as the
    # canonical force_new_wins_cols example — Trendlyne is a lower-priority
    # source (new_wins=False, existing higher-priority values are kept),
    # but its freshly-computed quality flag should still always land.
    import pandas as pd

    from datastore.staging.gate import stage_dataframe
    from datastore.staging.merge import coalesce_merge
    from datastore.staging.publish import publish_run_lock, publish_table

    cols = [
        "ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
        "revenue", "ebitda", "pat", "eps", "roe", "roce", "debt_to_equity",
        "interest_coverage", "ebitda_margin", "asset_turnover",
        "quality_flag", "quality_flag_reason", "fundamentals_source", "fundamentals_source_priority"
    ]
    new_df = pd.DataFrame([{c: row.get(c) for c in cols} for row in all_rows])

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        existing_df = conn.execute("SELECT * FROM fundamentals").df()

    merged_df = coalesce_merge(
        existing_df, new_df, key_cols=["ticker", "fiscal_year", "quarter"],
        new_wins=False,  # trendlyne: existing (higher-priority source) values win
        force_new_wins_cols=["quality_flag", "quality_flag_reason"],
    )

    with publish_run_lock() as acquired:
        if not acquired:
            logger.error("Another publish is in progress — Trendlyne cache NOT published.")
            return 0
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            result = stage_dataframe(conn, "fundamentals", merged_df, validators=[])
            if not result.ok:
                logger.error("Staging gate rejected the entire batch — nothing published.")
                return 0
            published_rows = publish_table(conn, "fundamentals")
            logger.info(f"Staged publish: {len(all_rows)} delta rows merged, {published_rows} now in fundamentals")

    logger.info(f"Persisted {len(all_rows)} rows to fundamentals")
    return len(all_rows)


def main():
    parser = argparse.ArgumentParser(
        description="Persist cached Trendlyne fundamentals to DuckDB"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be written")
    parser.add_argument("--clear-cache", type=int, metavar="DAYS",
                        help="Clear cached files older than N days")
    args = parser.parse_args()

    if args.clear_cache:
        scraper = TrendlyneScraperCache("", "")
        deleted = scraper.clear_old_cache(args.clear_cache)
        logger.info(f"Cleared {deleted} cache files older than {args.clear_cache} days")
        return

    rows_written = persist_cache_to_db(dry_run=args.dry_run)
    logger.info(f"Done: {rows_written} rows persisted")


if __name__ == "__main__":
    main()
