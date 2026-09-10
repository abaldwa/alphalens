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
    append_fundamentals_history,
    build_priority_update_clause,
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

    # ===== LOCK-CRITICAL SECTION: Minimal time =====
    # Write all rows in one bulk transaction
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        # Create temp table with exact schema
        conn.execute("CREATE TEMP TABLE trendlyne_delta AS SELECT * FROM fundamentals WHERE FALSE")

        # Insert all rows
        cols = [
            "ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
            "revenue", "ebitda", "pat", "eps", "roe", "roce", "debt_to_equity",
            "interest_coverage", "ebitda_margin", "asset_turnover",
            "quality_flag", "quality_flag_reason", "fundamentals_source", "fundamentals_source_priority"
        ]
        placeholders = ", ".join("?" for _ in cols)
        rows_tuple = [tuple(row.get(c) for c in cols) for row in all_rows]

        conn.executemany(
            f"INSERT INTO trendlyne_delta ({', '.join(cols)}) VALUES ({placeholders})",
            rows_tuple,
        )

        # Upsert: new Trendlyne data overwrites existing (unless from higher-priority source)
        update_cols = [
            "revenue", "ebitda", "pat", "eps", "roe", "roce", "debt_to_equity",
            "interest_coverage", "ebitda_margin", "asset_turnover",
            "quality_flag", "quality_flag_reason"
        ]
        update_clause = build_priority_update_clause(update_cols)

        conn.execute(
            f"""
            INSERT INTO fundamentals ({', '.join(cols)}, as_of_ingested)
            SELECT {', '.join(cols)}, CURRENT_TIMESTAMP FROM trendlyne_delta
            ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {update_clause}
            """
        )

        # History snapshots
        for row in all_rows:
            append_fundamentals_history(
                conn, row["ticker"], row["fiscal_year"], row["quarter"]
            )

    # ===== END LOCK-CRITICAL SECTION =====

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
