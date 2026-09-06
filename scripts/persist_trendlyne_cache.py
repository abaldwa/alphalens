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
from datetime import date
from pathlib import Path
from typing import Dict, Optional

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def parse_fundamentals_from_cache(ticker: str, cache_data: Dict) -> Optional[Dict]:
    """
    Parse raw Trendlyne JSON response from cache into fundamentals row.
    Returns dict with ticker/fiscal_year/quarter/announcement_date/...fields
    or None if parsing fails.
    """
    if not cache_data or not isinstance(cache_data, dict):
        return None

    # Extract quarterly and annual data
    quarterly = cache_data.get("quarterly", [])
    annual = cache_data.get("annual", [])

    if not quarterly and not annual:
        logger.debug(f"{ticker}: no data in cache")
        return None

    results = []

    # Parse quarterly data
    for q_row in quarterly:
        row = {
            "ticker": ticker,
            "fiscal_year": q_row.get("fy"),
            "quarter": q_row.get("quarter"),
            "quarter_end_date": q_row.get("quarter_end_date"),
            "announcement_date": q_row.get("announcement_date") or date.today(),
            "revenue": q_row.get("revenue"),
            "ebitda": q_row.get("ebitda"),
            "pat": q_row.get("pat"),
            "eps": q_row.get("eps"),
            "fundamentals_source": "trendlyne",
            "fundamentals_source_priority": SOURCE_PRIORITY["trendlyne"],
        }
        validate_and_annotate(row)
        results.append(row)

    # Parse annual data (replicate across quarters)
    for a_row in annual:
        fy = a_row.get("fy")
        for q in range(1, 5):
            row = {
                "ticker": ticker,
                "fiscal_year": fy,
                "quarter": q,
                "quarter_end_date": None,  # Annual data, no specific quarter date
                "announcement_date": date.today(),
                "roe": a_row.get("roe"),
                "roce": a_row.get("roce"),
                "debt_to_equity": a_row.get("debt_to_equity"),
                "interest_coverage": a_row.get("interest_coverage"),
                "ebitda_margin": a_row.get("ebitda_margin"),
                "asset_turnover": a_row.get("asset_turnover"),
                "fundamentals_source": "trendlyne",
                "fundamentals_source_priority": SOURCE_PRIORITY["trendlyne"],
            }
            validate_and_annotate(row)
            results.append(row)

    return results


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
        cache_file = TRENDLYNE_CACHE_DIR / f"{ticker.replace('.', '_')}.json"
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
