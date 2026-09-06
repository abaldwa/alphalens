"""
scripts/trendlyne_backfill_two_phase.py

[2026-09-06] Two-phase Trendlyne fundamentals backfill.

Orchestrates:
  Phase 1: Scrape Trendlyne in small batches, cache locally (no DB lock)
  Phase 2: Persist cached data to DuckDB (brief lock for atomic write)

Usage:
    # Full two-phase run
    .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --tickers 50 --limit 100

    # Phase 1 only (scrape + cache)
    .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --phase 1 --tickers 50

    # Phase 2 only (persist cache to DB)
    .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --phase 2

    # Show what's cached
    .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --status
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.scrapers.trendlyne_cache import TrendlyneScraperCache, TRENDLYNE_CACHE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_phase_1(tickers: int, limit: int = None, dry_run: bool = False):
    """
    Phase 1: Scrape Trendlyne, cache locally.
    No DuckDB lock. Graceful backoff on rate-limiting.
    """
    username = os.getenv("TRENDLYNE_USERNAME")
    password = os.getenv("TRENDLYNE_PASSWORD")

    if not username or not password:
        logger.error("TRENDLYNE_USERNAME/PASSWORD not in .env")
        return False

    # Get list of tickers to fetch
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        all_tickers = [
            r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fundamentals ORDER BY ticker").fetchall()
        ]
    if limit:
        all_tickers = all_tickers[:limit]

    logger.info(f"Phase 1: Scraping {len(all_tickers)} tickers in batches of {tickers}")

    scraper = TrendlyneScraperCache(username, password, batch_size=tickers)
    if not scraper.login():
        logger.error("Login failed")
        return False

    # Process in batches
    total_cached = 0
    for batch_start in range(0, len(all_tickers), tickers):
        batch_tickers = all_tickers[batch_start : batch_start + tickers]

        def progress(current, total, results):
            logger.info(f"  Batch progress: {current}/{total}, cached so far: {len(results)}")

        results = scraper.fetch_batch(batch_tickers, progress_callback=progress)
        total_cached += len(results)
        logger.info(f"Batch {batch_start // tickers + 1}: cached {len(results)}/{len(batch_tickers)} tickers")

    logger.info(f"Phase 1 complete: {total_cached} tickers cached")
    return True


def run_phase_2(dry_run: bool = False):
    """
    Phase 2: Persist cached data to DuckDB.
    Minimal lock hold time (bulk transaction).
    """
    logger.info("Phase 2: Persisting cached data to DuckDB...")

    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "persist_trendlyne_cache.py"),
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0


def show_status():
    """Show what's currently cached."""
    scraper = TrendlyneScraperCache("", "")
    cached = scraper.get_cached_tickers()

    logger.info(f"Cached tickers: {len(cached)}")
    for ticker in sorted(cached)[:20]:
        cache_file = TRENDLYNE_CACHE_DIR / f"{ticker.replace('.', '_')}.json"
        size_kb = cache_file.stat().st_size / 1024
        logger.info(f"  {ticker}: {size_kb:.1f} KB")
    if len(cached) > 20:
        logger.info(f"  ... and {len(cached) - 20} more")


def main():
    parser = argparse.ArgumentParser(description="Two-phase Trendlyne backfill: scrape + persist")
    parser.add_argument(
        "--phase",
        type=int,
        choices=[1, 2],
        default=None,
        help="Run only phase 1 (scrape) or phase 2 (persist). Default: both.",
    )
    parser.add_argument(
        "--tickers",
        type=int,
        default=50,
        help="Batch size for phase 1 scraping (default: 50)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max tickers to scrape (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done, don't write to DB",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show cached data status",
    )
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    # Determine which phases to run
    phases = [args.phase] if args.phase else [1, 2]

    success = True
    if 1 in phases:
        logger.info("=" * 60)
        logger.info("PHASE 1: SCRAPE TRENDLYNE (no DB lock)")
        logger.info("=" * 60)
        if not run_phase_1(args.tickers, args.limit, args.dry_run):
            logger.error("Phase 1 failed")
            success = False

    if 2 in phases:
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 2: PERSIST CACHE TO DB (brief lock)")
        logger.info("=" * 60)
        if not run_phase_2(args.dry_run):
            logger.error("Phase 2 failed")
            success = False

    if success:
        logger.info("")
        logger.info("✓ Two-phase backfill complete")
    else:
        logger.error("")
        logger.error("✗ Backfill failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
