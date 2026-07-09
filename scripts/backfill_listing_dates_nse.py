"""
scripts/backfill_listing_dates_nse.py

One-off/rerunnable backfill of stock_master.listing_date from NSE's real,
free "public past issues" API — see ingestion/scrapers/nse_ipo.py's module
docstring for the endpoint and its 2026-07-07 discovery. stock_master had
0/1626 tickers with a real listing_date before this (confirmed via
`SELECT COUNT(*) FROM stock_master WHERE listing_date IS NOT NULL`),
silently keeping ipo_lockin_expiry_proximity/ipo_listing_age_months
(features/corporate_action_features.py) permanently NaN — not because the
data was unavailable, but because nothing had ever fetched it.

Usage:
    .venv/bin/python3 scripts/backfill_listing_dates_nse.py [--dry-run]
"""

import argparse
import logging

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.nse_ipo import download_past_issues

logger = logging.getLogger(__name__)

_UPDATE_LISTING_DATE = """
    UPDATE stock_master SET listing_date = ?
    WHERE ticker = ? AND listing_date IS NULL
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill stock_master.listing_date from NSE's real IPO history")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    issues = download_past_issues()
    logger.info(f"Fetched {len(issues)} real NSE past-issue records")

    if args.dry_run:
        matched = 0
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            for ticker, listing_date in issues.items():
                row = conn.execute("SELECT 1 FROM stock_master WHERE ticker = ?", [ticker]).fetchone()
                if row:
                    matched += 1
        logger.info(f"Dry run: {matched}/{len(issues)} tickers exist in stock_master and would be updated")
        return

    # persist=False (SPEC-SCHED-013): one-off script, release the write lock immediately after.
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        before = conn.execute("SELECT COUNT(*) FROM stock_master WHERE listing_date IS NOT NULL").fetchone()[0]
        for ticker, listing_date in issues.items():
            conn.execute(_UPDATE_LISTING_DATE, [listing_date, ticker])
        after = conn.execute("SELECT COUNT(*) FROM stock_master WHERE listing_date IS NOT NULL").fetchone()[0]
    logger.info(f"stock_master.listing_date populated: {before} -> {after} tickers ({after - before} newly written)")


if __name__ == "__main__":
    main()
