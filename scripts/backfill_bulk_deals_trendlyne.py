"""
scripts/backfill_bulk_deals_trendlyne.py

Phase E (Big Investor Activity) — one-off/manual historical backfill.

large_deals only carries whatever NSE/BSE ingestion (ingestion/scrapers/
large_deals.py) has run for so far — as of 2026-07-05 that's a single
trade_date, since the daily pipeline only started recently and NSE/BSE's
own live endpoints don't offer a historical date range (see that module's
docstring). Trendlyne's per-investor bulk-block-deals page, however,
republishes each superstar investor's full disclosed bulk/block-deal
history with a real trade date and price per row, going back to 2010 for
at least one investor checked live (confirmed 2026-07-05) — see
ingestion/scrapers/trendlyne.py's TrendlyneScraper.backfill_bulk_deals_history
and _parse_bulk_block_deals_table docstrings for the verification detail.

This script:
  1. Scrapes all ~62 SUPERSTAR_INVESTORS' bulk-block-deals pages and
     inserts any not-already-present rows into large_deals (dedup'd, never
     deletes existing rows — see backfill_bulk_deals_history's docstring).
  2. Rebuilds bulk_deal_positions for every distinct trade_date touched by
     step 1, oldest-to-newest (ingestion.scrapers.bulk_deal_attribution.
     attribute_bulk_deals's cumulative_position_est is a running total, so
     it must be rebuilt in date order, not just for "today").

Not wired into the daily scheduler — this is a one-time historical catch-up
(plus safe to re-run later for newer investors/deals), not a daily step.

Usage:
    python -m scripts.backfill_bulk_deals_trendlyne
    python -m scripts.backfill_bulk_deals_trendlyne --no-write   # scrape + parse only, print counts
"""

import argparse
import logging

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-write", action="store_true", help="Scrape + parse only, skip DB writes")
    args = parser.parse_args()

    from ingestion.scrapers.trendlyne import TrendlyneScraper

    scraper = TrendlyneScraper()

    if args.no_write:
        df = scraper.export_bulk_deals_history()
        print(f"Scraped {len(df)} rows (across {df['trade_date'].nunique() if not df.empty else 0} distinct dates) — not written (--no-write)")
        return

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        inserted = scraper.backfill_bulk_deals_history(conn)
        logger.info(f"Inserted {inserted} new large_deals rows from Trendlyne")

        if inserted == 0:
            logger.info("No new rows — skipping bulk_deal_positions rebuild")
            return

        from ingestion.scrapers.bulk_deal_attribution import attribute_bulk_deals

        dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM large_deals ORDER BY trade_date"
            ).fetchall()
        ]
        logger.info(f"Rebuilding bulk_deal_positions for {len(dates)} distinct trade_date(s), oldest first")
        total_written = 0
        for d in dates:
            written = attribute_bulk_deals(conn, d)
            total_written += written
        logger.info(f"attribute_bulk_deals: {total_written} total (family, ticker, deal_type) position rows across {len(dates)} dates")


if __name__ == "__main__":
    main()
