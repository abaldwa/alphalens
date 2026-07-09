"""
scripts/reconcile_bulk_deal_families.py

Phase D (Big Investor Activity — plan: gentle-wobbling-swing.md)

Two-step manual/quarterly workflow:
  1. Pull the latest per-investor named holdings from Trendlyne and
     upsert into public_shareholders (ingestion/scrapers/trendlyne.py).
  2. Reconcile bulk_deal_positions against public_shareholders for a
     given quarter (ingestion/scrapers/bulk_deal_reconciliation.py).

Not wired into the daily scheduler — quarterly shareholding filings don't
arrive on a daily cadence, so this is triggered manually (or by a future
low-frequency scheduled job once real filing-date cadence is observed).

Usage:
    python -m scripts.reconcile_bulk_deal_families --fetch --quarter-end 2026-06-30
    python -m scripts.reconcile_bulk_deal_families --quarter-end 2026-06-30
"""

import argparse
import logging
from datetime import date

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.bulk_deal_reconciliation import reconcile_quarter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quarter-end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--fetch", action="store_true", help="Also fetch fresh Trendlyne named holdings first")
    args = parser.parse_args()

    quarter_end = date.fromisoformat(args.quarter_end)

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        if args.fetch:
            from ingestion.scrapers.trendlyne import TrendlyneScraper

            n = TrendlyneScraper().batch_export_named_holdings(conn)
            logger.info(f"Fetched and upserted {n} public_shareholders rows")

        results = reconcile_quarter(conn, quarter_end)

    for r in results:
        logger.info(f"  {r['family_id']}/{r['ticker']}: {r['status']} (discrepancy_pct={r.get('discrepancy_pct')})")
    n_flagged = sum(1 for r in results if r["status"] == "flagged_for_review")
    logger.info(f"Done: {len(results)} pairs, {n_flagged} flagged for review")


if __name__ == "__main__":
    main()
