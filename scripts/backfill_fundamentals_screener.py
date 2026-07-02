"""
scripts/backfill_fundamentals_screener.py

Phase: 3 (Fundamentals + Shareholding Backfill)
Specs: SPEC-PIPE-003 (CRITICAL — PIT), SPEC-SEC-001
Owner: Platform / Ingestion
Consumers: features/fundamental.py, features/governance.py

Runs the Screener.in Premium scraper across tickers, writing the most recent
10-12 quarters of fundamentals and shareholding into the DB.

PREREQUISITES
-------------
1. Set credentials in .env:
       SCREENER_USERNAME=your@email.com
       SCREENER_PASSWORD=yourpassword

2. Start the DataStore API in another terminal:
       .venv/bin/uvicorn datastore.api.main:app --host 127.0.0.1 --port 8000

Usage
-----
    # ~2492-ticker active universe (default)
    .venv/bin/python3 scripts/backfill_fundamentals_screener.py

    # All tickers in ohlcv_adjusted (~4100 tickers, ~4-6 hours)
    .venv/bin/python3 scripts/backfill_fundamentals_screener.py --all-db-tickers

    # Skip tickers already in the fundamentals table (resume after a crash)
    .venv/bin/python3 scripts/backfill_fundamentals_screener.py --all-db-tickers --skip-existing

    # Background run with log
    nohup .venv/bin/python3 scripts/backfill_fundamentals_screener.py \\
        --all-db-tickers --skip-existing \\
        > logs/screener_backfill.log 2>&1 &
    tail -f logs/screener_backfill.log

Timing
------
    ~3-5 s per ticker (Screener rate-limit).
    2492 tickers →  ~2-3.5 hours.
    4100 tickers →  ~4-6 hours (run overnight, --all-db-tickers).
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _check_api() -> bool:
    import httpx
    from config.settings import DATASTORE_API_BASE_URL
    try:
        r = httpx.get(f"{DATASTORE_API_BASE_URL}/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill fundamentals + shareholding via Screener.in")
    parser.add_argument("--all-db-tickers", action="store_true",
                        help="Scrape all tickers in ohlcv_adjusted (default: ~2492-ticker active universe)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip tickers that already have a row in the fundamentals table")
    parser.add_argument("--tickers-file", type=Path,
                        help="Only scrape tickers listed in this file (one ticker per line), "
                             "overriding --all-db-tickers / universe pool.")
    args = parser.parse_args()

    from config.settings import SCREENER_USERNAME, SCREENER_PASSWORD, DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.screener import ScreenerScraper

    # Guard: credentials
    if not SCREENER_USERNAME or not SCREENER_PASSWORD:
        logger.error(
            "SCREENER_USERNAME and SCREENER_PASSWORD must be set in .env\n"
            "  echo 'SCREENER_USERNAME=your@email.com' >> .env\n"
            "  echo 'SCREENER_PASSWORD=yourpassword'   >> .env"
        )
        sys.exit(1)

    # Guard: API server
    if not _check_api():
        logger.error(
            "DataStore API is not reachable. Start it first:\n"
            "  .venv/bin/uvicorn datastore.api.main:app --host 127.0.0.1 --port 8000"
        )
        sys.exit(1)

    # Build ticker list
    if args.tickers_file:
        tickers = [
            line.strip() for line in args.tickers_file.read_text().splitlines() if line.strip()
        ]
        logger.info("Using --tickers-file pool: %d tickers", len(tickers))
    elif args.all_db_tickers:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker"
            ).fetchall()]
        logger.info("Using all-DB ticker pool: %d tickers", len(tickers))
    else:
        from config.universe import get_tickers
        tickers = get_tickers()
        logger.info("Using universe ticker pool: %d tickers", len(tickers))

    # Optionally skip tickers already in fundamentals
    if args.skip_existing:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            existing = {r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM fundamentals"
            ).fetchall()}
        before = len(tickers)
        tickers = [t for t in tickers if t not in existing]
        logger.info("--skip-existing: %d already in DB, %d remaining", before - len(tickers), len(tickers))

    if not tickers:
        logger.info("Nothing to do.")
        return

    logger.info("Starting Screener backfill for %d tickers", len(tickers))
    t_start = time.monotonic()

    scraper = ScreenerScraper()
    scraper.login()

    results = scraper.batch_export(tickers, write=True)

    ok     = sum(1 for v in results.values() if v)
    failed = [t for t, v in results.items() if not v]
    elapsed_min = (time.monotonic() - t_start) / 60

    logger.info("Screener backfill complete in %.1f min: %d/%d succeeded",
                elapsed_min, ok, len(tickers))
    if failed:
        logger.warning("Failed tickers (%d): %s", len(failed), ", ".join(failed[:30]))
        if len(failed) > 30:
            logger.warning("  ... and %d more", len(failed) - 30)

    # Final DB counts
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        f = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM fundamentals").fetchone()
        s = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM shareholding").fetchone()
    logger.info("fundamentals : %d rows, %d tickers", f[0], f[1])
    logger.info("shareholding : %d rows, %d tickers", s[0], s[1])


if __name__ == "__main__":
    main()
