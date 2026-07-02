"""
scripts/backfill_equity_from_screener.py

Phase: 3.11 (Fundamentals — direct shareholder equity)
Specs: CLAUDE.md Absolute Rule 6, SPEC-PIPE-003 (CRITICAL — PIT)
Owner: Platform / Ingestion
Consumers: fundamentals.total_equity (in-place UPDATE), features/financial_ratios.py

Patches `fundamentals.total_equity` (Equity Capital + Reserves, INR Cr) for
every existing row, read per fiscal year from Screener.in's #balance-sheet
table (ingestion/scrapers/screener.py's `_parse_balance_sheet_history`).

One equity value per fiscal year is patched onto every quarter row of that
FY already in the DB — same one-value-per-FY pattern as Trendlyne's annual
fields (scripts/backfill_fundamentals_trendlyne.py). Only UPDATEs rows that
already exist (ticker, fiscal_year) in `fundamentals`; never inserts a new
row, since this script has no quarter_end_date/announcement_date of its own
to assign.

Two source modes
-----------------
--from-cache (default)
    Parses already-downloaded pages under SCREENER_RAW_DIR (no network
    call, no login, no credentials needed) — covers every ticker scraped
    by a previous screener.py run. Safe to re-run any time.
--live
    Logs into screener.in and fetches a fresh page for tickers not
    already covered by --from-cache (or all tickers, with --all), at the
    same SCREENER_RATE_LIMIT_SLEEP_SECONDS pace as the rest of this
    project's screener scraping. Requires SCREENER_USERNAME/PASSWORD.

Usage
-----
    .venv/bin/python3 scripts/backfill_equity_from_screener.py --dry-run
    .venv/bin/python3 scripts/backfill_equity_from_screener.py
    .venv/bin/python3 scripts/backfill_equity_from_screener.py --live --limit 50
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill fundamentals.total_equity from Screener.in")
    parser.add_argument("--live", action="store_true", help="Fetch fresh pages via login instead of using the local cache")
    parser.add_argument("--all", action="store_true", help="With --live, refetch every ticker (default: only tickers missing from the cache)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report but do not write to DB")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N tickers (for testing)")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH, SCREENER_RAW_DIR, SCREENER_RATE_LIMIT_SLEEP_SECONDS
    from ingestion.scrapers.screener import ScreenerScraper

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)
    try:
        db_tickers = {r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fundamentals").fetchall()}

        cached = {p.stem: p for p in Path(SCREENER_RAW_DIR).glob("*.html")}
        cached_tickers = sorted(t for t in cached if t in db_tickers)
        logger.info("DB tickers: %d, cached pages matching DB: %d", len(db_tickers), len(cached_tickers))

        scraper = ScreenerScraper()
        targets = list(cached_tickers)
        if args.live:
            if args.all:
                targets = sorted(db_tickers)
            else:
                targets = sorted(db_tickers - set(cached_tickers))
            logger.info("--live: will fetch %d tickers via login", len(targets))
            scraper.login()

        if args.limit:
            targets = targets[: args.limit]

        n_tickers_updated = 0
        n_rows_updated = 0
        n_no_history = 0
        n_errors = 0

        for i, ticker in enumerate(targets, 1):
            try:
                if args.live:
                    # Sleep unconditionally (success or failure) — a 404/error
                    # must not skip the rate limit, or a long run of bad
                    # tickers hammers screener.in back-to-back with zero
                    # delay (caught live: 8 consecutive 404s fired in under
                    # half a second before this fix).
                    try:
                        history = scraper.export_equity_history(ticker)
                    finally:
                        time.sleep(SCREENER_RATE_LIMIT_SLEEP_SECONDS)
                else:
                    html = cached[ticker].read_text(encoding="utf-8")
                    history = scraper.export_equity_history(ticker, html=html)

                if not history:
                    n_no_history += 1
                    continue

                existing_fys = {
                    row[0] for row in conn.execute(
                        "SELECT DISTINCT fiscal_year FROM fundamentals WHERE ticker = ?", [ticker]
                    ).fetchall()
                }

                ticker_touched = False
                for fiscal_year, equity_cr in history.items():
                    if fiscal_year not in existing_fys:
                        continue
                    if not args.dry_run:
                        conn.execute(
                            "UPDATE fundamentals SET total_equity = ? WHERE ticker = ? AND fiscal_year = ?",
                            [equity_cr, ticker, fiscal_year],
                        )
                    n_rows_updated += 1  # counts (ticker, fiscal_year) groups, not raw row count
                    ticker_touched = True
                if ticker_touched:
                    n_tickers_updated += 1
            except Exception as exc:
                logger.warning("equity history failed for %s: %s", ticker, exc)
                n_errors += 1

            if i % 200 == 0:
                logger.info("Progress: %d/%d tickers processed", i, len(targets))

        total = conn.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
        populated = conn.execute("SELECT COUNT(*) FROM fundamentals WHERE total_equity IS NOT NULL").fetchone()[0]

        logger.info("Done. Tickers processed: %d", len(targets))
        logger.info("  Tickers with equity history found : %d", n_tickers_updated)
        logger.info("  No balance-sheet history on page   : %d", n_no_history)
        logger.info("  Errors                             : %d", n_errors)
        logger.info("  (ticker, fiscal_year) groups patched: %d", n_rows_updated)
        logger.info("  total_equity completeness now: %d/%d (%.1f%%)", populated, total, 100.0 * populated / total if total else 0.0)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
