"""
scripts/backfill_balance_sheet_from_screener.py

Phase: 3.1 (Deep Forensic ML Features — Groups D-I)
Specs: CLAUDE.md Absolute Rule 6, SPEC-PIPE-003 (CRITICAL — PIT)
Owner: Platform / Ingestion
Consumers: features/deep_forensic.py's cwip_ratio, asset_inflation_flag, altman_z

Patches `fundamentals.total_assets` and `fundamentals.cwip` (both INR Cr)
for the most recent quarter row of every ticker, parsed from the
rightmost (current) column of Screener.in's #balance-sheet table
(ingestion/scrapers/screener.py's `_BALANCE_SHEET_FIELDS` / `_parse_section_table`,
same real "Total Assets"/"CWIP" rows already verified live against TCS —
see datastore/schema/create_normalised.py's total_assets column comment).

Why this script exists: the schema columns and parsing logic for
total_assets/cwip were added in an earlier session, but nothing had ever
actually WRITTEN a value into an existing DB row — `export_company_data()`
only populates them going forward on a fresh live scrape, and no fresh
scrape had run since. Confirmed live: `SELECT COUNT(*) FROM fundamentals
WHERE total_assets IS NOT NULL` was 0 despite ~3,300 cached raw pages
already sitting under SCREENER_RAW_DIR with the real data on them. This
backfills from that existing cache — no network call, no login needed.

Only patches the ticker's single most-recent `quarter_end_date` row
(matching `_current_quarter_end()`'s convention), since
`_parse_section_table` reads only the rightmost/current column of the
balance sheet (unlike `_parse_balance_sheet_history`'s full multi-year
read used for total_equity/retained_earnings) — this is a current
snapshot, not a full history backfill.

Usage
-----
    .venv/bin/python3 scripts/backfill_balance_sheet_from_screener.py --dry-run
    .venv/bin/python3 scripts/backfill_balance_sheet_from_screener.py
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill fundamentals.total_assets/cwip from cached Screener.in pages")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report but do not write to DB")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N tickers (for testing)")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH, SCREENER_RAW_DIR
    from ingestion.scrapers.screener import _BALANCE_SHEET_FIELDS, _parse_section_table

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)
    try:
        db_tickers = {r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fundamentals").fetchall()}
        cached = {p.stem: p for p in Path(SCREENER_RAW_DIR).glob("*.html")}
        targets = sorted(t for t in cached if t in db_tickers)
        if args.limit:
            targets = targets[: args.limit]
        logger.info("DB tickers: %d, cached pages matching DB: %d, targets: %d", len(db_tickers), len(cached), len(targets))

        n_updated = 0
        n_no_data = 0
        n_errors = 0

        for i, ticker in enumerate(targets, 1):
            try:
                html = cached[ticker].read_text(encoding="utf-8", errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                balance_sheet = _parse_section_table(soup, "balance-sheet", _BALANCE_SHEET_FIELDS)
                total_assets = balance_sheet.get("total_assets")
                cwip = balance_sheet.get("cwip")
                if total_assets is None and cwip is None:
                    n_no_data += 1
                    continue

                # Patch onto the ticker's most recent quarter_end_date row only
                # (the parsed value is the CURRENT/rightmost balance-sheet
                # column, not a full FY history like total_equity's backfill).
                latest_qed = conn.execute(
                    "SELECT MAX(quarter_end_date) FROM fundamentals WHERE ticker = ?", [ticker]
                ).fetchone()[0]
                if latest_qed is None:
                    n_no_data += 1
                    continue

                if not args.dry_run:
                    conn.execute(
                        "UPDATE fundamentals SET total_assets = COALESCE(?, total_assets), "
                        "cwip = COALESCE(?, cwip) WHERE ticker = ? AND quarter_end_date = ?",
                        [total_assets, cwip, ticker, latest_qed],
                    )
                n_updated += 1
            except Exception as exc:
                logger.warning("balance-sheet parse failed for %s: %s", ticker, exc)
                n_errors += 1

            if i % 500 == 0:
                logger.info("Progress: %d/%d tickers processed", i, len(targets))

        total = conn.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
        populated_ta = conn.execute("SELECT COUNT(*) FROM fundamentals WHERE total_assets IS NOT NULL").fetchone()[0]
        populated_cwip = conn.execute("SELECT COUNT(*) FROM fundamentals WHERE cwip IS NOT NULL").fetchone()[0]

        logger.info("Done. Tickers processed: %d", len(targets))
        logger.info("  Tickers updated       : %d", n_updated)
        logger.info("  No data on page        : %d", n_no_data)
        logger.info("  Errors                 : %d", n_errors)
        logger.info("  total_assets populated : %d/%d (%.1f%%)", populated_ta, total, 100.0 * populated_ta / total if total else 0.0)
        logger.info("  cwip populated         : %d/%d (%.1f%%)", populated_cwip, total, 100.0 * populated_cwip / total if total else 0.0)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
