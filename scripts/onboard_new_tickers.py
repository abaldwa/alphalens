"""
scripts/onboard_new_tickers.py

Weekly job: detect NSE tickers that have listed since we last checked,
pull their full history from FYERS, write it into ohlcv_adjusted with the
same force-upsert semantics scripts/fyers_force_overwrite_tickers.py uses
for existing tickers, then rebuild the universe CSV and stock_master so
the new ticker is "current" everywhere else in the system reads from.

Detection source: ingestion/scrapers/nse_ipo.py::download_past_issues(),
which returns {ticker: listing_date} for every NSE issue that has ever
actually listed (not just upcoming ones). "New" = present there but with
zero rows in ohlcv_adjusted today — this also naturally re-attempts any
ticker whose previous onboarding attempt failed (auth expiry, FYERS
symbol not yet mapped, etc.), since it still has zero rows.

A ticker can legitimately have zero FYERS history even after several
retries (FYERS's symbol-master lag for very recent listings is common —
see FYERSBackfill.download_history's existing "symbol not found"
handling) — this is logged and skipped, not fatal, same tolerance as
fyers_force_overwrite_tickers.py already has for auth/other errors per
ticker.

Usage
-----
    .venv/bin/python3 scripts/onboard_new_tickers.py
    .venv/bin/python3 scripts/onboard_new_tickers.py --dry-run
"""

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHECKPOINT_PATH = Path("datastore/raw/fyers/onboard_new_tickers_done.txt")

# Same invariant as fyers_force_overwrite_tickers.py: FYERS rows are
# already adjusted at source, so adj_factor/vol_adj_factor are always 1.0.
_FORCE_UPSERT_SQL = """
    INSERT INTO ohlcv_adjusted
        (date, ticker, open, high, low, close, volume, adj_factor, vol_adj_factor, source)
    VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, 1.0, 'fyers')
    ON CONFLICT (date, ticker) DO UPDATE SET
        open = excluded.open, high = excluded.high, low = excluded.low,
        close = excluded.close, volume = excluded.volume,
        adj_factor = 1.0, vol_adj_factor = 1.0, source = 'fyers'
"""


def _load_done() -> set:
    if not CHECKPOINT_PATH.exists():
        return set()
    return set(CHECKPOINT_PATH.read_text().splitlines())


def _mark_done(ticker: str) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_PATH, "a") as f:
        f.write(ticker + "\n")


def _existing_tickers() -> set:
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        rows = conn.execute("SELECT DISTINCT ticker FROM ohlcv_adjusted").fetchall()
    return {r[0] for r in rows}


def _write_with_lock_retry(ticker: str, df) -> int:
    import duckdb

    if df.empty:
        return 0
    rows = list(
        df[["date", "ticker", "open", "high", "low", "close", "volume"]].itertuples(
            index=False, name=None
        )
    )
    attempt = 0
    while True:
        try:
            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                conn.executemany(_FORCE_UPSERT_SQL, rows)
                return len(rows)
        except duckdb.IOException as exc:
            attempt += 1
            wait_seconds = min(30 * attempt, 300)
            logger.warning(f"{ticker}: DB locked (attempt {attempt}) — waiting {wait_seconds}s: {exc}")
            time.sleep(wait_seconds)


_AUTH_ERROR_MARKERS = ("authenticate", "token", "unauthoriz", "auth_code", "invalid access")


def _looks_like_auth_error(exc: Exception) -> bool:
    return any(marker in str(exc).lower() for marker in _AUTH_ERROR_MARKERS)


def main() -> None:
    import argparse
    from datetime import date

    from ingestion.scrapers.fyers_backfill import FYERSBackfill
    from ingestion.scrapers.nse_ipo import download_past_issues

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="List new tickers found, do not fetch/write")
    args = parser.parse_args()

    logger.info("Fetching NSE past-issues list...")
    all_issues = download_past_issues()
    logger.info(f"NSE past-issues: {len(all_issues)} ever-listed tickers")

    existing = _existing_tickers()
    done = _load_done()
    new_tickers = sorted(t for t in all_issues if t not in existing and t not in done)

    if not new_tickers:
        logger.info("No new tickers found — nothing to onboard.")
        return

    logger.info(f"{len(new_tickers)} new ticker(s) to onboard: {', '.join(new_tickers)}")
    if args.dry_run:
        return

    today = date.today().isoformat()
    client = FYERSBackfill(non_interactive=True)
    onboarded = []
    for ticker in new_tickers:
        listing_date = all_issues[ticker].isoformat()
        try:
            df = client.download_history(ticker, listing_date, today)
        except Exception as exc:
            if _looks_like_auth_error(exc):
                logger.error(f"{ticker}: FYERS auth invalid — aborting run, will retry next week")
                break
            logger.warning(f"{ticker}: FYERS download failed: {exc} — skipping, will retry next week")
            continue

        if df.empty:
            logger.info(f"{ticker}: FYERS returned no rows (symbol not yet mapped?) — skipping, will retry next week")
            continue

        rows_written = _write_with_lock_retry(ticker, df)
        _mark_done(ticker)
        onboarded.append(ticker)
        logger.info(f"{ticker}: {rows_written} rows written (listed {listing_date})")

    if not onboarded:
        logger.info("Done — no tickers successfully onboarded this run.")
        return

    logger.info(f"Onboarded {len(onboarded)} ticker(s): {', '.join(onboarded)} — refreshing universe + stock_master")
    from config.build_universe import build_full_nse_universe_from_db

    build_full_nse_universe_from_db()

    import subprocess

    subprocess.run(
        [sys.executable, "scripts/sync_stock_master_from_universe.py"],
        capture_output=False, timeout=600,
    )
    logger.info("Done.")


if __name__ == "__main__":
    main()
