"""
scripts/fyers_force_overwrite_tickers.py

One-off: re-pull raw FYERS OHLCV for a fixed, explicit ticker list across
their full available history and force-overwrite ohlcv_adjusted with it,
resetting adj_factor/vol_adj_factor to 1.0 for every touched row — same
force-overwrite semantics as scripts/fyers_force_overwrite_window.py, but
scoped by TICKER (arbitrary list) instead of by date window (full universe).

User-requested 2026-08-01: 2026-07-28 data_integrity_check's spot_check
found 6 tickers whose stored close diverges from BOTH Fyers and Yahoo
(which agree with each other) on specific historical dates — the same
corruption signature as the BAJFINANCE/COFORGE/SHRIRAMFIN/NAVA fix
(FeatureBacklogImplemented.md, 2026-07-30), but for different tickers:
GMBREW, SHILPAMED, CHOICEIN, KPIGREEN, KSB, CANTABIL. Re-pulling each
ticker's FULL history (not just the one flagged date) since a bad
adj_factor typically corrupts a ticker's entire pre-event price series,
not just the sampled spot-check date.

Also includes POCL/JLHL (FeatureBacklog.md CA6): their registered SPLIT
ratio didn't match the price factor actually applied in ohlcv_adjusted,
and it was unclear which side (registered ratio vs. applied adjustment)
was wrong. Per explicit user instruction, sidestepped that ambiguity —
same raw-Fyers/no-adjustment treatment as the 6 tickers above, rather
than editing the corporate_actions ratio.
"""

import logging
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TICKERS = ["GMBREW", "SHILPAMED", "CHOICEIN", "KPIGREEN", "KSB", "CANTABIL", "POCL", "JLHL"]
FROM_DATE = "2007-01-01"
TO_DATE = None  # filled with today at runtime
CHECKPOINT_PATH = Path("datastore/raw/fyers/force_overwrite_spotcheck_tickers_done.txt")
IST = ZoneInfo("Asia/Kolkata")

_FORCE_UPSERT_SQL = """
    INSERT INTO ohlcv_adjusted
        (date, ticker, open, high, low, close, volume, adj_factor, vol_adj_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, 1.0)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open = excluded.open, high = excluded.high, low = excluded.low,
        close = excluded.close, volume = excluded.volume,
        adj_factor = 1.0, vol_adj_factor = 1.0
"""


def _load_done() -> set:
    if not CHECKPOINT_PATH.exists():
        return set()
    return set(CHECKPOINT_PATH.read_text().splitlines())


def _mark_done(ticker: str) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_PATH, "a") as f:
        f.write(ticker + "\n")


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


def _wait_for_fresh_token(poll_seconds: int = 300) -> FYERSBackfill:
    while True:
        logger.warning(
            f"FYERS auth appears invalid/expired — waiting {poll_seconds}s for a fresh "
            f"token (run `python3 -m ingestion.scrapers.fyers_backfill exchange <URL>`)"
        )
        time.sleep(poll_seconds)
        candidate = FYERSBackfill(non_interactive=True)
        try:
            candidate.get_access_token()
            logger.info("FYERS token refreshed successfully — resuming")
            return candidate
        except Exception as exc:
            logger.warning(f"Still no valid FYERS token: {exc}")


def main() -> None:
    from datetime import date

    to_date = TO_DATE or date.today().isoformat()
    done = _load_done()
    remaining = [t for t in TICKERS if t not in done]
    logger.info(f"{len(TICKERS)} tickers total, {len(done)} already done, {len(remaining)} remaining")

    client = FYERSBackfill(non_interactive=True)
    i = 0
    while i < len(remaining):
        ticker = remaining[i]
        try:
            df = client.download_history(ticker, FROM_DATE, to_date)
        except Exception as exc:
            if _looks_like_auth_error(exc):
                client = _wait_for_fresh_token()
                continue
            logger.error(f"{ticker}: FYERS download failed: {exc} — skipping")
            _mark_done(ticker)
            i += 1
            continue

        rows_written = _write_with_lock_retry(ticker, df)
        _mark_done(ticker)
        logger.info(f"{ticker}: {rows_written} rows overwritten")
        i += 1

    logger.info("Done.")


if __name__ == "__main__":
    main()
