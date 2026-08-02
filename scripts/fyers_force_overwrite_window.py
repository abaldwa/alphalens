"""
scripts/fyers_force_overwrite_window.py

One-off: re-pull raw FYERS OHLCV for a fixed date window across the full
universe and force-overwrite ohlcv_adjusted with it, resetting adj_factor
and vol_adj_factor to 1.0 for every touched row (unlike
ingestion/backfill_runner.py's normal upsert, which leaves adj_factor
untouched on conflict — that path is for extending history, not for
replacing an already-adjusted window with unadjusted values).

User-requested 2026-08-01: 2024-07-08..2024-07-31, full universe, "Adjusted
OHLCV with no adjustment factor applied" == raw FYERS values with
adj_factor/vol_adj_factor forced to 1.0 for this window.

[2026-08-01] Previously slept until the next Asia/Kolkata midnight on
FYERS_MAX_CALLS_PER_DAY exhaustion — that daily-call cap was a project-
chosen planning number, not a FYERS-documented limit, and was removed at
explicit user request (see ingestion/scrapers/fyers_backfill.py's
_throttle()). A own checkpoint file (separate from the daily pipeline's
FYERS_RESUME_CHECKPOINT_PATH) tracks completed tickers so a restart after
a crash does not redo already-done tickers.
"""

import argparse
import logging
import time
from pathlib import Path

from config.settings import DUCKDB_PATH
from config.universe import get_tickers
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FORCE_UPSERT_SQL = """
    INSERT INTO ohlcv_adjusted
        (date, ticker, open, high, low, close, volume, adj_factor, vol_adj_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, 1.0)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open = excluded.open, high = excluded.high, low = excluded.low,
        close = excluded.close, volume = excluded.volume,
        adj_factor = 1.0, vol_adj_factor = 1.0
"""


def _load_done(checkpoint_path: Path) -> set:
    if not checkpoint_path.exists():
        return set()
    return set(checkpoint_path.read_text().splitlines())


def _mark_done(checkpoint_path: Path, ticker: str) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_path, "a") as f:
        f.write(ticker + "\n")


_AUTH_ERROR_MARKERS = ("authenticate", "token", "unauthoriz", "auth_code", "invalid access")


def _looks_like_auth_error(exc: Exception) -> bool:
    return any(marker in str(exc).lower() for marker in _AUTH_ERROR_MARKERS)


def _wait_for_fresh_token(poll_seconds: int = 300) -> "FYERSBackfill":
    """
    Block until a valid FYERS token is available again, polling the token
    cache file periodically instead of assuming a mid-run refresh (the
    running process's FYERSBackfill instance caches its token in memory
    forever, so it never notices a same-day re-`exchange` on its own — a
    fresh instance is required to re-read the cache file/env each time).

    Never marks the current ticker done — an expired token is not a
    per-ticker failure, and treating it as one would silently mark every
    remaining ticker as complete with 0 rows written.
    """
    while True:
        logger.warning(
            f"FYERS auth appears invalid/expired — waiting {poll_seconds}s for a "
            f"fresh token (run `python3 -m ingestion.scrapers.fyers_backfill exchange "
            f"<redirected URL or auth_code>` to refresh it), then retrying."
        )
        time.sleep(poll_seconds)
        candidate = FYERSBackfill(non_interactive=True)
        try:
            candidate.get_access_token()
            logger.info("FYERS token refreshed successfully — resuming")
            return candidate
        except Exception as exc:
            logger.warning(f"Still no valid FYERS token: {exc}")


def write_rows(conn, ticker: str, df) -> int:
    if df.empty:
        return 0
    rows = list(
        df[["date", "ticker", "open", "high", "low", "close", "volume"]].itertuples(
            index=False, name=None
        )
    )
    conn.executemany(_FORCE_UPSERT_SQL, rows)
    return len(rows)


def _write_with_lock_retry(ticker: str, df) -> int:
    """
    Write one ticker's rows, waiting out DuckDB single-writer lock contention
    from other long-running jobs (e.g. feature_backfill_hybrid.py) instead of
    crashing the whole multi-day run when get_duckdb_connection's own
    (short, ~16s total) retry budget is exhausted.
    """
    import duckdb

    attempt = 0
    while True:
        try:
            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                return write_rows(conn, ticker, df)
        except duckdb.IOException as exc:
            attempt += 1
            wait_seconds = min(30 * attempt, 300)
            logger.warning(
                f"{ticker}: DB locked by another process (attempt {attempt}) — "
                f"waiting {wait_seconds}s: {exc}"
            )
            time.sleep(wait_seconds)


def run_window(from_date: str, to_date: str, checkpoint_path: Path) -> None:
    tickers = get_tickers()
    done = _load_done(checkpoint_path)
    remaining = [t for t in tickers if t not in done]
    logger.info(
        f"[{from_date}..{to_date}] {len(tickers)} tickers total, "
        f"{len(done)} already done, {len(remaining)} remaining"
    )

    client = FYERSBackfill(non_interactive=True)
    total_rows = 0

    i = 0
    while i < len(remaining):
        ticker = remaining[i]
        try:
            df = client.download_history(ticker, from_date, to_date)
        except RuntimeError as exc:
            if _looks_like_auth_error(exc):
                client = _wait_for_fresh_token()
                continue  # retry same ticker with the refreshed client
            logger.error(f"{ticker}: {exc} — skipping")
            _mark_done(checkpoint_path, ticker)
            i += 1
            continue
        except Exception as exc:
            if _looks_like_auth_error(exc):
                client = _wait_for_fresh_token()
                continue  # retry same ticker with the refreshed client
            logger.error(f"{ticker}: FYERS download failed: {exc} — skipping")
            _mark_done(checkpoint_path, ticker)
            i += 1
            continue

        rows_written = _write_with_lock_retry(ticker, df)
        total_rows += rows_written
        _mark_done(checkpoint_path, ticker)
        logger.info(f"{ticker}: {rows_written} rows overwritten ({i + 1}/{len(remaining)})")
        i += 1

    logger.info(
        f"[{from_date}..{to_date}] Done. {total_rows} rows overwritten across {len(tickers)} tickers."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Force-overwrite ohlcv_adjusted for a date window with raw FYERS OHLCV (adj_factor reset to 1.0)"
    )
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--checkpoint",
        dest="checkpoint",
        required=True,
        help="Path to this window's done-tickers checkpoint file",
    )
    args = parser.parse_args()
    run_window(args.from_date, args.to_date, Path(args.checkpoint))


if __name__ == "__main__":
    main()
