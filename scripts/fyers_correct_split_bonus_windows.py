"""
scripts/fyers_correct_split_bonus_windows.py

Corrects ohlcv_adjusted around SPLIT/BONUS corporate actions from the
trailing 10 years by pulling Fyers' own historical OHLCV for a window
around each event's ex_date and writing it directly into ohlcv_adjusted,
instead of deriving a multiplicative adjustment factor ourselves.

Background: 2026-07-30 incident — a blind, full-universe run of
scripts/run_price_adjuster.py (deriving adj_factor from corporate_actions
ratios) was reverted after the user clarified that Fyers' own
split/bonus price adjustment is not a clean single-day step, so a
same-day raw-vs-Fyers comparison is not a reliable correctness test, and
that Fyers-sourced data should not additionally get our own multiplicative
adjustment applied on top.

Approach (per user instruction, 2026-07-30):
    1. corporate_actions rows with ex_date in the last 10 years and
       action_type in (SPLIT, BONUS) — one Fyers call per distinct
       (ticker, ex_date) event (not collapsed per-ticker), so tickers
       with multiple events in the window get each event corrected.
    2. anchor = ex_date + 10 days (clamped to today if that's in the
       future) — gives Fyers' own adjustment a few extra sessions to
       settle past the ex_date before we snapshot the trailing window.
    3. Pull Fyers history for [anchor - 365d, anchor] (one call; fits
       FYERS_HISTORY_MAX_DAYS_PER_CALL) and upsert those OHLCV rows
       directly into ohlcv_adjusted, adj_factor=1.0, vol_adj_factor=1.0.
    4. Record the corrected (ticker, ex_date, window) in
       fyers_ca_corrected so run_price_adjuster.py can exclude these
       tickers going forward — no multiplicative adjustment should ever
       be layered on top of directly-written Fyers data.

Rows older than the pulled window are explicitly out of scope for this
pass (2026-07-30 user decision) — only the window around each recent
event is corrected here.

Usage
-----
    .venv/bin/python3 scripts/fyers_correct_split_bonus_windows.py
    .venv/bin/python3 scripts/fyers_correct_split_bonus_windows.py --dry-run
    .venv/bin/python3 scripts/fyers_correct_split_bonus_windows.py --resume
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

LOG_PATH = Path("logs/fyers_correct_split_bonus_windows.log")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH)],
)
logger = logging.getLogger(__name__)

LOOKBACK_YEARS = 10
ANCHOR_OFFSET_DAYS = 10
WINDOW_DAYS = 365


def _ensure_tracking_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fyers_ca_corrected (
            ticker VARCHAR NOT NULL,
            ex_date DATE NOT NULL,
            action_type VARCHAR NOT NULL,
            window_start DATE NOT NULL,
            window_end DATE NOT NULL,
            rows_written INTEGER NOT NULL,
            corrected_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (ticker, ex_date)
        )
        """
    )


def _events_to_correct(conn, resume: bool) -> pd.DataFrame:
    cutoff = date.today() - timedelta(days=365 * LOOKBACK_YEARS)
    events = conn.execute(
        """
        SELECT DISTINCT ticker, ex_date, action_type
        FROM corporate_actions
        WHERE ex_date >= ? AND action_type IN ('SPLIT', 'BONUS')
        ORDER BY ticker, ex_date
        """,
        [cutoff],
    ).df()

    if resume:
        done = conn.execute("SELECT ticker, ex_date FROM fyers_ca_corrected").df()
        if not done.empty:
            done_keys = set(zip(done["ticker"], done["ex_date"]))
            events = events[
                ~events.apply(lambda r: (r["ticker"], r["ex_date"]) in done_keys, axis=1)
            ]
    return events.reset_index(drop=True)


def _upsert_ohlcv(conn, ticker: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df["adj_factor"] = 1.0
    df["vol_adj_factor"] = 1.0
    conn.register("_fy_correct_staging", df)
    try:
        conn.execute(
            """
            DELETE FROM ohlcv_adjusted
            WHERE ticker = ?
              AND date IN (SELECT date FROM _fy_correct_staging)
            """,
            [ticker],
        )
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted
                (date, ticker, open, high, low, close, volume,
                 delivery_qty, delivery_pct, adj_factor, vol_adj_factor)
            SELECT date, ticker, open, high, low, close, volume,
                   NULL, NULL, adj_factor, vol_adj_factor
            FROM _fy_correct_staging
            """
        )
    finally:
        conn.unregister("_fy_correct_staging")
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Correct SPLIT/BONUS windows directly from Fyers")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and log but do not write to DB")
    parser.add_argument("--resume", action="store_true", help="Skip (ticker, ex_date) pairs already in fyers_ca_corrected")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N events (for testing)")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.fyers_backfill import FYERSBackfill

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        _ensure_tracking_table(conn)
        events = _events_to_correct(conn, args.resume)

    if args.limit:
        events = events.iloc[: args.limit]

    logger.info("Events to correct: %d (dry_run=%s, resume=%s)", len(events), args.dry_run, args.resume)

    fb = FYERSBackfill(non_interactive=True)
    cached_token = fb._load_cached_token()
    if not cached_token or not fb._validate_token(cached_token):
        logger.error("No valid cached FYERS token — aborting (this job must never trigger interactive OAuth).")
        return

    today = date.today()
    ok = err = 0
    t_start = time.monotonic()

    for i, row in enumerate(events.itertuples(), start=1):
        ticker, ex_date, action_type = row.ticker, pd.Timestamp(row.ex_date).date(), row.action_type
        anchor = min(ex_date + timedelta(days=ANCHOR_OFFSET_DAYS), today)
        window_start = anchor - timedelta(days=WINDOW_DAYS)

        try:
            hist = fb.download_history(ticker, window_start.isoformat(), anchor.isoformat())
        except Exception as exc:  # noqa: BLE001
            logger.error("[%d/%d] %s ex_date=%s: FYERS fetch failed: %s", i, len(events), ticker, ex_date, exc)
            err += 1
            continue

        if hist is None or hist.empty:
            logger.warning("[%d/%d] %s ex_date=%s: no FYERS data for %s..%s", i, len(events), ticker, ex_date, window_start, anchor)
            err += 1
            continue

        n_written = 0
        if not args.dry_run:
            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                n_written = _upsert_ohlcv(conn, ticker, hist)
                conn.execute(
                    "DELETE FROM fyers_ca_corrected WHERE ticker = ? AND ex_date = ?",
                    [ticker, ex_date],
                )
                conn.execute(
                    """
                    INSERT INTO fyers_ca_corrected
                        (ticker, ex_date, action_type, window_start, window_end, rows_written)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [ticker, ex_date, action_type, window_start, anchor, len(hist)],
                )
        else:
            n_written = len(hist)

        ok += 1
        elapsed = time.monotonic() - t_start
        rate = i / elapsed if elapsed > 0 else 0
        eta_min = (len(events) - i) / rate / 60 if rate > 0 else 0
        logger.info(
            "[%d/%d] %s ex_date=%s (%s): %d rows from FYERS %s..%s  ok=%d err=%d  ETA ~%.0f min",
            i, len(events), ticker, ex_date, action_type, n_written, window_start, anchor, ok, err, eta_min,
        )

    logger.info("─" * 60)
    logger.info("Done. ok=%d err=%d total=%d", ok, err, len(events))


if __name__ == "__main__":
    main()
