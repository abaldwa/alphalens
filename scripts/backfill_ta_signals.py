"""
scripts/backfill_ta_signals.py

Backfills the TA screener's `ta_signals` table across historical trading
dates by re-running DailyAlertChecker (all 42 templates) against each
date's ALREADY-COMPUTED feature Parquet (config.settings.FEATURES_DAILY_DIR
— confirmed to span 2007-01-03 to today, ~4,800 files). No feature
recompute is needed: the screener already accepts an arbitrary historical
`date` (ScreenerEngine.screen(..., date=...)), this script just drives it
across a date range instead of "today" only.

This exists because backtest/strategy_confidence.py's confidence tiers
need >=60 independent trading dates (and ideally multiple market regimes)
per template to say anything statistically meaningful — the live daily
pipeline alone only accumulates ~1 new date per real trading day, so a
historical backfill is the only practical way to get there in a single
session rather than waiting months.

Idempotent (ta_signals upserts ON CONFLICT), and skips dates already
present in ta_signals by default, so it's safe to interrupt and re-run.

Usage:
    python -m scripts.backfill_ta_signals --years 5
    python -m scripts.backfill_ta_signals --start-date 2021-07-19 --end-date 2026-07-17
    python -m scripts.backfill_ta_signals --years 5 --force   # re-evaluate even already-covered dates
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, timedelta

from config.settings import FEATURES_DAILY_DIR, SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from systems.technical_analysis.alerts.daily_alert_checker import DailyAlertChecker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _available_feature_dates(start: str, end: str) -> list:
    """Every date with a real feature Parquet file in [start, end]."""
    dates = []
    for p in FEATURES_DAILY_DIR.glob("*.parquet"):
        d = p.stem
        if start <= d <= end:
            dates.append(d)
    return sorted(dates)


def _existing_ta_signals_dates() -> set:
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
        ).fetchall()]
        if not tables:
            return set()
        return {str(r[0]) for r in conn.execute("SELECT DISTINCT date FROM ta_signals").fetchall()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=float, default=None, help="Backfill this many years back from today")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD (overrides --years)")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, default today")
    parser.add_argument("--force", action="store_true", help="Re-evaluate dates already present in ta_signals")
    parser.add_argument("--progress-every", type=int, default=50)
    args = parser.parse_args()

    end_date = args.end_date or date.today().isoformat()
    if args.start_date:
        start_date = args.start_date
    elif args.years:
        start_date = (date.today() - timedelta(days=int(args.years * 365.25))).isoformat()
    else:
        parser.error("Must pass --years or --start-date")

    all_dates = _available_feature_dates(start_date, end_date)
    if not all_dates:
        logger.warning("No feature Parquet files found in [%s, %s]", start_date, end_date)
        return

    existing = set() if args.force else _existing_ta_signals_dates()
    todo = [d for d in all_dates if d not in existing]
    logger.info(
        "backfill_ta_signals: %d feature dates in range, %d already in ta_signals, %d to evaluate",
        len(all_dates), len(all_dates) - len(todo), len(todo),
    )

    checker = DailyAlertChecker()
    start_time = time.monotonic()
    total_matches = 0
    for i, d in enumerate(todo, 1):
        counts = checker.run(d)
        total_matches += sum(counts.values())
        if i % args.progress_every == 0 or i == len(todo):
            elapsed = time.monotonic() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta_sec = (len(todo) - i) / rate if rate > 0 else 0
            logger.info(
                "  [%d/%d] date=%s matches_so_far=%d elapsed=%.0fs eta=%.0fs",
                i, len(todo), d, total_matches, elapsed, eta_sec,
            )

    logger.info(
        "backfill_ta_signals: done. %d dates evaluated, %d total full-match rows written, %.0fs elapsed",
        len(todo), total_matches, time.monotonic() - start_time,
    )


if __name__ == "__main__":
    main()
