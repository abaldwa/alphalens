"""
scripts/backfill_index_ohlcv.py

Phase: FutureDevelopment #25/#30 (sector rotation + real backtest benchmark)
Owner: Platform / Ingestion
Consumers: features/sector_rotation.py, backtest/run_phase1_backtest.py

One-off historical backfill of index_ohlcv (see
ingestion/scrapers/nse_indices.py) — NSE's ind_close_all archive has no
batch/range endpoint, only one CSV per trading date, so this walks the
project's own trading calendar (distinct ohlcv_adjusted dates, since that
table already reflects which days NSE actually traded) one date at a time.

Usage
-----
    # Default: full available ohlcv_adjusted date range
    .venv/bin/python3 scripts/backfill_index_ohlcv.py

    # Custom range
    .venv/bin/python3 scripts/backfill_index_ohlcv.py --from-date 2024-01-01
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

SLEEP_SECONDS = 1.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill index_ohlcv from NSE's indices-close archive")
    parser.add_argument("--from-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.scheduler.daily_pipeline import _UPSERT_INDEX_OHLCV
    from ingestion.scrapers import nse_indices

    to_dt = date.fromisoformat(args.to_date) if args.to_date else date.today()
    from_dt = date.fromisoformat(args.from_date) if args.from_date else None

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        params = [to_dt.isoformat()]
        where = "date <= ?"
        if from_dt is not None:
            where += " AND date >= ?"
            params.append(from_dt.isoformat())
        trading_days = [
            row[0] for row in conn.execute(
                f"SELECT DISTINCT date FROM ohlcv_adjusted WHERE {where} ORDER BY date", params
            ).fetchall()
        ]

    logger.info("Backfilling index_ohlcv for %d trading days (%s → %s)", len(trading_days), trading_days[0] if trading_days else None, trading_days[-1] if trading_days else None)

    total_rows = 0
    failures = 0
    for i, trading_day in enumerate(trading_days, start=1):
        date_str = trading_day.isoformat() if hasattr(trading_day, "isoformat") else str(trading_day)
        try:
            df = nse_indices.download_index_ohlcv(date_str)
        except ConnectionError as exc:
            failures += 1
            logger.warning("Skipping %s: %s", date_str, exc)
            time.sleep(SLEEP_SECONDS)
            continue

        rows = [
            (date_str, r.index_name, r.open, r.high, r.low, r.close,
             None if pd_isna(r.volume) else int(r.volume))
            for r in df.itertuples()
        ]
        if rows:
            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                conn.executemany(_UPSERT_INDEX_OHLCV, rows)
            total_rows += len(rows)

        if i % 50 == 0 or i == len(trading_days):
            logger.info("[%d/%d] %s: %d index rows (total so far: %d, failures: %d)", i, len(trading_days), date_str, len(rows), total_rows, failures)

        time.sleep(SLEEP_SECONDS)

    logger.info("Done. %d rows upserted across %d trading days, %d dates failed.", total_rows, len(trading_days), failures)


def pd_isna(value) -> bool:
    import pandas as pd
    return bool(pd.isna(value))


if __name__ == "__main__":
    main()
