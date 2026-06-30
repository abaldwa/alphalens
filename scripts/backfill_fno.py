"""
scripts/backfill_fno.py

Phase: 3 (F&O Historical Backfill)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: features/fno_features.py

Backfills the fno_data table using NSE's UDiFF bhavcopy archive.
The current URL format (BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip)
was introduced by NSE around 2024. Dates before that will get a 404 and
are skipped — we only persist what's actually available.

No credentials required.

Usage
-----
    # Default: 2024-01-01 → today (where UDiFF format is confirmed available)
    .venv/bin/python3 scripts/backfill_fno.py

    # Try further back (will gracefully skip 404 dates)
    .venv/bin/python3 scripts/backfill_fno.py --from-date 2020-01-01
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill fno_data from NSE UDiFF archives")
    parser.add_argument("--from-date", default="2024-01-01", metavar="YYYY-MM-DD",
                        help="Start date (UDiFF format confirmed from ~2024; earlier dates attempted but may 404)")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from config.universe import get_tickers
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.fno import download_fno_bhavcopy

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date) if args.to_date else date.today()
    universe = set(get_tickers())

    # Get all trading dates in range from OHLCV (only dates with actual market data)
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT CAST(date AS VARCHAR) FROM ohlcv_adjusted "
            "WHERE date >= ? AND date <= ? ORDER BY date",
            [from_dt.isoformat(), to_dt.isoformat()],
        ).fetchall()
        existing_dates = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM fno_data"
            ).fetchall()
        }

    all_dates = [date.fromisoformat(r[0]) for r in rows]
    pending = [d for d in all_dates if d.isoformat() not in existing_dates]
    logger.info(
        "F&O backfill %s → %s: %d trading dates, %d already in DB, %d to fetch",
        from_dt, to_dt, len(all_dates), len(existing_dates), len(pending),
    )

    ok = skipped = err = 0

    for i, d in enumerate(pending, start=1):
        date_str = d.isoformat()
        try:
            df = download_fno_bhavcopy(date_str)
            if df.empty:
                logger.info("[%d/%d] %s: empty (non-trading day or 404)", i, len(pending), date_str)
                skipped += 1
                continue

            # Filter to universe tickers only to keep DB lean
            df_filtered = df[df["ticker"].isin(universe)]

            rows_to_insert = [
                (
                    date_str, row["ticker"], row["instrument"], str(row["expiry"]),
                    row.get("strike"), row.get("option_type"),
                    row.get("oi"), row.get("oi_change"), row.get("volume"),
                    row.get("settle_price"), row.get("close_price"), row.get("underlying_price"),
                )
                for _, row in df_filtered.iterrows()
            ]

            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                conn.execute("DELETE FROM fno_data WHERE trade_date = ?", [date_str])
                conn.executemany(
                    """INSERT INTO fno_data
                       (trade_date, ticker, instrument, expiry, strike, option_type,
                        oi, oi_change, volume, settle_price, close_price, underlying_price)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    rows_to_insert,
                )
            logger.info("[%d/%d] %s: %d rows (%d universe)", i, len(pending), date_str,
                        len(df), len(df_filtered))
            ok += 1
        except Exception as exc:
            msg = str(exc)
            if "404" in msg or "Not Found" in msg:
                logger.debug("[%d/%d] %s: 404 (date outside UDiFF range)", i, len(pending), date_str)
                skipped += 1
            else:
                logger.warning("[%d/%d] %s FAILED: %s", i, len(pending), date_str, exc)
                err += 1
        time.sleep(0.5)

    logger.info("F&O backfill complete: %d ok, %d skipped/404, %d errors", ok, skipped, err)

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        n = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM fno_data").fetchone()[0]
        logger.info("fno_data now covers %d trading dates", n)


if __name__ == "__main__":
    main()
