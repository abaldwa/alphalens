"""
scripts/backfill_corporate_actions.py

Phase: 3 (Corporate Actions Backfill)
Specs: SPEC-PIPE-002
Owner: Platform / Ingestion
Consumers: ingestion/adjust/price_adjuster.py, features/corporate_action_features.py

Backfills the corporate_actions table from the NSE JSON API using quarterly
windows from a start date to today. One API call per 3-month window rather
than one call per day, keeping the request count manageable (~80 calls for
a 20-year backfill).

No credentials required — NSE corporate actions API is public.

Usage
-----
    # Default: 2006-01-01 → today
    .venv/bin/python3 scripts/backfill_corporate_actions.py

    # Custom range
    .venv/bin/python3 scripts/backfill_corporate_actions.py --from-date 2015-01-01

    # Dry-run: print counts without writing to DB
    .venv/bin/python3 scripts/backfill_corporate_actions.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

# Ensure project root is on sys.path when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

NSE_CA_URL = "https://www.nseindia.com/api/corporates-corporateActions"
NSE_HOMEPAGE_URL = "https://www.nseindia.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
WINDOW_MONTHS = 3   # API call granularity: one call per quarter
SLEEP_BETWEEN_CALLS = 2.0   # seconds


def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    s.get(NSE_HOMEPAGE_URL, timeout=15)
    return s


def _quarter_windows(from_dt: date, to_dt: date) -> List[tuple]:
    """Return (start, end) pairs covering [from_dt, to_dt] in quarterly chunks."""
    windows = []
    start = from_dt
    while start <= to_dt:
        # End of this quarter window
        m = start.month + WINDOW_MONTHS - 1
        year = start.year + (m - 1) // 12
        month = (m - 1) % 12 + 1
        # Last day of the end month
        if month == 12:
            end_of_window = date(year, 12, 31)
        else:
            end_of_window = date(year, month + 1, 1) - timedelta(days=1)
        end = min(end_of_window, to_dt)
        windows.append((start, end))
        start = end + timedelta(days=1)
    return windows


def _fetch_window(session: requests.Session, from_dt: date, to_dt: date) -> List[dict]:
    """Fetch all corporate actions from NSE for the given date window."""
    params = {
        "index": "equities",
        "from_date": from_dt.strftime("%d-%m-%Y"),
        "to_date": to_dt.strftime("%d-%m-%Y"),
    }
    for attempt in range(1, 4):
        try:
            r = session.get(NSE_CA_URL, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, list):
                return payload
            return payload.get("data", [])
        except Exception as exc:
            logger.warning("Attempt %d/3 failed for %s→%s: %s", attempt, from_dt, to_dt, exc)
            if attempt < 3:
                time.sleep(5)
            # Refresh session on failure
            try:
                session = _nse_session()
            except Exception:
                pass
    return []


def _parse_records(records: List[dict], universe: Optional[set]) -> pd.DataFrame:
    """Re-use the existing parser logic from ingestion/scrapers/corporate_actions.py."""
    # Use the private helpers directly
    from ingestion.scrapers.corporate_actions import (
        _parse_nse_date,
        _parse_purpose,
        _EQ_SERIES,
    )

    rows = []
    for rec in records:
        symbol = (rec.get("symbol") or rec.get("SYMBOL") or "").strip()
        series = (rec.get("series") or rec.get("SERIES") or "").strip().upper()
        ex_date_raw = rec.get("exDate") or rec.get("EX_DATE") or ""
        rec_date_raw = rec.get("recDate") or rec.get("REC_DATE") or ""
        purpose = (rec.get("purpose") or rec.get("PURPOSE") or rec.get("subject") or "").strip()

        if not symbol or series not in _EQ_SERIES:
            continue
        if universe and symbol not in universe:
            continue

        ex_date = _parse_nse_date(ex_date_raw)
        if not ex_date:
            continue

        record_date = _parse_nse_date(rec_date_raw)
        action_type, ratio = _parse_purpose(purpose, symbol)

        rows.append({
            "ticker": symbol,
            "ex_date": ex_date,
            "action_type": action_type,
            "ratio": ratio,
            "announcement_date": None,
            "record_date": record_date,
            "details": purpose or None,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["ticker", "ex_date", "action_type", "ratio",
                 "announcement_date", "record_date", "details"]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill corporate_actions from NSE API")
    parser.add_argument("--from-date", default="2006-01-01", metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--all-tickers", action="store_true",
                        help="Include tickers outside the current ~2492-ticker active universe")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse but do not write to DB")
    parser.add_argument("--publish-mode", choices=["direct", "staged"], default="direct",
                        help="'direct' (default): unchanged legacy per-window INSERT ON CONFLICT "
                             "DO NOTHING. 'staged' (A25): accumulate every window's rows across "
                             "the whole run and publish atomically once at the end.")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from config.universe import get_tickers
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.corporate_actions import (
        upsert_corporate_actions,
        upsert_corporate_actions_staged,
    )

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date) if args.to_date else date.today()
    universe = None if args.all_tickers else set(get_tickers())

    windows = _quarter_windows(from_dt, to_dt)
    logger.info(
        "Backfilling corporate actions %s → %s (%d quarterly windows, universe=%s)",
        from_dt, to_dt, len(windows),
        f"{len(universe)} tickers" if universe else "all",
    )

    session = _nse_session()
    total_upserted = 0
    staged_batches: List[pd.DataFrame] = []

    for i, (w_start, w_end) in enumerate(windows, start=1):
        records = _fetch_window(session, w_start, w_end)
        df = _parse_records(records, universe)

        if df.empty:
            logger.info("[%d/%d] %s → %s: 0 records", i, len(windows), w_start, w_end)
        else:
            logger.info(
                "[%d/%d] %s → %s: %d records (%s)",
                i, len(windows), w_start, w_end, len(df),
                df["action_type"].value_counts().to_dict(),
            )
            if not args.dry_run:
                if args.publish_mode == "staged":
                    staged_batches.append(df)
                else:
                    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                        n = upsert_corporate_actions(conn, df)
                    total_upserted += n

        time.sleep(SLEEP_BETWEEN_CALLS)
        # Refresh session every 20 windows to avoid cookie expiry
        if i % 20 == 0:
            try:
                session = _nse_session()
                logger.info("NSE session refreshed")
            except Exception as exc:
                logger.warning("Session refresh failed: %s", exc)

    if not args.dry_run and args.publish_mode == "staged" and staged_batches:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            total_upserted = upsert_corporate_actions_staged(conn, pd.concat(staged_batches, ignore_index=True))

    if args.dry_run:
        logger.info("DRY RUN — no writes. Would have processed %d windows.", len(windows))
    else:
        logger.info("Backfill complete. Total rows upserted: %d", total_upserted)

        # Show summary
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            summary = conn.execute(
                "SELECT action_type, COUNT(*) AS n FROM corporate_actions "
                "GROUP BY action_type ORDER BY n DESC"
            ).fetchall()
            logger.info("corporate_actions table summary:")
            for r in summary:
                logger.info("  %-12s  %d rows", r[0], r[1])


if __name__ == "__main__":
    main()
