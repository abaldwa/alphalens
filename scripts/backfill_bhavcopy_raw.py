"""
scripts/backfill_bhavcopy_raw.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / Ingestion

Backfills the raw bhavcopy landing zone (datastore/raw/bhavcopy/) so full
daily OHLCV history is retained raw, not just the last ~17 days
(FeatureBacklog.md A25's confirmed gap — the raw-landing mechanism itself,
ingestion/scrapers/bhavcopy.py::_save_raw(), already writes one CSV per
date; it has simply never been backfilled historically). Resumable and
idempotent — skips any date whose CSV already exists, matching the
resumable style of scripts/backfill_fno.py.

Usage:
    python scripts/backfill_bhavcopy_raw.py --from-date 2021-01-01 --to-date 2026-07-08
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

from config.settings import RAW_DIR  # noqa: E402
from ingestion.scheduler.gap_detector import is_trading_day  # noqa: E402
from ingestion.scrapers.bhavcopy import download_bhavcopy  # noqa: E402

_RAW_BHAVCOPY_DIR = RAW_DIR / "bhavcopy"


def _pending_dates(from_dt: date, to_dt: date) -> list:
    all_dates = []
    d = from_dt
    while d <= to_dt:
        if is_trading_day(d):
            all_dates.append(d)
        d += timedelta(days=1)
    existing = {p.stem for p in _RAW_BHAVCOPY_DIR.glob("*.csv")} if _RAW_BHAVCOPY_DIR.exists() else set()
    return [d for d in all_dates if d.isoformat() not in existing]


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill raw bhavcopy landing zone (A25)")
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date) if args.to_date else date.today()

    pending = _pending_dates(from_dt, to_dt)
    logger.info(
        "Bhavcopy raw backfill %s → %s: %d trading dates pending (raw CSVs already on disk are skipped)",
        from_dt, to_dt, len(pending),
    )

    ok = err = 0
    for i, d in enumerate(pending, start=1):
        date_str = d.isoformat()
        try:
            df = download_bhavcopy(date_str)
            logger.info("[%d/%d] %s: %d rows landed raw", i, len(pending), date_str, len(df))
            ok += 1
        except Exception as exc:
            logger.warning("[%d/%d] %s FAILED: %s", i, len(pending), date_str, exc)
            err += 1
        time.sleep(0.5)

    logger.info("Done: %d ok, %d failed, %d already present", ok, err, len(pending) - ok - err)


if __name__ == "__main__":
    main()
