"""
scripts/backfill_delivery_from_bhavcopy.py

[2026-09-06] Backfills ohlcv_adjusted.delivery_qty/delivery_pct from NSE's
"sec_bhavdata_full" bhavcopy for a date range, reusing the exact same,
already-correct merge logic daily_pipeline.py's step_download_bhavcopy
uses live every day (its ON CONFLICT upsert always writes delivery_qty/
delivery_pct from the fresh bhavcopy row regardless of whether the
existing row's `source` is 'bhavcopy' or 'fyers' — see that function's
_UPSERT_OHLCV_WITH_DELIVERY docstring). This script does NOT touch price
columns' correctness beyond what that same upsert already does; it is a
thin, resumable, error-isolating loop over dates, not new merge logic.

Why this exists: found 2026-09-06 that delivery_qty/delivery_pct is 0%
populated for 2005-2023 and only 5-27%/year for 2024-2026 in the live
alphalens.duckdb, despite the ingestion code being correct — the gap is
purely "bhavcopy was never (re)run for these historical dates with
today's correct merge logic", not a code defect. A prior ad-hoc backfill
pass for the 2026-08-14..09-05 scheduler-pause gap (see project memory)
used this exact same step function directly; this script generalizes
that into a proper, reusable, resumable tool instead of a scratchpad.

Historical scope limit (verified live 2026-09-06): NSE's combined
"sec_bhavdata_full" format (the one carrying delivery_qty in the same
file as OHLCV) does not exist before ~2020 — 2017/2018/2019 all return
404 from archives.nseindia.com; 2020 onward returns real files. Running
this for dates before ~2020 will simply fail every date (logged, not
fatal) — that data is permanently unbackfillable from this source, not a
bug in this script. Very early 2020 dates may also fail
bhavcopy.download_bhavcopy's own validation gate (minimum 2000 EQ-series
stocks) if the exchange had fewer active stocks at that point; this is
the existing validator's behavior, unchanged here.

Usage
-----
    .venv/bin/python3 scripts/backfill_delivery_from_bhavcopy.py \\
        --from-date 2024-01-01 --to-date 2024-12-31

    # Dry run: report what would be attempted without writing
    .venv/bin/python3 scripts/backfill_delivery_from_bhavcopy.py \\
        --from-date 2024-01-01 --to-date 2024-01-31 --dry-run
"""

import argparse
import logging
import sys
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def _trading_days(from_date: date_type, to_date: date_type) -> list:
    """Weekdays only — NSE holidays are handled per-date by the existing
    bhavcopy fetch failing gracefully (logged, counted as skipped), same
    as the live daily pipeline already tolerates."""
    days = []
    d = from_date
    while d <= to_date:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--dry-run", action="store_true", help="List dates that would be attempted, do not write")
    args = parser.parse_args()

    from_date = date_type.fromisoformat(args.from_date)
    to_date = date_type.fromisoformat(args.to_date)
    if from_date > to_date:
        raise SystemExit("--from-date must be <= --to-date")

    days = _trading_days(from_date, to_date)
    logger.info(f"Backfilling delivery for {len(days)} weekday(s): {from_date} .. {to_date}")

    if args.dry_run:
        for d in days:
            print(d.isoformat())
        return

    from ingestion.scheduler.daily_pipeline import step_download_bhavcopy

    ok = 0
    failed = 0
    for d in days:
        try:
            step_download_bhavcopy(d)
            ok += 1
            logger.info(f"{d}: OK")
        except Exception as exc:
            failed += 1
            logger.warning(f"{d}: FAILED — {exc}")

    logger.info(f"Done: {ok} OK, {failed} failed/unavailable out of {len(days)}")


if __name__ == "__main__":
    main()
