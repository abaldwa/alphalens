"""
scripts/record_backfill_checkpoints.py

[2026-09-10] When persist_cached_pipeline.sh publishes real data for a
gap date via an ad-hoc backfill script (fyers_multiday_backfill.py,
backfill_index_ohlcv.py, etc.) instead of daily_pipeline.py's own
step_* functions, pipeline_checkpoints never learns that step happened —
so a later scripts/catchup_pipeline_for_dates.py --from-prerequisites run
has no way to tell "this data already exists" from "this was never
fetched," and force_run_date_sync correctly (by its own contract) refuses
to skip ahead, redundantly re-pulling the full universe from FYERS for
every gap date. Confirmed live 2026-09-10: a 3-date catch-up spent 10+
minutes stuck re-pulling ~2317 tickers for a single date whose FYERS data
we'd already published minutes earlier.

This script closes that gap the honest way — not by faking a checkpoint
for work that didn't happen, but by recording one for work that
genuinely did, just through a different code path. Only mark a step here
once its equivalent persist has actually succeeded and been verified;
this script does no verification of its own.

Usage
-----
    # After persist_cached_pipeline.sh successfully published FYERS
    # OHLCV, bhavcopy delivery, index OHLCV, and corporate actions for
    # 2026-09-07..2026-09-09:
    .venv/bin/python3 scripts/record_backfill_checkpoints.py \\
        --from-date 2026-09-07 --to-date 2026-09-09 \\
        --steps download_bhavcopy download_fyers_daily fyers_health_check \\
                download_index_ohlcv download_corporate_actions adjust_prices
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def _trading_days(from_dt: date, to_dt: date) -> "list[date]":
    from ingestion.scheduler.gap_detector import is_trading_day

    days = []
    d = from_dt
    while d <= to_dt:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def main() -> None:
    from ingestion.scheduler.checkpoint import STEP_NAMES, CheckpointManager

    parser = argparse.ArgumentParser(
        description="Record pipeline_checkpoints rows for steps whose data was persisted via ad-hoc backfill scripts"
    )
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument(
        "--steps", nargs="+", required=True, choices=STEP_NAMES,
        help="STEPS entries whose underlying data is already correctly persisted for this range",
    )
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date)
    dates = _trading_days(from_dt, to_dt)

    if not dates:
        logger.info(f"No trading days in {from_dt}..{to_dt} — nothing to record")
        return

    cm = CheckpointManager()
    for d in dates:
        for step in args.steps:
            cm.save_checkpoint(d, step, "success", is_backfill=True)
        logger.info(f"{d.isoformat()}: recorded {args.steps}")

    logger.info(f"Done: {len(dates)} date(s) x {len(args.steps)} step(s) recorded")


if __name__ == "__main__":
    main()
