"""
scripts/catchup_pipeline_for_dates.py

[2026-09-10] Runs the DB-lock-dependent tail of the daily pipeline
(compute_features -> compute_momentum + run_models -> write_signals ->
sanity_check, per ingestion.scheduler.checkpoint.STEPS' depends_on graph)
forward for a list of gap dates, via the same force_run_date_sync() the
Ops "force-run" API endpoint and the A21 catch-up action use.

This is the companion to scripts/persist_cached_pipeline.sh: once the
fetch-only caches (FYERS OHLCV, NSE XBRL, corporate actions, etc.) are
actually published to DuckDB, the raw data existing isn't the same as
being "current" — features and momentum signals for those newly-published
dates don't exist until this runs too. Intended to be chained immediately
after persist_cached_pipeline.sh, while the write lock this process just
used is still ours to keep using.

[2026-09-10, user decision] ML model inference (STEPS entry "run_models"
and its dependents write_signals/sanity_check) is deliberately NOT run by
default — the user is about to revisit the ML strategy approach, so
backfilling model outputs on the old approach for every gap date is
wasted work until that's settled. Default stops after compute_momentum
(the separate, non-ML R-family momentum signal system). Pass --include-ml
to run the full cascade through run_models when that's actually wanted
again.

Each date is run in its OWN subprocess (not looped in-process) — a known,
observed bug (see project memory: "compute_features worker-lock bug")
has panel_workers' multiprocessing pool sometimes leak a held DuckDB lock
across dates if reused in-process; per-date subprocess isolation is the
established workaround, not a style preference.

Usage
-----
    # Default: compute_features + compute_momentum only, no ML
    .venv/bin/python3 scripts/catchup_pipeline_for_dates.py \\
        --from-date 2026-09-07 --to-date 2026-09-09

    # Only compute_features, skip momentum too
    .venv/bin/python3 scripts/catchup_pipeline_for_dates.py \\
        --from-date 2026-09-07 --to-date 2026-09-09 --features-only

    # Full cascade including ML model inference
    .venv/bin/python3 scripts/catchup_pipeline_for_dates.py \\
        --from-date 2026-09-07 --to-date 2026-09-09 --include-ml
"""

import argparse
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

_RUN_ONE_DATE_SNIPPET = """
import sys
from datetime import date
from ingestion.scheduler.force_run import force_run_date_sync

run_date = date.fromisoformat(sys.argv[1])
step_name = sys.argv[2]
cascade = sys.argv[3] == "1"

results = force_run_date_sync(step_name, [run_date], date.today(), cascade=cascade)
failed = [r for r in results if r.status == "failed"]
for r in results:
    print(f"{r.date} {r.step_name}: {r.status}" + (f" ({r.error_message})" if r.error_message else ""))
sys.exit(1 if failed else 0)
"""


def _trading_days(from_dt: date, to_dt: date) -> "list[date]":
    from ingestion.scheduler.gap_detector import is_trading_day

    days = []
    d = from_dt
    while d <= to_dt:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def _run_step(run_date: date, step_name: str, cascade: bool) -> bool:
    """Runs force_run_date_sync for exactly one (date, step) in its own subprocess."""
    proc = subprocess.run(
        [sys.executable, "-c", _RUN_ONE_DATE_SNIPPET, run_date.isoformat(), step_name, "1" if cascade else "0"],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True,
        text=True,
    )
    for line in proc.stdout.splitlines():
        logger.info(f"  {line}")
    if proc.returncode != 0:
        for line in proc.stderr.splitlines()[-20:]:
            logger.warning(f"  [stderr] {line}")
    return proc.returncode == 0


def run_one_date(run_date: date, steps: "list[str]") -> bool:
    """Runs each step in `steps` (in order, non-cascading) for one date, in
    separate subprocesses. Stops at the first failed step for this date."""
    for step_name in steps:
        if not _run_step(run_date, step_name, cascade=False):
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Catch up compute_features/momentum signals for gap dates")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument(
        "--features-only", action="store_true",
        help="Run only compute_features, skip compute_momentum too.",
    )
    parser.add_argument(
        "--include-ml", action="store_true",
        help="Also run run_models/write_signals/sanity_check (ML model inference). "
             "Off by default — see module docstring.",
    )
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date)
    dates = _trading_days(from_dt, to_dt)

    if not dates:
        logger.info(f"No trading days in {from_dt}..{to_dt} — nothing to catch up")
        return

    if args.include_ml:
        steps = ["compute_features"]  # cascades the rest via force_run_date_sync itself
        cascade_full = True
    elif args.features_only:
        steps = ["compute_features"]
        cascade_full = False
    else:
        steps = ["compute_features", "compute_momentum"]
        cascade_full = False

    logger.info(
        f"Catching up {len(dates)} trading day(s): {[d.isoformat() for d in dates]} "
        f"-- steps: {steps}{' (full ML cascade)' if cascade_full else ''}"
    )

    succeeded = 0
    failed_dates = []
    for d in dates:
        logger.info(f"=== {d.isoformat()} ===")
        ok = _run_step(d, steps[0], cascade=True) if cascade_full else run_one_date(d, steps)
        if ok:
            succeeded += 1
            logger.info(f"✓ {d.isoformat()} complete")
        else:
            failed_dates.append(d.isoformat())
            logger.warning(f"✗ {d.isoformat()} failed — continuing to next date")

    logger.info(f"Catch-up complete: {succeeded}/{len(dates)} dates succeeded")
    if failed_dates:
        logger.warning(f"Failed dates (re-run individually to retry): {failed_dates}")
        sys.exit(1)


if __name__ == "__main__":
    main()
