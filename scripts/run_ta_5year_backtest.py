#!/usr/bin/env python3
"""
scripts/run_ta_5year_backtest.py

One entry point for the 5-year, 46-template Technical strategy comparison:
generate the queue -> run it (backtest/run_strategy_queue.py) -> collate the
comparison report (backtest/ta_comparison_report.py).

    python scripts/run_ta_5year_backtest.py                    # full run
    python scripts/run_ta_5year_backtest.py --dry-run          # queue only
    python scripts/run_ta_5year_backtest.py --report-only S    # re-collate suffix S

Resumable: pass the same --report-suffix after an interruption and
run_strategy_queue skips whichever jobs already reached 'completed'.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtest.run_strategy_queue import run_queue
from backtest.ta_comparison_report import write_reports
from scripts.generate_ta_backtest_queue import build_queue

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the 5-year TA strategy comparison end to end")
    parser.add_argument("--start-date", default="2021-04-01")
    parser.add_argument("--end-date", default="2026-03-31")
    parser.add_argument("--capital", type=float, default=1_000_000.0)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--categories", nargs="*", default=None)
    parser.add_argument("--queue-file", default="backtest/queues/ta_5year_comparison.json")
    parser.add_argument("--report-suffix", default=None, help="Reuse a prior suffix to resume that run")
    parser.add_argument(
        "--max-workers", type=int, default=2,
        help="Concurrent jobs. Every job sets defer_db_writes, so >1 is safe; keep it low to stay clear of OOM.",
    )
    parser.add_argument("--min-free-mb", type=float, default=4096.0)
    parser.add_argument("--dry-run", action="store_true", help="Generate the queue and stop")
    parser.add_argument("--report-only", default=None, metavar="SUFFIX", help="Skip running; just collate this suffix")
    args = parser.parse_args()

    if args.report_only:
        for kind, path in write_reports(args.report_only).items():
            print(f"{kind}: {path}")
        return

    queue_def = build_queue(
        start_date=args.start_date, end_date=args.end_date, initial_capital=args.capital,
        output_path=args.queue_file, include_categories=args.categories, top_n=args.top_n,
    )
    if args.dry_run:
        print(f"dry run — {len(queue_def['jobs'])} jobs written to {args.queue_file}, nothing executed")
        return

    suffix = args.report_suffix or datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"run_ta_5year_backtest: starting {len(queue_def['jobs'])} jobs, report_suffix={suffix}")
    summary = run_queue(
        jobs=queue_def["jobs"], min_free_mb=args.min_free_mb, report_suffix=suffix,
        # A single template failing must not abandon the other 45 — this is a
        # comparison sweep, not a gated pipeline; failures are surfaced in the
        # queue summary and again as `failed_reports` in the comparison JSON.
        stop_on_failure=False, resume=True, max_workers=args.max_workers,
    )
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2, default=str))

    paths = write_reports(suffix)
    for kind, path in paths.items():
        print(f"{kind}: {path}")
    sys.exit(0 if summary["all_passed"] else 1)


if __name__ == "__main__":
    main()
