#!/usr/bin/env python3
"""
scripts/clean_backtest_artifacts.py

Reclaim disk from finished backtest runs. Dry run by default.

    python scripts/clean_backtest_artifacts.py                  # show what would go
    python scripts/clean_backtest_artifacts.py --apply          # actually delete
    python scripts/clean_backtest_artifacts.py --min-age-days 7 --apply
    python scripts/clean_backtest_artifacts.py --keep-cache --apply

An artifact referenced by any surviving run report is never deleted, whatever
the age filter says — see backtest/artifacts.py. Run reports and referenced
trade logs are the durable record of a sweep and are never candidates.

Safe to run while a sweep is in flight ONLY with --min-age-days set beyond the
current run's duration; otherwise a live run's cache can be pulled out from
under it. The default refuses to run when a queue driver is active.
"""

import argparse
import os
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtest.artifacts import apply, format_sweep, scan  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("clean_backtest_artifacts")


def _queue_running() -> bool:
    """True if a real backtest process is live.

    `pgrep -f <pattern>` matches any process whose full command line contains the
    pattern — including a shell that happens to mention it, which makes a bare
    pgrep both false-positive prone and self-matching. So: match python
    processes only, and drop this process and its parent explicitly.
    """
    own = {os.getpid(), os.getppid()}
    for pattern in ("run_strategy_queue.py", "run_orchestrator_backtest"):
        r = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
        for line in r.stdout.split():
            try:
                pid = int(line)
            except ValueError:
                continue
            if pid in own:
                continue
            try:
                cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode()
            except OSError:
                continue  # process exited between pgrep and the read
            if "python" in cmdline and pattern in cmdline:
                logger.info("clean_backtest_artifacts: live backtest process %d — %s", pid, cmdline[:110])
                return True
    return False


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    p.add_argument("--min-age-days", type=float, default=0.0,
                   help="only consider artifacts older than this")
    p.add_argument("--keep-cache", action="store_true", help="leave backtest/cache/ alone")
    p.add_argument("--keep-trade-books", action="store_true",
                   help="leave trade_book_*.csv alone (they have no reader, but keep them anyway)")
    p.add_argument("--force", action="store_true",
                   help="proceed even if a sweep appears to be running")
    args = p.parse_args()

    if _queue_running() and not args.force:
        logger.error(
            "a backtest queue or orchestrator is running — cleaning now can remove cache a live run "
            "depends on. Wait for it, or pass --min-age-days beyond its runtime plus --force."
        )
        raise SystemExit(2)

    sweep = scan(
        min_age_days=args.min_age_days,
        include_cache=not args.keep_cache,
        include_trade_books=not args.keep_trade_books,
    )

    print(f"{'APPLY' if args.apply else 'DRY RUN'} — artifacts older than {args.min_age_days:g} day(s)\n")
    print(format_sweep(sweep))

    protected = sweep.protected()
    if protected:
        print(f"\n  {len(protected)} artifact(s) protected by a surviving run report, e.g.:")
        for c in protected[:3]:
            print(f"      {c.path.name}  <- {c.protected_by}")

    if not args.apply:
        print("\nNothing deleted. Re-run with --apply to reclaim.")
        return

    reclaimed = apply(sweep, dry_run=False)
    print(f"\nReclaimed {reclaimed / 1048576:.1f} MB")


if __name__ == "__main__":
    main()
