#!/usr/bin/env python3
"""
scripts/purge_stale_screener_cache.py

Purges technical_screener_cache when the feature store underneath it has been
recomputed.

WHY THIS IS NEEDED (2026-08-11)
-------------------------------
backtest/core/screener_cache.py caches ScreenerEngine.screen() output keyed by
(template_name, as_of_date, ticker). That key is correct given its stated
assumption -- screen() is a pure function of template + date -- but it is a pure
function of template + date *and the feature values for that date*. The key
carries no feature-version component, so there is NO invalidation path when the
feature store is recomputed: stale rows are silently reused forever.

That happened here. The fresh 2007-04-01..2026-08-10 feature compute finished on
2026-08-10, but all 1,927,632 cached rows had as_of_date <= 2026-07-28, i.e.
every one was scored against the superseded feature store.

Not a live-run problem for the current queue: the --defer-db-writes path in
run_orchestrator_backtest._run_deferred skips screener_cache entirely. The
exposure is the app/UI Technical screener path, which does read it.

The table is a pure performance cache -- dropping rows costs recompute time and
nothing else.

Usage:
    python scripts/purge_stale_screener_cache.py              # dry run
    python scripts/purge_stale_screener_cache.py --apply
    python scripts/purge_stale_screener_cache.py --apply --before 2026-08-10
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import BACKTEST_DUCKDB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLE = "technical_screener_cache"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", default=str(BACKTEST_DUCKDB_PATH))
    p.add_argument(
        "--before",
        default=None,
        help="Only purge rows with as_of_date < this (YYYY-MM-DD). Default: purge all.",
    )
    p.add_argument("--apply", action="store_true", help="Actually delete (default is a dry run)")
    args = p.parse_args()

    where = "" if args.before is None else f" WHERE as_of_date < DATE '{args.before}'"

    conn = duckdb.connect(args.db_path, read_only=not args.apply)
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        target = conn.execute(f"SELECT COUNT(*) FROM {TABLE}{where}").fetchone()[0]
        logger.info("%s: %d rows total, %d match the purge filter", TABLE, total, target)

        if not args.apply:
            logger.info("dry run - nothing deleted. Re-run with --apply to purge.")
            return

        conn.execute(f"DELETE FROM {TABLE}{where}")
        conn.execute("CHECKPOINT")
        remaining = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        logger.info("purged %d rows; %d remain", target, remaining)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
