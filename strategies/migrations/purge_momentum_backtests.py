"""
strategies/migrations/purge_momentum_backtests.py

Owner: Platform / Architecture
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.purge_momentum_backtests [--dry-run]

Deletes every MOMENTUM backtest record so the channel can be re-run from
scratch against the corrected seven-band partition (the 2026-08-19 RANK_BANDS
renumbering).

Scope, deliberately narrow
--------------------------
Momentum only. Technical and Fundamental results are untouched -- the user's
"historical data is not referred to" statement was about the momentum rank
bands, and nothing licenses deleting the other channels' work.

Tables cleared, in dependency order:

    strategy_signals          \\
    backtest_feature_log       |  child rows, keyed by run_id
    backtest_exit_decisions    |
    backtest_trades           /
    backtest_runs             <- the runs themselves (channel = 'momentum')
    strategy_catalog          <- the per-strategy rollup (channel = 'momentum')

NOT touched: `strategy_registry`. That holds strategy DEFINITIONS, not
results -- it is what the fresh backtests will be run against, and it was
just migrated onto the contiguous 1-7 band ids.

No backup
---------
Taken deliberately, on the user's explicit 2026-08-19 instruction ("can we
delete all the rows for Backtesting of Momentum Strategies without taking
backups?"). This is IRREVERSIBLE. Run --dry-run first.
"""

from __future__ import annotations

import argparse
import logging

import duckdb

from typing import Any, List, Optional

from config.settings import BACKTEST_DUCKDB_PATH

logger = logging.getLogger(__name__)

def _scalar(conn: duckdb.DuckDBPyConnection, sql: str, params: Optional[List[Any]] = None) -> Any:
    """The single value of a one-row query.

    `fetchone()` is typed Optional because a SELECT can return no rows -- but
    an aggregate always returns exactly one, so None here means the query was
    not the aggregate it looks like. Failing loudly beats indexing None.
    """
    row = conn.execute(sql, params or []).fetchone()
    if row is None:
        raise RuntimeError(f"expected one row, got none: {sql}")
    return row[0]


#: Child tables keyed by run_id, deleted before the runs they point at.
CHILD_TABLES = (
    "strategy_signals",
    "backtest_feature_log",
    "backtest_exit_decisions",
    "backtest_trades",
)

_MOMENTUM_RUN_IDS = (
    "SELECT run_id FROM backtest_runs WHERE channel = 'momentum'"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=args.dry_run)
    try:
        counts = {}
        for table in CHILD_TABLES:
            counts[table] = _scalar(conn, f"SELECT count(*) FROM {table} WHERE run_id IN ({_MOMENTUM_RUN_IDS})")
        counts["backtest_runs"] = _scalar(conn, "SELECT count(*) FROM backtest_runs WHERE channel = 'momentum'")
        counts["strategy_catalog"] = _scalar(conn, "SELECT count(*) FROM strategy_catalog WHERE channel = 'momentum'")

        for table, n in counts.items():
            logger.info("%-24s %d row(s)", table, n)

        if args.dry_run:
            logger.info("[dry-run] would delete %d row(s) total", sum(counts.values()))
            return

        for table in CHILD_TABLES:
            conn.execute(
                f"DELETE FROM {table} WHERE run_id IN ({_MOMENTUM_RUN_IDS})"
            )
        conn.execute("DELETE FROM backtest_runs WHERE channel = 'momentum'")
        conn.execute("DELETE FROM strategy_catalog WHERE channel = 'momentum'")

        remaining = _scalar(conn, "SELECT count(*) FROM backtest_runs WHERE channel = 'momentum'") + _scalar(conn, "SELECT count(*) FROM strategy_catalog WHERE channel = 'momentum'")
        logger.info("deleted=%d remaining_momentum=%d", sum(counts.values()), remaining)

        # Proof the narrow scope held.
        for channel in ("technical", "fundamental", "ml"):
            n = _scalar(conn, "SELECT count(*) FROM strategy_catalog WHERE channel = ?", [channel])
            logger.info("untouched %-12s catalog rows: %d", channel, n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
