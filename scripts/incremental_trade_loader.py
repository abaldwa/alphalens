#!/usr/bin/env python3
"""
scripts/incremental_trade_loader.py

Loads finished trade books into backtest_trades WHILE a sweep is still running,
so the table is never more than one interval behind (user request 2026-08-12).

Why not just run scripts/load_trade_books_to_db.py periodically
---------------------------------------------------------------
That script reprocesses EVERY matching trade book on every invocation. At ~800
files (the 390-job combined sweep) each pass would hold BACKTEST_DUCKDB_PATH's
single write lock for minutes, starving the 5 workers' tail writes — the exact
contention that made template C4 fail with "16 retries exhausted" on 2026-08-11.

This loads only run_ids not already present, and takes
backtest.batch_common.exclusive_backtest_lock so it QUEUES BEHIND job tails
rather than competing with them. A pass with nothing new to do costs one cheap
read and exits without ever taking the write lock.

A pass with nothing new to do still takes the lock briefly, then exits.

Durability note: trade books are already written to disk per job, so nothing is
ever lost on a crash — this only controls how current the DB table is.

Usage:
    python scripts/incremental_trade_loader.py                 # one pass
    python scripts/incremental_trade_loader.py --watch 900     # every 15 min until the queue exits
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from backtest.batch_common import exclusive_backtest_lock  # noqa: E402
from config.settings import (  # noqa: E402
    BACKTEST_DUCKDB_PATH,
    DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
    DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
    DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
)
from datastore.api.db import get_duckdb_connection  # noqa: E402
from scripts.load_trade_books_to_db import (  # noqa: E402
    _CREATE,
    _CREATE_VIEW,
    _INSERT,
    _RUN_ID_RE,
    REPORTS_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("incremental_trade_loader")


def _pending_paths(conn) -> list:
    """Trade books whose run_id has no rows in backtest_trades yet."""
    try:
        loaded = {r[0] for r in conn.execute("SELECT DISTINCT run_id FROM backtest_trades").fetchall()}
    except duckdb.Error:
        loaded = set()  # table not created yet

    # trade_log_*.csv, NOT trade_book_*.csv. The orchestrator writes BOTH per
    # run; scripts/load_trade_books_to_db.py's _RUN_ID_RE and its own glob both
    # key on trade_log_, and _INSERT expects that file's schema. Globbing
    # trade_book_ here matched nothing and loaded silently — caught 2026-08-12
    # before this ran against the sweep.
    out = []
    for path in sorted(REPORTS_DIR.glob("trade_log_*.csv")):
        m = _RUN_ID_RE.search(path.name)
        if m and m.group(1) not in loaded:
            out.append((m.group(1), path))
    return out


def one_pass() -> int:
    """Load every not-yet-loaded trade book. Returns rows added."""
    # exclusive_backtest_lock alone is NOT sufficient. Verified 2026-08-12: the
    # loader held the flock and still hit
    #   IOException: Could not set lock ... held in <python> (PID 725658)
    # because a worker's DuckDB connection outlives its flock release (cached
    # persist=True connections stay open after the tail completes). The flock
    # serialises the WORK; it does not guarantee the FILE is free. So take the
    # flock (to queue politely behind tails) AND use the project's retrying
    # connector, which is what makes the workers' own tail writes survive.
    with exclusive_backtest_lock(label="incremental_trade_loader"):
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn:
            pending = _pending_paths(conn)
            if not pending:
                logger.info("nothing new to load")
                return 0
            logger.info("%d new trade log(s) to load", len(pending))

            before = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0] if _table_exists(conn) else 0
            conn.execute(_CREATE)
            n_files = 0
            for run_id, path in pending:
                meta = conn.execute(
                    """SELECT strategy_id,
                              json_extract_string(config_json, '$.template_name'),
                              exit_policy_variant,
                              channel, created_at, start_date, end_date
                       FROM backtest_runs WHERE run_id = ?""",
                    [run_id],
                ).fetchone()
                if meta is None:
                    # The run row is written in the same tail as the trade book,
                    # but a job could be mid-tail right now. Leave it for the
                    # next pass rather than inserting rows with a NULL strategy.
                    logger.info("  %s: run row not written yet, deferring", run_id[:28])
                    continue
                conn.execute("DELETE FROM backtest_trades WHERE run_id = ?", [run_id])
                try:
                    conn.execute(_INSERT, [run_id, *meta, str(path)])
                    n_files += 1
                except duckdb.Error as exc:
                    logger.warning("  skipping %s — %s", path.name, exc)
            conn.execute(_CREATE_VIEW)
            after = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0]
            conn.execute("CHECKPOINT")
            logger.info("loaded %d file(s), trades %d -> %d", n_files, before, after)
            return after - before


def _table_exists(conn) -> bool:
    try:
        conn.execute("SELECT 1 FROM backtest_trades LIMIT 1")
        return True
    except duckdb.Error:
        return False


def _queue_running() -> bool:
    import subprocess

    r = subprocess.run(["pgrep", "-f", "run_strategy_queue.py"], capture_output=True)
    return r.returncode == 0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--watch", type=int, default=0,
                   help="seconds between passes; keeps going until the queue driver exits (0 = single pass)")
    args = p.parse_args()

    if not args.watch:
        one_pass()
        return

    logger.info("watching every %ds until run_strategy_queue.py exits", args.watch)
    while True:
        try:
            one_pass()
        except Exception as exc:  # noqa: BLE001
            # A loader failure must never take the sweep down with it.
            logger.error("pass failed: %s", exc, exc_info=True)
        if not _queue_running():
            logger.info("queue driver gone — final pass")
            try:
                one_pass()
            except Exception as exc:  # noqa: BLE001
                logger.error("final pass failed: %s", exc, exc_info=True)
            return
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
