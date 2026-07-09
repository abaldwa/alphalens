"""
datastore/staging/publish.py

Phase: A25 (Write-Audit-Publish Architecture)
Specs: SPEC-SCHED-013 (single-writer-per-file DuckDB discipline)
Owner: Platform / DataStore
Consumers: scripts/insert_fno_files.py, ingestion/backfill_runner.py (pilot),
    scripts/restore_snapshot.py

Why this exists
----------------
Promotes a batch already validated into `staging.<table>` (see
datastore/staging/gate.py) into the real production table, atomically.

Commit 8147579 ("Fix check_ta_alerts cross-process DuckDB lock race")
established that DuckDB is single-writer-per-file at the OS level, and
that a design where two OS processes can each open their own writable
connection to the same file is unsafe — the fix there was to route every
write through one already-established writer, not to add more locking
around concurrent connections. publish_table follows the same rule: the
caller must pass an existing writable connection (its own, single, sole
writer for the operation), never a second connection opened independently
by this module. publish_run_lock() below is a cross-process advisory lock
(same fcntl.flock pattern as ingestion/scheduler/pipeline_scheduler.py's
pipeline_run_lock) guarding the window between staging and publish, so two
separate invocations of the whole stage->publish sequence (e.g. a manual
backfill running concurrently with the daily pipeline) can't interleave.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
from typing import Iterator

from config.settings import PUBLISH_RUN_LOCK_PATH
from datastore.staging.gate import drop_staging_table

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def publish_run_lock() -> Iterator[bool]:
    """
    Cross-process, non-blocking advisory lock guarding the staging->publish
    sequence. Yields True if the lock was acquired, False if another
    process already holds it (caller should skip/retry, not proceed).
    """
    try:
        PUBLISH_RUN_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_file = open(PUBLISH_RUN_LOCK_PATH, "w")
    except OSError as exc:
        logger.warning(f"publish_run_lock: could not open lock file ({exc}) — proceeding without it")
        yield True
        return

    acquired = False
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        yield True
    except BlockingIOError:
        yield False
    finally:
        if acquired:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def publish_table(conn, table_name: str, drop_staging: bool = True) -> int:
    """
    Atomically promote staging.<table_name> to the production table
    `table_name`, via a single CREATE OR REPLACE TABLE ... AS SELECT
    statement (single atomic DuckDB operation — no partial-update window,
    unlike the previous per-date DELETE+INSERT pattern in
    scripts/insert_fno_files.py / ingestion/backfill_runner.py).

    Must be called while holding publish_run_lock() and using the single
    writable connection already established for `table_name`'s DuckDB file
    — never a second, independently-opened connection (see module
    docstring).

    Returns the number of rows now in the production table. Raises
    duckdb.CatalogException if staging.<table_name> doesn't exist (caller
    must have staged something first via gate.stage_dataframe).
    """
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM staging.{table_name}")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("publish_table: %s now has %d rows", table_name, row_count)
    if drop_staging:
        drop_staging_table(conn, table_name)
    return row_count
