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


def publish_fno_data(conn, drop_staging: bool = True) -> int:
    """
    A50 (2026-07-10): atomic file-swap publish for fno_data specifically —
    fno_data lives in its own DuckDB file (config.settings.FNO_DATA_DB_PATH,
    ATTACHed transparently by datastore/api/db.py's connection helper) so
    a publish can build the new version in a THROWAWAY file and swap it in
    via a near-instant `os.replace()`, instead of `publish_table`'s
    `CREATE OR REPLACE TABLE fno_data AS SELECT * FROM staging.fno_data` —
    which physically rewrites all ~121M rows in place, holding an
    exclusive lock on the file for however long that rewrite takes even
    when only one trade_date's ~50k rows actually changed.

    Must be called while holding publish_run_lock() and using the same
    connection that already has `staging.fno_data` populated (via
    datastore/staging/gate.py::stage_via_sql) and fno_db ATTACHed — same
    calling convention as publish_table.

    Returns the number of rows now in the production fno_data table.
    """
    import os
    import uuid
    from pathlib import Path

    # Read the ACTUAL attached fno_db file path from this connection, not a
    # hardcoded setting — datastore/api/db.py::_attach_fno_db derives this
    # per-connection from whatever main db_path is in use (fno_db_path_for),
    # which is NOT always config.settings.FNO_DATA_DB_PATH (e.g. every
    # isolated tmp_path test DB has its own companion file at a different
    # path). Using the hardcoded setting here was a real bug this session —
    # it silently swapped the wrong file while the connection's own attach
    # kept pointing at the correct one, making every read through THIS
    # connection look fine while a fresh connection saw stale/empty data.
    db_list = {row[1]: row[2] for row in conn.execute("PRAGMA database_list").fetchall()}
    FNO_DATA_DB_PATH = Path(db_list["fno_db"])

    tmp_path = FNO_DATA_DB_PATH.parent / f".fno_data.new.{uuid.uuid4().hex}.duckdb"
    conn.execute(f"ATTACH '{tmp_path}' AS fno_new")
    try:
        conn.execute("CREATE TABLE fno_new.fno_data AS SELECT * FROM staging.fno_data")
        row_count = conn.execute("SELECT COUNT(*) FROM fno_new.fno_data").fetchone()[0]
        # Without this, a fresh connection opened AFTER the os.replace() below
        # can see a stale/empty file — DuckDB doesn't guarantee the new
        # table's pages are flushed to disk until checkpointed or the
        # database is closed/detached; a plain DETACH was confirmed live
        # (this session) to NOT force that flush on its own.
        conn.execute("CHECKPOINT fno_new")
    finally:
        conn.execute("DETACH fno_new")

    # The old fno_db handle must be released before the swap, or the OS
    # rename would leave this connection's already-open file descriptor
    # pointing at the (now unlinked) old inode until it reconnects.
    conn.execute("DETACH fno_db")
    os.replace(tmp_path, FNO_DATA_DB_PATH)
    conn.execute(f"ATTACH IF NOT EXISTS '{FNO_DATA_DB_PATH}' AS fno_db")
    conn.execute("SET search_path = 'main,fno_db'")
    conn.execute("CHECKPOINT fno_db")

    logger.info("publish_fno_data: fno_data now has %d rows (atomic file swap)", row_count)
    if drop_staging:
        drop_staging_table(conn, "fno_data")
    return row_count


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
