"""
datastore/api/db.py

Phase: 0.1 (Project Skeleton); concurrency resilience added per SPEC-SCHED-013
Specs: SPEC-DS-007, SPEC-QUALITY-002, SPEC-SCHED-013
Owner: Platform / DataStore
Consumers: datastore/api, ingestion/*, systems/*, backtest

Database connection management and initialization.
Abstracts DuckDB (analytical queries) and SQLite (transactional/scheduling) setup.
SOLID: Dependency Injection — clients receive connections via context managers.

[AS BUILT, SPEC-SCHED-013] DuckDB allows multiple concurrent read-only
connections OR exactly one read-write connection — never both at once,
even across separate processes. The original "keep every connection open
forever in a pool" design (fine for a single long-lived process) meant
that once the DataStore API opened so much as one read-only connection to
a DuckDB file, that file stayed locked open for the *life of the API
process* — permanently blocking any other process (the ingestion
scheduler's write steps, a manual backfill run) from ever opening a
read-write connection to the same file while the API was up. Caught when
a real, multi-day-running scheduler process's 20:00 backfill-catchup job
crashed against this exact conflict and (separately) the scheduler then
stopped firing entirely — see BuildLog.md "Scheduler/DuckDB concurrency
resilience" for the full incident.

Fix: `persist=False` opens a connection, yields it, and closes it again
on exit — never cached in the pool — so the file is only held open for
the duration of the actual query/operation, not the life of the process.
Callers that only ever do brief, frequent reads against a file another
process also writes to (the API's OHLCV endpoints) should use
`persist=False`; callers that are the *sole* writer/reader of a file for
the life of their process (e.g. the API's own ml_signals access) can keep
`persist=True` (the default, unchanged) for connection-reuse efficiency.

`get_duckdb_connection` also now retries with backoff on a lock-conflict
IOException — even with `persist=False` reducing the window, a write
operation that is *actively* in progress when a read arrives will still
briefly hold the file exclusively; retrying instead of failing the
request outright turns that into a short delay rather than a hard error.
"""

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

try:
    import duckdb
except ImportError:
    duckdb = None

logger = logging.getLogger(__name__)

# SPEC-SCHED-013: retry budget for a transient DuckDB lock conflict (another
# process briefly holding the file open). Total worst-case wait ~3.5s
# (0.5+1.0+2.0) — short enough not to make an API request hang noticeably,
# long enough to ride out a quick write-step handoff.
DUCKDB_LOCK_RETRY_ATTEMPTS = 4
DUCKDB_LOCK_RETRY_BASE_DELAY_S = 0.5


# Global connection pools (simple implementation; upgrade to proper pooling if needed)
_duckdb_connections: dict = {}
_sqlite_connections: dict = {}
_sqlite_locks: dict = {}


def fno_db_path_for(main_db_path: str) -> Path:
    """
    A50 (2026-07-10): derive fno_data's companion file path from whatever
    main DB path is actually in use — NOT a single hardcoded production
    path, so every isolated test/tmp_path DB (test_fno_api.py etc.) gets
    its own correctly-scoped companion file instead of accidentally
    sharing (or missing) the real production one.
    """
    p = Path(main_db_path)
    return p.parent / f"{p.stem}_fno_data.duckdb"


def _attach_fno_db(conn, path_key: str, read_only: bool) -> None:
    """
    fno_data lives in its own file (see fno_db_path_for) so it can be
    published via an atomic file-swap instead of an in-place 121M-row
    rewrite (see datastore/staging/publish.py::publish_fno_data). ATTACH +
    search_path makes every existing unqualified `fno_data` reference
    (SELECT/INSERT/DELETE) resolve transparently against the attached
    file — live-verified this session (ATTACH ... AS fno_db; SET
    search_path = 'main,fno_db' makes bare `FROM fno_data`/
    `INSERT INTO fno_data`/`DELETE FROM fno_data` all work exactly as if
    the table were still in the main file). Only called for real (non-
    in-memory) connections — create_normalised.create_schema(in_memory=True)
    keeps fno_data inline for tests that don't care about this file split.
    """
    fno_path = fno_db_path_for(path_key)
    if read_only and not fno_path.exists():
        # This connection is to some OTHER DuckDB file entirely (e.g.
        # SIGNALS_DUCKDB_PATH) that never had a companion fno_data file
        # created — a real regression found this session: attaching
        # unconditionally for ANY real path broke every read-only
        # connection to an unrelated DB, since `ATTACH IF NOT EXISTS ...
        # (READ_ONLY)` cannot create a missing file. Skip silently; only
        # a real normalised-schema DB (created via
        # create_normalised.create_schema()) ever has this companion file.
        return
    fno_path.parent.mkdir(parents=True, exist_ok=True)
    mode = " (READ_ONLY)" if read_only else ""
    conn.execute(f"ATTACH IF NOT EXISTS '{fno_path}' AS fno_db{mode}")
    conn.execute("SET search_path = 'main,fno_db'")


def _connect_with_retry(path_key: str, read_only: bool):
    """SPEC-SCHED-013: retry-with-backoff on a transient DuckDB lock conflict."""
    last_exc = None
    for attempt in range(DUCKDB_LOCK_RETRY_ATTEMPTS):
        try:
            conn = duckdb.connect(path_key, read_only=read_only)
            if path_key != ":memory:":
                _attach_fno_db(conn, path_key, read_only)
            return conn
        except duckdb.IOException as exc:
            last_exc = exc
            if "Could not set lock" not in str(exc) or attempt == DUCKDB_LOCK_RETRY_ATTEMPTS - 1:
                raise
            delay = DUCKDB_LOCK_RETRY_BASE_DELAY_S * (2**attempt)
            logger.warning(
                f"DuckDB lock conflict on {path_key} (attempt {attempt + 1}/"
                f"{DUCKDB_LOCK_RETRY_ATTEMPTS}) — retrying in {delay:.1f}s: {exc}"
            )
            time.sleep(delay)
    raise last_exc  # pragma: no cover — unreachable, loop always returns or raises


@contextmanager
def get_duckdb_connection(
    db_path: Optional[Path] = None,
    read_only: bool = False,
    persist: bool = True,
) -> Iterator:
    """
    Context manager for DuckDB connections.

    Yields a DuckDB connection object.

    Args:
        db_path: Path to .duckdb file. If None, uses in-memory database (for testing).
        read_only: Open in read-only mode. DuckDB allows only one read-write
            connection to a given file at a time, but any number of
            concurrent read_only connections — pass True for any caller
            that never writes (e.g. the DataStore API's GET endpoints,
            features/macro_features.py's direct reads per SPEC-DS-002) so
            a long-lived process holding the file open doesn't lock out
            other readers (caught wiring features/matrix_builder.py, P1.1
            — see BuildLog.md).
        persist: If True (default), the connection is cached and kept open
            for the life of the process (efficient for a sole reader/writer
            of a file). If False, the connection is opened fresh and
            CLOSED again on exit — never cached — so the file's lock is
            released as soon as this `with` block ends. Use False for any
            caller sharing a file with another long-lived process (e.g.
            the API's OHLCV endpoints, which share DUCKDB_PATH with the
            ingestion scheduler — SPEC-SCHED-013; see module docstring).
            Ignored (always treated as True) when db_path is None — an
            in-memory `:memory:` database has no cross-process file lock to
            release in the first place, and separate `persist=False` calls
            would each get an independent, empty in-memory database instead
            of sharing state — breaking tests that seed an in-memory DB in
            one call and read it back in another.

    Yields:
        DuckDB connection object

    Raises:
        ImportError: If duckdb not installed
        IOError: If db_path is invalid
        duckdb.IOException: If the file is still lock-conflicted after
            DUCKDB_LOCK_RETRY_ATTEMPTS retries.

    Example:
        with get_duckdb_connection(db_path) as conn:
            result = conn.execute("SELECT * FROM ohlcv LIMIT 10").fetchall()
    """
    if duckdb is None:
        raise ImportError("duckdb is not installed")

    # Default to in-memory for testing
    path_key = str(db_path) if db_path else ":memory:"
    is_in_memory = path_key == ":memory:"

    if not persist and not is_in_memory:
        conn = _connect_with_retry(path_key, read_only)
        try:
            yield conn
        finally:
            conn.close()
        return

    cache_key = f"{path_key}|read_only={read_only}"
    if cache_key not in _duckdb_connections:
        _duckdb_connections[cache_key] = _connect_with_retry(path_key, read_only)

    conn = _duckdb_connections[cache_key]
    try:
        yield conn
    finally:
        # Keep connection open in pool; close only on explicit cleanup
        pass


@contextmanager
def get_sqlite_connection(
    db_path: Optional[Path] = None,
) -> Iterator[sqlite3.Connection]:
    """
    Context manager for SQLite connections.

    Yields a SQLite connection object. Handles cleanup automatically.

    Args:
        db_path: Path to .db file. If None, uses in-memory database (for testing).

    Yields:
        SQLite connection object

    Example:
        with get_sqlite_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pipeline_runs")
    """
    # Default to in-memory for testing
    path_key = str(db_path) if db_path else ":memory:"

    if path_key not in _sqlite_connections:
        # check_same_thread=False: the connection is cached process-wide
        # (APScheduler runs each job in its own worker thread), so the
        # default thread-affinity check would raise ProgrammingError on
        # every scheduled job. The lock below serializes actual access
        # since sqlite3.Connection isn't safe for concurrent use.
        _sqlite_connections[path_key] = sqlite3.connect(
            path_key, check_same_thread=False
        )
        _sqlite_locks[path_key] = threading.Lock()

    conn = _sqlite_connections[path_key]
    with _sqlite_locks[path_key]:
        try:
            yield conn
        finally:
            # Keep connection open in pool
            pass


def close_all_connections() -> None:
    """
    Close all pooled database connections.

    Call during application shutdown or test cleanup.
    """
    for conn in _duckdb_connections.values():
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")

    for conn in _sqlite_connections.values():
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing SQLite connection: {e}")

    _duckdb_connections.clear()
    _sqlite_connections.clear()

    logger.info("Closed all database connections")
