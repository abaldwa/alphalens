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
from typing import Any, Dict, Iterator, Optional

from config.settings import DUCKDB_LOCK_RETRY_ATTEMPTS, DUCKDB_LOCK_RETRY_BASE_DELAY_S

try:
    import duckdb
except ImportError:
    duckdb = None

logger = logging.getLogger(__name__)

# SPEC-SCHED-013 / REV27: retry budget for a transient DuckDB lock conflict
# (another process briefly holding the file open). Now sourced from
# config.settings (env-overridable) — see that module's REV27 comment for
# the worst-case-wait math and the operational rule this retry does NOT
# replace (don't hold a long write open while API traffic is expected).


# Global connection pools (simple implementation; upgrade to proper pooling if needed)
_duckdb_connections: Dict[str, Any] = {}
# path_key -> whether the cached connection was opened read_only. Needed because
# the cache is keyed by path alone (see get_duckdb_connection): a cached
# read-only connection must be reopened before it can serve a write.
_duckdb_connection_modes: Dict[str, bool] = {}
_sqlite_connections: Dict[str, Any] = {}
_sqlite_locks: Dict[str, Any] = {}


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


def _attach_fno_db(conn: Any, path_key: str, read_only: bool) -> None:
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


def _connect_with_retry(
    path_key: str, read_only: bool,
    retry_attempts: Optional[int] = None, retry_base_delay_s: Optional[float] = None,
    retry_max_delay_s: Optional[float] = None,
) -> Any:
    """SPEC-SCHED-013: retry-with-backoff on a transient DuckDB lock conflict.

    retry_attempts/retry_base_delay_s: per-call override of the module
    defaults (DUCKDB_LOCK_RETRY_ATTEMPTS/DUCKDB_LOCK_RETRY_BASE_DELAY_S).
    2026-07-26: added so a caller with a longer-than-usual tolerance for
    waiting on a lock conflict (e.g. a backtest job's write connection,
    which has no outer timeout) can use a wider budget than the API's
    read-only endpoints, without widening the read-only path's budget too
    — the two were previously forced to share one global setting.

    retry_max_delay_s: 2026-07-26 follow-up — caps the exponential delay
    (None = uncapped, the original behavior). Against a contending process
    that briefly opens/closes read locks on a short, steady cycle (e.g. the
    API's ~6-7s frontend-polling cadence), uncapped exponential backoff
    quickly grows the per-attempt delay past that cycle length, so most
    attempts land inside a lock window purely by bad luck and most of the
    total budget is spent NOT retrying at all. Capping the delay keeps
    attempts frequent enough, for enough of the budget, to reliably land in
    a gap shorter than the cap.
    """
    attempts = retry_attempts if retry_attempts is not None else DUCKDB_LOCK_RETRY_ATTEMPTS
    base_delay = retry_base_delay_s if retry_base_delay_s is not None else DUCKDB_LOCK_RETRY_BASE_DELAY_S
    last_exc = None
    for attempt in range(attempts):
        try:
            conn = duckdb.connect(path_key, read_only=read_only)
            if path_key != ":memory:":
                _attach_fno_db(conn, path_key, read_only)
            return conn
        except duckdb.IOException as exc:
            last_exc = exc
            if "Could not set lock" not in str(exc) or attempt == attempts - 1:
                raise
            delay = base_delay * (2**attempt)
            if retry_max_delay_s is not None:
                delay = min(delay, retry_max_delay_s)
            logger.warning(
                f"DuckDB lock conflict on {path_key} (attempt {attempt + 1}/"
                f"{attempts}) — retrying in {delay:.1f}s: {exc}"
            )
            time.sleep(delay)
    assert last_exc is not None  # pragma: no cover
    raise last_exc  # pragma: no cover — unreachable, loop always returns or raises


@contextmanager
def get_duckdb_connection(
    db_path: Optional[Path] = None,
    read_only: bool = False,
    persist: bool = True,
    retry_attempts: Optional[int] = None,
    retry_base_delay_s: Optional[float] = None,
    retry_max_delay_s: Optional[float] = None,
) -> Iterator[Any]:
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
        retry_attempts: Optional override of DUCKDB_LOCK_RETRY_ATTEMPTS for
            this call only (see _connect_with_retry). None (default) uses
            the module-level setting.
        retry_base_delay_s: Optional override of DUCKDB_LOCK_RETRY_BASE_DELAY_S
            for this call only. None (default) uses the module-level setting.
        retry_max_delay_s: Optional per-attempt delay cap (see
            _connect_with_retry). None (default) means uncapped exponential
            backoff.

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
        # [2026-08-18] If this path already has a CACHED connection, serve that
        # one instead of opening a second. DuckDB allows only one configuration
        # per file per process, so opening a fresh read-only connection while a
        # persisted read-write one is still open raises outright -- which is
        # what happened whenever a writer used the default persist=True (e.g.
        # create_signal_tables_schema) and a reader then asked for
        # persist=False, read_only=True (e.g. the signals router).
        #
        # This does not weaken persist=False's purpose. Its point is to release
        # the file lock promptly; when a cached connection already holds that
        # lock for the life of the process, opening and closing a second one
        # would not have released anything anyway. So the cached connection is
        # yielded and, being owned by the cache, deliberately NOT closed here.
        pooled = _duckdb_connections.get(path_key)
        if pooled is not None:
            pooled_is_read_only = _duckdb_connection_modes.get(path_key) is True
            if read_only or not pooled_is_read_only:
                # A read request is satisfied by either mode, and a write
                # request by an already-read-write connection.
                yield pooled
                return
            # Write wanted, pooled connection is read-only: it cannot serve
            # this, and it blocks opening a read-write one. Retire it.
            try:
                pooled.close()
            except Exception:  # noqa: BLE001 - a close failure must not mask the reopen
                logger.warning(f"Could not close read-only connection to {path_key}")
            _duckdb_connections.pop(path_key, None)
            _duckdb_connection_modes.pop(path_key, None)
        conn = _connect_with_retry(path_key, read_only, retry_attempts, retry_base_delay_s, retry_max_delay_s)
        try:
            yield conn
        finally:
            conn.close()
        return

    # [2026-08-18] The cache is keyed by PATH ALONE, deliberately.
    #
    # It used to be keyed by f"{path_key}|read_only={read_only}", which made a
    # read-only and a read-write request to the same file two distinct cache
    # entries -- so the second one opened a SECOND connection to that file with
    # a different configuration, which DuckDB refuses outright:
    #
    #   Can't open a connection to same database file with a different
    #   configuration than existing connections
    #
    # The key was more specific than the resource it guards. Any process that
    # created a DB read-write and then read it back read-only through this
    # helper hit it -- including create_signal_tables_schema() followed by the
    # signals router, which is why tests/integration/test_daily_pipeline.py's
    # test_signals_written_are_readable_via_api returned 500.
    #
    # One connection per path, then. A read-write connection serves a read-only
    # request perfectly well, so that case reuses it. The reverse is NOT safe:
    # serving a write through a cached read-only connection would fail at the
    # first write, far from the cause -- so that case closes the read-only
    # connection and reopens read-write.
    cache_key = path_key
    cached = _duckdb_connections.get(cache_key)
    if cached is not None and not read_only and _duckdb_connection_modes.get(cache_key) is True:
        logger.debug(
            f"Upgrading cached read-only connection to {path_key} to read-write"
        )
        try:
            cached.close()
        except Exception:  # noqa: BLE001 - a close failure must not mask the reopen
            logger.warning(f"Could not close read-only connection to {path_key}")
        cached = None
        _duckdb_connections.pop(cache_key, None)
        _duckdb_connection_modes.pop(cache_key, None)

    if cached is None:
        _duckdb_connections[cache_key] = _connect_with_retry(
            path_key, read_only, retry_attempts, retry_base_delay_s, retry_max_delay_s,
        )
        _duckdb_connection_modes[cache_key] = read_only

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
    _duckdb_connection_modes.clear()
    _sqlite_connections.clear()

    logger.info("Closed all database connections")
