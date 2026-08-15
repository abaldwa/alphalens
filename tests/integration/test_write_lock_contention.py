"""
tests/integration/test_write_lock_contention.py

Demonstrates that the `get_duckdb_connection(persist=False, read_only=False)`
retry path succeeds when a concurrent *process* holds the DuckDB write lock.

This is the exact pattern `defer_db_writes` depends on: a short-lived
write connection that retries with backoff against a contended lock,
versus a long-lived persistent connection that would hold the lock open
and starve every other writer.

Tests use `tmp_path` + `subprocess` to simulate cross-process contention,
which is the real deployment shape (scheduler + backtest queue + API are
all separate OS processes). DuckDB's file-level lock means only one
read-write connection exists per file cluster-wide; a second attempt
raises `duckdb.IOException("Could not set lock")`.

The `_connect_with_retry` path in `datastore/api/db.py` retries on
exactly that error, which is what these tests exercise.
"""

import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pytest

from datastore.api.db import get_duckdb_connection


# ── subprocess helper script ────────────────────────────────────────────

_HOLDER_PAYLOAD = """\
import sys, time, duckdb
db_path, duration_s = sys.argv[1], float(sys.argv[2])
conn = duckdb.connect(str(db_path), read_only=False)
print("HOLDER_READY", flush=True)
time.sleep(duration_s)
conn.execute("INSERT INTO t VALUES (99)")
conn.close()
print("HOLDER_DONE", flush=True)
"""


def _start_holder(db_path: Path, duration_s: float) -> subprocess.Popen:
    """Launch the lock-holder subprocess and wait for ready signal."""
    proc = subprocess.Popen(
        [sys.executable, "-c", _HOLDER_PAYLOAD, str(db_path), str(duration_s)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    line = proc.stdout.readline()  # type: ignore[union-attr]
    while line and "HOLDER_READY" not in line:
        line = proc.stdout.readline()
    if not line:
        out, err = proc.communicate(timeout=2)
        raise RuntimeError(f"Holder never became ready. stdout={out} stderr={err}")
    return proc


def _create_db(db_path: Path) -> None:
    """Create a tiny DuckDB with one table (no contention possible here)."""
    with get_duckdb_connection(db_path, read_only=False, persist=False) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER);")


# ── tests ───────────────────────────────────────────────────────────────


class TestDeferredWriteUnderContention:

    def test_short_contention_succeeds_via_retry(self, tmp_path: Path) -> None:
        """
        A persist=False write retries and succeeds when another process
        briefly holds the write lock — the core defer_db_writes guarantee.
        """
        db_path = tmp_path / "short_contention.duckdb"
        _create_db(db_path)

        holder = _start_holder(db_path, 0.3)
        time.sleep(0.05)  # margin for holder to acquire the lock

        with get_duckdb_connection(
            db_path, read_only=False, persist=False,
            retry_attempts=15, retry_base_delay_s=0.02,
            retry_max_delay_s=0.1,
        ) as conn:
            result = conn.execute("SELECT SUM(x) FROM t").fetchone()[0]

        holder.wait(timeout=5)
        assert holder.returncode == 0
        assert result >= 99, f"Expected at least 99, got {result}"

    def test_sequential_writes_no_conflict(self, tmp_path: Path) -> None:
        """Short persist=False writes in sequence each succeed without retry."""
        db_path = tmp_path / "seq_deferred.duckdb"
        _create_db(db_path)

        for i in range(5):
            with get_duckdb_connection(
                db_path, read_only=False, persist=False,
            ) as conn:
                conn.execute("INSERT INTO t VALUES (?)", [i])

        with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
            rows = sorted(r[0] for r in conn.execute("SELECT x FROM t").fetchall())
        assert rows == [i for i in range(5)]

    def test_long_contention_survives_via_retry(self, tmp_path: Path) -> None:
        """
        When a long-running writer holds the lock longer than one retry
        interval, the persist=False writer retries and succeeds via the
        backoff mechanism (SPEC-SCHED-013).
        """
        db_path = tmp_path / "long_contention.duckdb"
        _create_db(db_path)

        holder = _start_holder(db_path, 0.6)
        time.sleep(0.05)

        with get_duckdb_connection(
            db_path, read_only=False, persist=False,
            retry_attempts=20, retry_base_delay_s=0.02,
            retry_max_delay_s=0.1,
        ) as conn:
            conn.execute("INSERT INTO t VALUES (200)")

        holder.wait(timeout=5)
        assert holder.returncode == 0

        with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
            vals = sorted(r[0] for r in conn.execute("SELECT x FROM t").fetchall())
        assert vals == [99, 200], f"Expected [99, 200], got {vals}"

    def test_exhausted_retry_gives_clear_error(self, tmp_path: Path) -> None:
        """
        When all retries are exhausted, the error is a duckdb.IOException
        with 'Could not set lock' — not a hang or a cryptic traceback.
        """
        db_path = tmp_path / "timeout.duckdb"
        _create_db(db_path)

        holder = _start_holder(db_path, 2.0)
        time.sleep(0.05)

        with pytest.raises(duckdb.IOException) as excinfo:
            with get_duckdb_connection(
                db_path, read_only=False, persist=False,
                retry_attempts=3, retry_base_delay_s=0.01,
                retry_max_delay_s=0.02,
            ) as conn:
                conn.execute("SELECT 1")

        assert "Could not set lock" in str(excinfo.value)
        holder.wait(timeout=5)

    def test_read_only_retries_through_write_lock(self, tmp_path: Path) -> None:
        """
        Read-only connections also retry when a write lock is held (DuckDB
        blocks both read-write AND read-only connections while a write is
        active). The retry-with-backoff path should still succeed once the
        writer releases.
        """
        db_path = tmp_path / "read_concurrent.duckdb"
        _create_db(db_path)

        holder = _start_holder(db_path, 0.4)
        time.sleep(0.05)

        with get_duckdb_connection(
            db_path, read_only=True, persist=False,
            retry_attempts=15, retry_base_delay_s=0.02,
            retry_max_delay_s=0.1,
        ) as conn:
            result = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]

        # After the holder released, count reflects holder's insert.
        assert result == 1
        holder.wait(timeout=5)
        assert holder.returncode == 0