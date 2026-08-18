"""
Unit tests for datastore.api.db's connection cache across read_only modes.

DuckDB permits only ONE configuration per database file per process. The cache
used to be keyed by f"{path}|read_only={mode}", which made a read-only and a
read-write request to the same file two separate cache entries -- so the second
one tried to open a second connection with a different configuration, and
DuckDB refused:

    Can't open a connection to same database file with a different
    configuration than existing connections

That is not a hypothetical. It was returning 500 from the signals API in
tests/integration/test_daily_pipeline.py, because create_signal_tables_schema()
writes through the default persist=True (leaving a cached read-write connection
open for the life of the process) and the signals router then reads through
persist=False, read_only=True.

tmp_path DuckDB files only -- never the real database.
"""

from __future__ import annotations

import duckdb
import pytest

from datastore.api.db import close_all_connections, get_duckdb_connection


@pytest.fixture(autouse=True)
def _clean_pool():
    """The cache is module-global; leaking entries between tests would let one
    test's connection satisfy another's and hide a real regression."""
    close_all_connections()
    yield
    close_all_connections()


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "modes.duckdb"


class TestSameFileAcrossModes:
    def test_persisted_write_then_persisted_read(self, db_path):
        """The original failure, at its simplest."""
        with get_duckdb_connection(db_path, persist=True, read_only=False) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (1), (2)")
        with get_duckdb_connection(db_path, persist=True, read_only=True) as conn:
            assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 2

    def test_persisted_write_then_unpersisted_read(self, db_path):
        """The exact shape the signals API hit: a writer on the default
        persist=True, then a reader on persist=False, read_only=True."""
        with get_duckdb_connection(db_path, persist=True, read_only=False) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (7)")
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            assert conn.execute("SELECT x FROM t").fetchone()[0] == 7

    def test_read_then_write_upgrades_rather_than_failing_at_the_write(self, db_path):
        """A cached read-only connection cannot serve a write, and it blocks
        opening a read-write one. It must be retired, not reused -- reusing it
        would fail at the first write, far from the cause."""
        duckdb.connect(str(db_path)).close()  # the file must exist to open read-only
        with get_duckdb_connection(db_path, persist=True, read_only=True) as conn:
            conn.execute("SELECT 1")
        with get_duckdb_connection(db_path, persist=True, read_only=False) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (9)")
            assert conn.execute("SELECT x FROM t").fetchone()[0] == 9

    def test_write_request_is_never_served_by_a_read_only_connection(self, db_path):
        """The upgrade must actually happen: if the read-only connection were
        silently reused, this write would raise."""
        duckdb.connect(str(db_path)).close()
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            conn.execute("SELECT 1")
        with get_duckdb_connection(db_path, persist=True, read_only=True) as conn:
            conn.execute("SELECT 1")
        # Now a write, through the unpersisted path, against a cached RO conn.
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0

    def test_two_different_files_still_get_two_connections(self, tmp_path):
        """Keying by path must not collapse distinct files onto one connection."""
        a, b = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
        with get_duckdb_connection(a, persist=True, read_only=False) as conn:
            conn.execute("CREATE TABLE only_in_a (x INTEGER)")
        with get_duckdb_connection(b, persist=True, read_only=False) as conn:
            conn.execute("CREATE TABLE only_in_b (x INTEGER)")
            names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert names == {"only_in_b"}, f"b's connection sees a's tables: {names}"
