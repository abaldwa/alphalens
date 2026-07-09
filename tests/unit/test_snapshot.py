"""
tests/unit/test_snapshot.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / QA
Consumers: CI, pytest

Tests datastore/staging/snapshot.py against a private in-memory DuckDB
connection and a pytest tmp_path snapshot directory (never the real
datastore/snapshots/ or alphalens.duckdb).
"""

import duckdb
import pytest

from datastore.staging.snapshot import (
    list_snapshot_dates,
    prune_snapshots,
    restore_snapshot,
    take_snapshot,
)


def _conn_with_table(rows=(("A", 1.0), ("B", 2.0))):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE fno_data (ticker VARCHAR, close DOUBLE)")
    conn.executemany("INSERT INTO fno_data VALUES (?, ?)", list(rows))
    return conn


class TestTakeSnapshot:
    def test_produces_correct_parquet_per_table(self, tmp_path):
        conn = _conn_with_table()
        out_dir = take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-01")
        parquet_path = out_dir / "fno_data.parquet"
        assert parquet_path.exists()

        roundtrip = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') ORDER BY ticker").fetchall()
        assert roundtrip == [("A", 1.0), ("B", 2.0)]

    def test_unchanged_table_is_hardlinked_not_recopied(self, tmp_path):
        conn = _conn_with_table()
        day1 = take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-01")
        day2 = take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-02")

        p1 = day1 / "fno_data.parquet"
        p2 = day2 / "fno_data.parquet"
        assert p1.stat().st_ino == p2.stat().st_ino  # same inode == hard link

    def test_changed_table_gets_a_fresh_export(self, tmp_path):
        conn = _conn_with_table()
        day1 = take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-01")
        conn.execute("INSERT INTO fno_data VALUES ('C', 3.0)")
        day2 = take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-02")

        p1 = day1 / "fno_data.parquet"
        p2 = day2 / "fno_data.parquet"
        assert p1.stat().st_ino != p2.stat().st_ino


class TestPruneSnapshots:
    def test_keeps_exactly_n_most_recent(self, tmp_path):
        conn = _conn_with_table()
        for d in ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04", "2026-07-05"]:
            take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date=d)

        removed = prune_snapshots(tmp_path, keep_n=3)
        assert len(removed) == 2
        remaining = list_snapshot_dates(tmp_path)
        assert remaining == ["2026-07-03", "2026-07-04", "2026-07-05"]

    def test_prune_on_empty_dir_is_a_noop(self, tmp_path):
        assert prune_snapshots(tmp_path / "does_not_exist", keep_n=7) == []


class TestRestoreSnapshot:
    def test_restore_round_trips_table_content(self, tmp_path):
        conn = _conn_with_table()
        take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-01")

        conn.execute("DELETE FROM fno_data")
        conn.execute("INSERT INTO fno_data VALUES ('CORRUPTED', 999.0)")

        restored = restore_snapshot(conn, tmp_path, "2026-07-01")
        assert restored == ["fno_data"]
        rows = conn.execute("SELECT * FROM fno_data ORDER BY ticker").fetchall()
        assert rows == [("A", 1.0), ("B", 2.0)]

    def test_restore_missing_date_raises_with_available_dates_listed(self, tmp_path):
        conn = _conn_with_table()
        take_snapshot(conn, ["fno_data"], tmp_path, snapshot_date="2026-07-01")

        with pytest.raises(FileNotFoundError, match="2026-07-01"):
            restore_snapshot(conn, tmp_path, "2026-01-01")

    def test_restore_specific_table_only(self, tmp_path):
        conn = _conn_with_table()
        conn.execute("CREATE TABLE ohlcv_adjusted (ticker VARCHAR)")
        conn.execute("INSERT INTO ohlcv_adjusted VALUES ('X')")
        take_snapshot(conn, ["fno_data", "ohlcv_adjusted"], tmp_path, snapshot_date="2026-07-01")

        conn.execute("DELETE FROM ohlcv_adjusted")
        restored = restore_snapshot(conn, tmp_path, "2026-07-01", tables=["fno_data"])
        assert restored == ["fno_data"]
        assert conn.execute("SELECT COUNT(*) FROM ohlcv_adjusted").fetchone()[0] == 0
