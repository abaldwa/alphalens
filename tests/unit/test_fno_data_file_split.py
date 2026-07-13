"""
tests/unit/test_fno_data_file_split.py

A50 (2026-07-10): fno_data was split into its own DuckDB file
(datastore/api/db.py::fno_db_path_for/_attach_fno_db) so a publish can
swap it in atomically (datastore/staging/publish.py::publish_fno_data)
instead of rewriting all ~121M rows in place via CREATE OR REPLACE TABLE.

Exercises: the companion-path derivation, transparent ATTACH+search_path
resolution (SELECT/INSERT/DELETE all work unqualified), and the atomic
publish itself — all against real on-disk tmp_path DuckDB files, never
the production alphalens.duckdb.
"""

from pathlib import Path

import pandas as pd

from datastore.api.db import fno_db_path_for, get_duckdb_connection
from datastore.schema import create_normalised
from datastore.staging.gate import null_check_validator, stage_via_sql
from datastore.staging.publish import publish_fno_data, publish_run_lock


class TestFnoDbPathFor:
    def test_derives_a_sibling_file_named_after_the_main_db(self):
        main = Path("/some/dir/alphalens.duckdb")
        assert fno_db_path_for(str(main)) == Path("/some/dir/alphalens_fno_data.duckdb")

    def test_different_main_paths_get_different_companion_files(self):
        a = fno_db_path_for("/tmp/test_a.duckdb")
        b = fno_db_path_for("/tmp/test_b.duckdb")
        assert a != b


class TestTransparentAttach:
    def test_schema_creation_puts_fno_data_in_its_own_file(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        create_normalised.create_schema(db_path=db_path)

        fno_path = fno_db_path_for(str(db_path))
        assert fno_path.exists()
        assert fno_path != db_path

    def test_unqualified_select_insert_delete_all_resolve_transparently(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        create_normalised.create_schema(db_path=db_path)

        with get_duckdb_connection(db_path, persist=False) as conn:
            conn.execute(
                "INSERT INTO fno_data (trade_date, ticker, instrument, expiry, oi, oi_change, "
                "volume, settle_price, close_price, underlying_price) "
                "VALUES ('2026-01-01','AAA','STF','2026-01-29',100,10,50,10.0,10.0,10.0)"
            )
            rows = conn.execute("SELECT ticker, oi FROM fno_data").fetchall()
            assert rows == [("AAA", 100)]

            conn.execute("DELETE FROM fno_data WHERE ticker = 'AAA'")
            assert conn.execute("SELECT COUNT(*) FROM fno_data").fetchone()[0] == 0

    def test_read_only_connection_can_also_read_the_attached_file(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        create_normalised.create_schema(db_path=db_path)

        with get_duckdb_connection(db_path, persist=False) as conn:
            conn.execute(
                "INSERT INTO fno_data (trade_date, ticker, instrument, expiry, oi, oi_change, "
                "volume, settle_price, close_price, underlying_price) "
                "VALUES ('2026-01-01','AAA','STF','2026-01-29',100,10,50,10.0,10.0,10.0)"
            )

        with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
            rows = conn.execute("SELECT ticker FROM fno_data").fetchall()
            assert rows == [("AAA",)]

    def test_in_memory_schema_keeps_fno_data_inline_not_split(self):
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            # No ATTACH for in-memory — this must not raise, and fno_data
            # is queryable directly in the single in-memory catalog.
            conn.execute(
                "INSERT INTO fno_data (trade_date, ticker, instrument, expiry, oi, oi_change, "
                "volume, settle_price, close_price, underlying_price) "
                "VALUES ('2026-01-01','AAA','STF','2026-01-29',100,10,50,10.0,10.0,10.0)"
            )
            assert conn.execute("SELECT COUNT(*) FROM fno_data").fetchone()[0] == 1


class TestPublishFnoData:
    def test_atomic_swap_preserves_untouched_dates_and_adds_new_ones(self, tmp_path, monkeypatch):
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setattr("datastore.staging.publish.PUBLISH_RUN_LOCK_PATH", tmp_path / "publish.lock")
        create_normalised.create_schema(db_path=db_path)

        with get_duckdb_connection(db_path, persist=False) as conn:
            conn.execute(
                "INSERT INTO fno_data (trade_date, ticker, instrument, expiry, oi, oi_change, "
                "volume, settle_price, close_price, underlying_price) "
                "VALUES ('2026-01-01','AAA','STF','2026-01-29',100,10,50,10.0,10.0,10.0)"
            )

            new_df = pd.DataFrame([{
                "trade_date": pd.Timestamp("2026-01-02"), "ticker": "BBB", "instrument": "STF",
                "expiry": pd.Timestamp("2026-01-29"), "strike": None, "option_type": None,
                "oi": 200, "oi_change": 20, "volume": 60,
                "settle_price": 20.0, "close_price": 20.0, "underlying_price": 20.0,
            }])
            merge_sql = (
                "SELECT * FROM fno_data WHERE trade_date NOT IN (?) "
                "UNION ALL SELECT * FROM _stage_new_batch"
            )
            result = stage_via_sql(
                conn, "fno_data", new_df, merge_sql, [pd.Timestamp("2026-01-02").date()],
                validators=[null_check_validator(["trade_date", "ticker"])],
            )
            assert result.staged_rows == 1

            with publish_run_lock() as acquired:
                assert acquired
                published_rows = publish_fno_data(conn)

            assert published_rows == 2
            rows = conn.execute("SELECT ticker, oi FROM fno_data ORDER BY trade_date").fetchall()
            assert rows == [("AAA", 100), ("BBB", 200)]

    def test_fno_db_path_still_a_valid_standalone_file_after_swap(self, tmp_path, monkeypatch):
        """The atomic swap must leave a real, openable DuckDB file at the
        SAME path — not a dangling temp file or a broken symlink."""
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setattr("datastore.staging.publish.PUBLISH_RUN_LOCK_PATH", tmp_path / "publish.lock")
        create_normalised.create_schema(db_path=db_path)

        with get_duckdb_connection(db_path, persist=False) as conn:
            new_df = pd.DataFrame([{
                "trade_date": pd.Timestamp("2026-01-01"), "ticker": "AAA", "instrument": "STF",
                "expiry": pd.Timestamp("2026-01-29"), "strike": None, "option_type": None,
                "oi": 100, "oi_change": 10, "volume": 50,
                "settle_price": 10.0, "close_price": 10.0, "underlying_price": 10.0,
            }])
            merge_sql = "SELECT * FROM fno_data WHERE trade_date NOT IN (?) UNION ALL SELECT * FROM _stage_new_batch"
            stage_via_sql(
                conn, "fno_data", new_df, merge_sql, [pd.Timestamp("2026-01-01").date()],
                validators=[null_check_validator(["trade_date", "ticker"])],
            )
            with publish_run_lock() as acquired:
                assert acquired
                publish_fno_data(conn)

        fno_path = fno_db_path_for(str(db_path))
        assert fno_path.exists()
        # No leftover temp swap files
        leftovers = list(fno_path.parent.glob(".fno_data.new.*.duckdb"))
        assert leftovers == []

        # A fresh connection (simulating a different process) sees the swapped data
        with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
            rows = conn.execute("SELECT ticker FROM fno_data").fetchall()
            assert rows == [("AAA",)]
