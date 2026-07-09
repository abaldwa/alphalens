"""
tests/unit/test_publish.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / QA
Consumers: CI, pytest

Tests datastore/staging/publish.py against a private in-memory DuckDB
connection.
"""

import duckdb
import pandas as pd
import pytest

from datastore.staging.gate import stage_dataframe
from datastore.staging.publish import publish_table


def _conn():
    return duckdb.connect(":memory:")


class TestPublishTable:
    def test_publish_atomically_replaces_table_content(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (ticker VARCHAR, close DOUBLE)")
        conn.execute("INSERT INTO fno_data VALUES ('OLD', 1.0)")

        new_df = pd.DataFrame({"ticker": ["NEW1", "NEW2"], "close": [2.0, 3.0]})
        stage_dataframe(conn, "fno_data", new_df, validators=[])
        row_count = publish_table(conn, "fno_data")

        assert row_count == 2
        rows = conn.execute("SELECT ticker FROM fno_data ORDER BY ticker").fetchall()
        assert [r[0] for r in rows] == ["NEW1", "NEW2"]

    def test_publish_drops_staging_table_by_default(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (ticker VARCHAR)")
        stage_dataframe(conn, "fno_data", pd.DataFrame({"ticker": ["A"]}), validators=[])
        publish_table(conn, "fno_data")

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'"
        ).fetchall()
        assert ("fno_data",) not in tables

    def test_publish_without_staging_first_raises(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (ticker VARCHAR)")
        with pytest.raises(duckdb.Error):
            publish_table(conn, "fno_data")

    def test_failed_staging_never_touches_production_table(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (ticker VARCHAR)")
        conn.execute("INSERT INTO fno_data VALUES ('ORIGINAL')")

        # Simulate an all-rejected batch: caller correctly does NOT call
        # publish_table when StageResult.ok is False.
        from datastore.staging.gate import null_check_validator

        bad_df = pd.DataFrame({"ticker": [None]})
        result = stage_dataframe(conn, "fno_data", bad_df, validators=[null_check_validator(["ticker"])])
        assert not result.ok

        rows = conn.execute("SELECT ticker FROM fno_data").fetchall()
        assert rows == [("ORIGINAL",)]


class TestPublishRunLock:
    def test_lock_is_exclusive_across_two_holders(self, tmp_path, monkeypatch):
        import config.settings as settings
        from datastore.staging import publish as publish_module

        lock_path = tmp_path / ".publish_run.lock"
        monkeypatch.setattr(settings, "PUBLISH_RUN_LOCK_PATH", lock_path)
        monkeypatch.setattr(publish_module, "PUBLISH_RUN_LOCK_PATH", lock_path)

        with publish_module.publish_run_lock() as acquired_first:
            assert acquired_first is True
            with publish_module.publish_run_lock() as acquired_second:
                assert acquired_second is False

        # Lock released after the first `with` exits — a fresh acquire succeeds.
        with publish_module.publish_run_lock() as acquired_third:
            assert acquired_third is True
