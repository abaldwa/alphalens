"""
tests/unit/test_staging_gate.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / QA
Consumers: CI, pytest

Tests datastore/staging/gate.py against a private in-memory DuckDB
connection (never the real alphalens.duckdb — see feedback memory on
never inserting test rows into the real DB).
"""

import duckdb
import pandas as pd

from datastore.staging.gate import (
    drop_staging_table,
    null_check_validator,
    stage_dataframe,
    stage_via_sql,
)


def _conn():
    return duckdb.connect(":memory:")


def _sample_df():
    return pd.DataFrame({
        "trade_date": ["2026-07-01", "2026-07-01", "2026-07-01"],
        "ticker": ["RELIANCE", "TCS", None],
        "close": [2500.0, 3800.0, 1200.0],
    })


class TestStageDataframe:
    def test_passing_rows_land_in_staging_table(self):
        conn = _conn()
        df = pd.DataFrame({"ticker": ["A", "B"], "close": [1.0, 2.0]})
        result = stage_dataframe(conn, "fno_data", df, validators=[])
        assert result.staged_rows == 2
        assert result.rejected_rows == 0
        assert result.ok

        staged = conn.execute("SELECT * FROM staging.fno_data ORDER BY ticker").df()
        assert list(staged["ticker"]) == ["A", "B"]

    def test_failing_rows_land_in_rejected_table_with_reason(self):
        conn = _conn()
        df = _sample_df()
        result = stage_dataframe(
            conn, "fno_data", df, validators=[null_check_validator(["ticker"])]
        )
        assert result.staged_rows == 2
        assert result.rejected_rows == 1
        assert result.ok

        rejected = conn.execute(
            "SELECT source_table, reason FROM staging.rejected_rows"
        ).fetchall()
        assert len(rejected) == 1
        assert rejected[0][0] == "fno_data"
        assert "ticker" in rejected[0][1]

    def test_all_rows_rejected_is_not_ok(self):
        conn = _conn()
        df = pd.DataFrame({"ticker": [None, None], "close": [1.0, 2.0]})
        result = stage_dataframe(
            conn, "fno_data", df, validators=[null_check_validator(["ticker"])]
        )
        assert result.staged_rows == 0
        assert result.rejected_rows == 2
        assert not result.ok

    def test_gate_is_idempotent_on_rerun(self):
        conn = _conn()
        df = pd.DataFrame({"ticker": ["A"], "close": [1.0]})
        stage_dataframe(conn, "fno_data", df, validators=[])
        result2 = stage_dataframe(conn, "fno_data", df, validators=[])
        assert result2.staged_rows == 1
        staged = conn.execute("SELECT COUNT(*) FROM staging.fno_data").fetchone()[0]
        assert staged == 1  # CREATE OR REPLACE, not appended


class TestDropStagingTable:
    def test_drop_staging_table_removes_it(self):
        conn = _conn()
        df = pd.DataFrame({"ticker": ["A"], "close": [1.0]})
        stage_dataframe(conn, "fno_data", df, validators=[])
        drop_staging_table(conn, "fno_data")
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'"
        ).fetchall()
        assert ("fno_data",) not in tables

    def test_drop_staging_table_is_safe_when_absent(self):
        conn = _conn()
        drop_staging_table(conn, "does_not_exist")  # should not raise


class TestStageViaSql:
    def test_merge_via_sql_never_materializes_existing_table_in_python(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (trade_date VARCHAR, ticker VARCHAR)")
        conn.executemany(
            "INSERT INTO fno_data VALUES (?, ?)",
            [("2026-01-01", "OLD1"), ("2026-01-01", "OLD2"), ("2026-01-02", "KEEP")],
        )
        new_df = pd.DataFrame({"trade_date": ["2026-01-01"], "ticker": ["NEW1"]})
        merge_sql = (
            "SELECT * FROM fno_data WHERE trade_date NOT IN (?) "
            "UNION ALL SELECT * FROM _stage_new_batch"
        )
        result = stage_via_sql(conn, "fno_data", new_df, merge_sql, ["2026-01-01"], validators=[])
        assert result.staged_rows == 1
        assert result.ok

        staged = conn.execute("SELECT * FROM staging.fno_data ORDER BY trade_date, ticker").df()
        assert list(staged["ticker"]) == ["NEW1", "KEEP"]

    def test_rejected_new_rows_never_reach_the_merge(self):
        conn = _conn()
        conn.execute("CREATE TABLE fno_data (trade_date VARCHAR, ticker VARCHAR)")
        conn.execute("INSERT INTO fno_data VALUES ('2026-01-02', 'KEEP')")
        new_df = pd.DataFrame({"trade_date": ["2026-01-01"], "ticker": [None]})
        merge_sql = (
            "SELECT * FROM fno_data WHERE trade_date NOT IN (?) "
            "UNION ALL SELECT * FROM _stage_new_batch"
        )
        result = stage_via_sql(
            conn, "fno_data", new_df, merge_sql, ["2026-01-01"],
            validators=[null_check_validator(["ticker"])],
        )
        assert not result.ok
        assert result.rejected_rows == 1
        rejected = conn.execute("SELECT reason FROM staging.rejected_rows").fetchall()
        assert len(rejected) == 1


class TestNullCheckValidator:
    def test_empty_dataframe_passes_through(self):
        validator = null_check_validator(["ticker"])
        empty = pd.DataFrame({"ticker": pd.Series(dtype=str)})
        passed, rejected = validator(empty)
        assert passed.empty
        assert rejected.empty

    def test_column_absent_from_frame_is_ignored(self):
        validator = null_check_validator(["not_a_real_column"])
        df = pd.DataFrame({"ticker": ["A", "B"]})
        passed, rejected = validator(df)
        assert len(passed) == 2
        assert rejected.empty
