"""tests/unit/test_fundamentals_history.py — append-only fundamentals_history
+ datastore/api/pit.py::get_fundamentals_pit (BacktestUmbrellaPlan.md
Truthful Review Gap #2 fix, 2026-07-20).

Real seeded DuckDB schema (create_normalised.create_schema), no mocks over
the DB layer — proves a restatement recorded AFTER a backtest's as_of date
does not leak into that backtest, using real INSERT/append_fundamentals_history
calls, not fabricated rows.
"""

from datetime import datetime

import pytest

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.pit import get_fundamentals_pit
from datastore.schema import create_normalised
from features.fundamental_source_priority import append_fundamentals_history


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    close_all_connections()
    return p


def _insert_fundamentals(
    conn, ticker, fiscal_year, quarter, announcement_date, revenue,
    quarter_end_date="2018-03-31", source="nse_xbrl", priority=4,
):
    conn.execute(
        """
        INSERT INTO fundamentals
            (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, revenue,
             fundamentals_source, fundamentals_source_priority, as_of_ingested)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET
            revenue = excluded.revenue,
            announcement_date = excluded.announcement_date,
            fundamentals_source = excluded.fundamentals_source,
            fundamentals_source_priority = excluded.fundamentals_source_priority,
            as_of_ingested = excluded.as_of_ingested
        """,
        [ticker, fiscal_year, quarter, quarter_end_date, announcement_date, revenue, source, priority],
    )
    append_fundamentals_history(conn, ticker, fiscal_year, quarter)


class TestFundamentalsHistorySchema:
    def test_table_created_and_starts_empty(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            n = conn.execute("SELECT count(*) FROM fundamentals_history").fetchone()[0]
        assert n == 0

    def test_has_same_data_columns_as_fundamentals_plus_history_id_and_recorded_at(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            fcols = {r[0] for r in conn.execute("DESCRIBE fundamentals").fetchall()}
            hcols = {r[0] for r in conn.execute("DESCRIBE fundamentals_history").fetchall()}
        assert fcols.issubset(hcols)
        assert {"history_id", "recorded_at"}.issubset(hcols)


class TestAppendFundamentalsHistory:
    def test_a_single_write_appends_one_history_row(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 100.0)
            n = conn.execute(
                "SELECT count(*) FROM fundamentals_history WHERE ticker = 'RELIANCE'"
            ).fetchone()[0]
        assert n == 1

    def test_a_restatement_appends_a_second_row_never_overwrites(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 100.0)
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 150.0)  # restated
            rows = conn.execute(
                "SELECT revenue FROM fundamentals_history WHERE ticker = 'RELIANCE' ORDER BY recorded_at"
            ).fetchall()
        assert [r[0] for r in rows] == [100.0, 150.0]
        # live `fundamentals` table still shows only the current value (unchanged behavior)
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            live = conn.execute("SELECT revenue FROM fundamentals WHERE ticker = 'RELIANCE'").fetchone()[0]
        assert live == 150.0


class TestGetFundamentalsPit:
    def test_restatement_recorded_after_as_of_does_not_leak(self, db_path):
        """The core regression test for Gap #2: a backtest 'as of' 2018-06-01
        must see the ORIGINAL 100.0 value, never the 150.0 restatement that
        was only recorded (in real wall-clock time) after that as_of date.

        append_fundamentals_history always stamps recorded_at as the real
        wall-clock time of the call (CURRENT_TIMESTAMP) — correct production
        behavior, but it means BOTH calls in a test running today get
        recorded_at ~= 2026, not the 2018-era timestamps a real 2018 ingest
        and a real later restatement would actually have. Backdating the
        first snapshot's recorded_at to a real 2018 timestamp (metadata
        only, not the financial data itself) is the only way to test this
        temporal-filtering logic without waiting years for it to be real —
        same "control the clock, not the data" pattern as any date-mocking
        test elsewhere in this suite."""
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 100.0)
            conn.execute(
                "UPDATE fundamentals_history SET recorded_at = '2018-05-02' WHERE ticker = 'RELIANCE'"
            )

        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 150.0)  # restated "now" (real 2026 recorded_at)
            pit = get_fundamentals_pit(conn, ["RELIANCE"], datetime(2018, 6, 1))

        assert len(pit) == 1
        assert pit.iloc[0]["revenue"] == 100.0  # NOT the later restatement

    def test_a_query_as_of_today_sees_the_latest_restatement(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 100.0)
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 150.0)
            pit = get_fundamentals_pit(conn, ["RELIANCE"], datetime(2030, 1, 1))
        assert pit.iloc[0]["revenue"] == 150.0

    def test_announcement_date_filter_still_applies(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "RELIANCE", 2018, 1, "2018-05-01", 100.0)
            pit = get_fundamentals_pit(conn, ["RELIANCE"], datetime(2018, 1, 1))  # before announcement
        assert pit.empty

    def test_empty_tickers_returns_empty_df_not_raise(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            pit = get_fundamentals_pit(conn, [], datetime(2020, 1, 1))
        assert pit.empty

    def test_rejects_non_datetime_as_of(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            with pytest.raises(ValueError):
                get_fundamentals_pit(conn, ["RELIANCE"], "2020-01-01")
