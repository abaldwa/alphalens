"""
tests/unit/test_fundamental_source_priority.py

A36: verifies features/fundamental_source_priority.py's shared merge
clause resolves real conflicts by source priority (nse_xbrl > trendlyne >
screener), never blanks an existing value with an incoming NULL,
and treats an unranked/legacy row (NULL fundamentals_source_priority) as
priority 0 so any known writer can win against it. Exercised against a
real in-memory DuckDB `fundamentals` table (create_normalised.create_schema),
never the production alphalens.duckdb.

A53 (2026-07-10): kaggle removed entirely (dead code, never scheduled) —
these tests previously used "kaggle" as the lowest-ranked source; now use
"screener" for that role instead.
"""

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from features.fundamental_source_priority import SOURCE_PRIORITY, build_priority_update_clause

_UPSERT_SQL_TEMPLATE = """
INSERT INTO fundamentals (
    ticker, fiscal_year, quarter, quarter_end_date, announcement_date,
    revenue, current_assets, fundamentals_source, fundamentals_source_priority
) VALUES (?,?,?,?,?, ?,?, ?,?)
ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {clause}
"""


def _upsert(conn, source: str, revenue, current_assets=None, ticker="AAA", fy=2026, q=1):
    clause = build_priority_update_clause(["revenue", "current_assets"])
    conn.execute(
        _UPSERT_SQL_TEMPLATE.format(clause=clause),
        [
            ticker, fy, q, "2026-03-31", "2026-05-15",
            revenue, current_assets, source, SOURCE_PRIORITY[source],
        ],
    )


def _row(conn, ticker="AAA", fy=2026, q=1):
    return conn.execute(
        "SELECT revenue, current_assets, fundamentals_source, fundamentals_source_priority "
        "FROM fundamentals WHERE ticker = ? AND fiscal_year = ? AND quarter = ?",
        [ticker, fy, q],
    ).fetchone()


class TestSourcePriorityOrdering:
    def test_priority_order_matches_operator_decision(self):
        assert SOURCE_PRIORITY["nse_xbrl"] > SOURCE_PRIORITY["trendlyne"]
        assert SOURCE_PRIORITY["trendlyne"] > SOURCE_PRIORITY["screener"]
        assert "kaggle" not in SOURCE_PRIORITY


class TestPriorityUpdateClause:
    def test_higher_priority_source_overwrites_lower_on_real_conflict(self):
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            _upsert(conn, "screener", revenue=100.0)
            _upsert(conn, "nse_xbrl", revenue=999.0)

            row = _row(conn)
            assert row[0] == 999.0
            assert row[2] == "nse_xbrl"
            assert row[3] == SOURCE_PRIORITY["nse_xbrl"]

    def test_lower_priority_source_cannot_overwrite_higher_on_real_conflict(self):
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            _upsert(conn, "nse_xbrl", revenue=999.0)
            _upsert(conn, "screener", revenue=100.0)

            row = _row(conn)
            assert row[0] == 999.0
            assert row[2] == "nse_xbrl"

    def test_trendlyne_beats_screener_chain(self):
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            _upsert(conn, "screener", revenue=2.0)
            row = _row(conn)
            assert row[0] == 2.0 and row[2] == "screener"

            _upsert(conn, "trendlyne", revenue=3.0)
            row = _row(conn)
            assert row[0] == 3.0 and row[2] == "trendlyne"

            # screener retrying afterward must not beat trendlyne's higher rank
            _upsert(conn, "screener", revenue=4.0)
            row = _row(conn)
            assert row[0] == 3.0 and row[2] == "trendlyne"

    def test_incoming_null_never_blanks_existing_value_regardless_of_priority(self):
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            _upsert(conn, "trendlyne", revenue=500.0, current_assets=200.0)
            # nse_xbrl outranks trendlyne but doesn't cover `revenue` this write
            _upsert(conn, "nse_xbrl", revenue=None, current_assets=300.0)

            row = _row(conn)
            assert row[0] == 500.0  # revenue untouched by the NULL
            assert row[1] == 300.0  # current_assets overwritten (higher priority, real conflict)

    def test_unranked_existing_row_loses_to_any_known_source(self):
        """A row written before this fix (fundamentals_source_priority NULL)
        must be treated as priority 0 — any covered writer can win against
        it, per build_priority_update_clause's COALESCE(...,0) handling."""
        create_normalised.create_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            conn.execute(
                "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, "
                "announcement_date, revenue) VALUES (?,?,?,?,?,?)",
                ["AAA", 2026, 1, "2026-03-31", "2026-05-15", 42.0],
            )
            _upsert(conn, "screener", revenue=7.0)  # lowest ranked source, still known
            row = _row(conn)
            assert row[0] == 7.0
            assert row[2] == "screener"
