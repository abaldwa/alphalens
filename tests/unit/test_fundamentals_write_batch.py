"""
tests/unit/test_fundamentals_write_batch.py

A35: POST /api/v1/fundamentals/write_batch — exercises the real FastAPI
app against a real, isolated on-disk DuckDB file (same pattern as
test_pit_alignment.py), never the production alphalens.duckdb.

Covers: many rows written in one request, one bad row (SPEC-PIPE-003
violation) isolated without aborting the rest of the batch, and A36's
priority-aware merge still applying correctly when rows arrive via the
batch endpoint.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import fundamentals as fundamentals_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "write_batch_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(fundamentals_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _row(ticker, fy, q, revenue=None, **kwargs):
    return {
        "ticker": ticker,
        "fiscal_year": fy,
        "quarter": q,
        "quarter_end_date": date(2026, 3, 31).isoformat(),
        "announcement_date": date(2026, 5, 15).isoformat(),
        "revenue": revenue,
        **kwargs,
    }


class TestWriteBatch:
    def test_many_rows_written_in_one_request(self, client):
        response = client.post(
            "/api/v1/fundamentals/write_batch",
            json={"records": [
                _row("AAA", 2026, 1, revenue=100.0),
                _row("BBB", 2026, 1, revenue=200.0),
                _row("CCC", 2026, 1, revenue=300.0),
            ]},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body == {"written": 3, "failed": 0}

        for ticker, expected in (("AAA", 100.0), ("BBB", 200.0), ("CCC", 300.0)):
            get_resp = client.get(f"/api/v1/fundamentals/{ticker}", params={"start_date": "2026-01-01", "end_date": "2026-06-30", "as_of": "2026-06-01"})
            assert get_resp.json()["data"][0]["revenue"] == expected

    def test_one_bad_row_isolated_does_not_abort_batch(self, client):
        """SPEC-PIPE-003: announcement_date <= quarter_end_date is invalid
        for a single row — must be skipped (counted in `failed`), not fail
        the whole request the way /write's HTTPException would."""
        bad_row = _row("BADCO", 2026, 1)
        bad_row["announcement_date"] = date(2026, 1, 1).isoformat()  # before quarter_end_date

        response = client.post(
            "/api/v1/fundamentals/write_batch",
            json={"records": [_row("GOODCO", 2026, 1, revenue=50.0), bad_row]},
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"written": 1, "failed": 1}

        get_resp = client.get("/api/v1/fundamentals/GOODCO", params={"start_date": "2026-01-01", "end_date": "2026-06-30", "as_of": "2026-06-01"})
        assert get_resp.json()["data"][0]["revenue"] == 50.0

        get_bad = client.get("/api/v1/fundamentals/BADCO", params={"start_date": "2026-01-01", "end_date": "2026-06-30", "as_of": "2026-06-01"})
        assert get_bad.json()["data"] == []

    def test_priority_still_applies_when_writing_via_batch_endpoint(self, client):
        """A36: write_batch stamps fundamentals_source='screener'
        (SOURCE_PRIORITY 2) same as /write — must not beat a
        higher-priority nse_xbrl row already in the table."""
        # Seed a higher-priority nse_xbrl row directly (bypassing the API,
        # same as the real nse_xbrl backfill script would have written it).
        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(fundamentals_router.DUCKDB_PATH, persist=False) as conn:
            conn.execute(
                "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, "
                "announcement_date, revenue, fundamentals_source, fundamentals_source_priority) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ["XBRLCO", 2026, 1, "2026-03-31", "2026-05-01", 999.0, "nse_xbrl", 4],
            )

        response = client.post(
            "/api/v1/fundamentals/write_batch",
            json={"records": [_row("XBRLCO", 2026, 1, revenue=1.0)]},
        )
        assert response.status_code == 200
        assert response.json() == {"written": 1, "failed": 0}

        get_resp = client.get("/api/v1/fundamentals/XBRLCO", params={"start_date": "2026-01-01", "end_date": "2026-06-30", "as_of": "2026-06-01"})
        assert get_resp.json()["data"][0]["revenue"] == 999.0  # nse_xbrl still wins
