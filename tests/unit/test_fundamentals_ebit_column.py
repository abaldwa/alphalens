"""
tests/unit/test_fundamentals_ebit_column.py

Regression test for the 2026-07-13 FO1/FO9 wiring fix: `ebit` is a real,
computed column (datastore/schema/create_normalised.py:184) that was
missing from both datastore/api/routers/fundamentals.py's `_COLUMNS`
SELECT list and datastore/api/schemas.py's FundamentalsWrite model — so
GET /api/v1/fundamentals/{ticker} silently dropped it from every response
even though the DB held real values (same class of bug already fixed for
total_equity/retained_earnings/total_assets/cwip on 2026-07-07).

Exercises the real FastAPI app against a real, isolated on-disk DuckDB
file (same pattern as test_fundamentals_write_batch.py), never the
production alphalens.duckdb.
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
    db_path = tmp_path / "ebit_column_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(fundamentals_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _row(ticker, fy, q, **kwargs):
    return {
        "ticker": ticker,
        "fiscal_year": fy,
        "quarter": q,
        "quarter_end_date": date(2026, 3, 31).isoformat(),
        "announcement_date": date(2026, 5, 15).isoformat(),
        **kwargs,
    }


class TestEbitReturnedByGetFundamentals:
    def test_ebit_round_trips_through_write_and_get(self, client):
        write_resp = client.post(
            "/api/v1/fundamentals/write",
            json=_row("AAA", 2026, 1, revenue=100.0, ebitda=180.0, ebit=120.0),
        )
        assert write_resp.status_code == 200, write_resp.text

        get_resp = client.get(
            "/api/v1/fundamentals/AAA",
            params={"start_date": "2026-01-01", "end_date": "2026-12-31"},
        )
        assert get_resp.status_code == 200, get_resp.text
        body = get_resp.json()
        assert body["data"], body
        row = body["data"][0]
        assert "ebit" in row, "ebit missing from GET response — _COLUMNS/schema regression"
        assert row["ebit"] == pytest.approx(120.0)
