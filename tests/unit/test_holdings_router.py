"""
tests/unit/test_holdings_router.py

ML30 — real seeded-DuckDB (tmp_path, never the production
datastore/normalised/alphalens.duckdb) TestClient(app) tests for
datastore/api/routers/holdings.py's CRUD + CSV-upload endpoints, and for
the my_holdings table itself (datastore/schema/create_normalised.py).
No mocks over the DB layer, per this repo's no-stub/synthetic-data
testing policy.
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import holdings as holdings_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(holdings_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


class TestMyHoldingsSchema:
    def test_table_created_with_expected_columns(self, tmp_path):
        db_path = tmp_path / "normalised_test.duckdb"
        create_normalised.create_schema(db_path=db_path)
        close_all_connections()
        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            cols = {
                r[0] for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'my_holdings'"
                ).fetchall()
            }
        assert {
            "id", "ticker", "purchase_date", "qty", "purchase_price",
            "sale_date", "sell_price", "purchase_rationale", "sell_rationale",
            "journal_entry",
        } <= cols


class TestCreateListUpdateDeleteHolding:
    def test_create_and_list_holding(self, client):
        resp = client.post(
            "/api/v1/holdings/",
            json={
                "ticker": "reliance", "purchase_date": "2026-01-05", "qty": 10,
                "purchase_price": 2500.0, "purchase_rationale": "Long-term compounder",
            },
        )
        assert resp.status_code == 200, resp.text
        created = resp.json()
        assert created["ticker"] == "RELIANCE"
        assert created["sale_date"] is None

        listed = client.get("/api/v1/holdings/").json()
        assert len(listed) == 1
        assert listed[0]["id"] == created["id"]

    def test_open_only_filter_excludes_sold_positions(self, client):
        open_pos = client.post(
            "/api/v1/holdings/",
            json={"ticker": "TCS", "purchase_date": "2026-01-01", "qty": 5},
        ).json()
        client.post(
            "/api/v1/holdings/",
            json={
                "ticker": "INFY", "purchase_date": "2026-01-01", "qty": 5,
                "sale_date": "2026-02-01", "sell_price": 1600.0,
            },
        )

        open_only = client.get("/api/v1/holdings/", params={"open_only": True}).json()
        assert len(open_only) == 1
        assert open_only[0]["id"] == open_pos["id"]

    def test_update_records_a_sale(self, client):
        created = client.post(
            "/api/v1/holdings/",
            json={"ticker": "HDFCBANK", "purchase_date": "2026-01-01", "qty": 3, "purchase_price": 1500.0},
        ).json()

        resp = client.put(
            f"/api/v1/holdings/{created['id']}",
            json={"sale_date": "2026-03-01", "sell_price": 1650.0, "sell_rationale": "Target achieved"},
        )
        assert resp.status_code == 200, resp.text
        updated = resp.json()
        assert updated["sale_date"] == "2026-03-01"
        assert updated["sell_price"] == 1650.0
        assert updated["sell_rationale"] == "Target achieved"
        # Unrelated fields unchanged.
        assert updated["ticker"] == "HDFCBANK"
        assert updated["qty"] == 3

    def test_update_nonexistent_holding_404s(self, client):
        resp = client.put("/api/v1/holdings/99999", json={"sell_price": 100.0})
        assert resp.status_code == 404

    def test_delete_holding(self, client):
        created = client.post(
            "/api/v1/holdings/", json={"ticker": "WIPRO", "purchase_date": "2026-01-01", "qty": 1},
        ).json()
        resp = client.delete(f"/api/v1/holdings/{created['id']}")
        assert resp.status_code == 200
        assert client.get("/api/v1/holdings/").json() == []

    def test_delete_nonexistent_holding_404s(self, client):
        resp = client.delete("/api/v1/holdings/99999")
        assert resp.status_code == 404


class TestUploadHoldingsCsv:
    def test_upload_valid_csv_creates_rows(self, client):
        csv_text = (
            "ticker,purchase_date,qty,purchase_price,purchase_rationale\n"
            "RELIANCE,2026-01-05,10,2500.0,Long-term compounder\n"
            "TCS,2026-01-06,5,3800.0,\n"
        )
        resp = client.post(
            "/api/v1/holdings/upload-csv",
            content=csv_text.encode(),
            headers={"content-type": "text/csv"},
        )
        assert resp.status_code == 200, resp.text
        rows = resp.json()
        assert len(rows) == 2
        tickers = {r["ticker"] for r in rows}
        assert tickers == {"RELIANCE", "TCS"}
        tcs_row = next(r for r in rows if r["ticker"] == "TCS")
        assert tcs_row["purchase_rationale"] is None  # blank cell => real NULL, not ""

    def test_upload_csv_missing_required_column_400s(self, client):
        csv_text = "ticker,qty\nRELIANCE,10\n"
        resp = client.post(
            "/api/v1/holdings/upload-csv",
            content=csv_text.encode(),
            headers={"content-type": "text/csv"},
        )
        assert resp.status_code == 400

    def test_upload_csv_skips_rows_missing_required_values(self, client):
        csv_text = (
            "ticker,purchase_date,qty\n"
            "RELIANCE,2026-01-05,10\n"
            ",2026-01-06,5\n"  # missing ticker — skipped
        )
        resp = client.post(
            "/api/v1/holdings/upload-csv",
            content=csv_text.encode(),
            headers={"content-type": "text/csv"},
        )
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        assert rows[0]["ticker"] == "RELIANCE"
