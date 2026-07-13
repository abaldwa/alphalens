"""
tests/unit/test_corporate_announcements_router.py

A65: router-level tests for `datastore/api/routers/corporate_announcements.py`
(SPEC-DS-001/SPEC-DS-002), previously untested (41.10% coverage, no test
file). Real seeded DuckDB fixtures via TestClient(app) — no mocks.
"""

from datetime import date, timedelta

from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import corporate_announcements as ca_router
from datastore.schema import create_normalised

TODAY = date.today()


def _client(tmp_path, monkeypatch):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(ca_router, "DUCKDB_PATH", db_path)
    return TestClient(app), db_path


def _insert(db_path, seq_id, ticker, company_name, category, announced_at, subject=None):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO corporate_announcements (seq_id, ticker, company_name, category, "
            "subject, announced_at) VALUES (?, ?, ?, ?, ?, ?)",
            [seq_id, ticker, company_name, category, subject, announced_at],
        )


class TestRecentAnnouncements:
    def test_invalid_category_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.get("/api/v1/corporate-announcements/recent", params={"category": "not_a_category"})
        assert resp.status_code == 400

    def test_no_data_returns_empty(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.get("/api/v1/corporate-announcements/recent")
        assert resp.status_code == 200
        assert resp.json()["record_count"] == 0

    def test_trailing_days_filter_excludes_old_rows(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", TODAY)
        _insert(db_path, "2", "TCS", "Tata Consultancy", "qip", TODAY - timedelta(days=100))
        resp = client.get("/api/v1/corporate-announcements/recent", params={"days": 5})
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 1
        assert body["data"][0]["ticker"] == "RELIANCE"

    def test_category_filter(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", TODAY)
        _insert(db_path, "2", "TCS", "Tata Consultancy", "qip", TODAY)
        resp = client.get("/api/v1/corporate-announcements/recent", params={"category": "qip"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 1
        assert body["data"][0]["category"] == "qip"

    def test_newest_first_ordering(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", TODAY - timedelta(days=2))
        _insert(db_path, "2", "TCS", "Tata Consultancy", "qip", TODAY)
        resp = client.get("/api/v1/corporate-announcements/recent")
        assert resp.status_code == 200
        tickers = [r["ticker"] for r in resp.json()["data"]]
        assert tickers == ["TCS", "RELIANCE"]


class TestSearchAnnouncements:
    def test_neither_ticker_nor_company_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.get("/api/v1/corporate-announcements/search")
        assert resp.status_code == 400

    def test_invalid_category_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.get("/api/v1/corporate-announcements/search", params={"ticker": "RELIANCE", "category": "bogus"})
        assert resp.status_code == 400

    def test_from_after_to_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.get(
            "/api/v1/corporate-announcements/search",
            params={"ticker": "RELIANCE", "from": "2026-06-10", "to": "2026-06-01"},
        )
        assert resp.status_code == 400

    def test_search_by_ticker(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", TODAY)
        _insert(db_path, "2", "TCS", "Tata Consultancy", "qip", TODAY)
        resp = client.get("/api/v1/corporate-announcements/search", params={"ticker": "RELIANCE"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 1
        assert body["data"][0]["ticker"] == "RELIANCE"

    def test_search_by_company_substring_case_insensitive(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", TODAY)
        resp = client.get("/api/v1/corporate-announcements/search", params={"company": "reliance"})
        assert resp.status_code == 200
        assert resp.json()["record_count"] == 1

    def test_search_by_date_range_inclusive_of_to_date(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        _insert(db_path, "1", "RELIANCE", "Reliance Industries", "buyback", date(2026, 6, 15))
        _insert(db_path, "2", "RELIANCE", "Reliance Industries", "qip", date(2026, 7, 1))
        resp = client.get(
            "/api/v1/corporate-announcements/search",
            params={"ticker": "RELIANCE", "from": "2026-06-01", "to": "2026-06-30"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 1
        assert body["data"][0]["category"] == "buyback"

    def test_limit_applied(self, tmp_path, monkeypatch):
        client, db_path = _client(tmp_path, monkeypatch)
        for i in range(5):
            _insert(db_path, str(i), "RELIANCE", "Reliance Industries", "buyback", TODAY - timedelta(days=i))
        resp = client.get(
            "/api/v1/corporate-announcements/search", params={"ticker": "RELIANCE", "limit": 2}
        )
        assert resp.status_code == 200
        assert resp.json()["record_count"] == 2
