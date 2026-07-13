"""
tests/unit/test_regime_router.py

A65: router-level tests for `datastore/api/routers/regime.py` (SPEC-DS-002/
SPEC-DS-003), previously untested (57.69% coverage, no test file). Real
seeded DuckDB (ml_signals, 'hmm_market'/'MARKET' sentinel rows) via
TestClient(app) — no mocks.
"""

from datetime import date

from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import regime as regime_router
from datastore.schema import create_signals


def _seed(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=duckdb_path)
    close_all_connections()
    monkeypatch.setattr(regime_router, "SIGNALS_DUCKDB_PATH", duckdb_path)
    return duckdb_path


def _insert_regime(db_path, d, regime, prob, stability):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO ml_signals (date, ticker, model_name, model_version, hmm_regime, "
            "hmm_regime_prob, hmm_stability) VALUES (?, 'MARKET', 'hmm_market', 'v1', ?, ?, ?)",
            [d, regime, prob, stability],
        )


class TestGetRegime:
    def test_no_data_returns_unavailable(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime")
        assert resp.status_code == 200
        body = resp.json()
        assert body["available"] is False

    def test_returns_latest_when_no_as_of(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime")
        assert resp.status_code == 200
        body = resp.json()
        assert body["available"] is True
        assert body["date"] == "2026-06-05T00:00:00"
        assert body["hmm_regime"] == "bear"

    def test_as_of_returns_pit_correct_row(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime", params={"as_of": "2026-06-03"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-06-01T00:00:00"
        assert body["hmm_regime"] == "bull"

    def test_as_of_before_any_data_returns_unavailable(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime", params={"as_of": "2026-05-01"})
        assert resp.status_code == 200
        assert resp.json()["available"] is False


class TestGetRegimeHistory:
    def test_no_data_returns_empty_list(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history")
        assert resp.status_code == 200
        assert resp.json()["days"] == []

    def test_returns_ascending_by_date(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 3), "neutral", 0.5, 0.85)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history")
        assert resp.status_code == 200
        days = resp.json()["days"]
        assert [d["date"] for d in days] == ["2026-06-01T00:00:00", "2026-06-03T00:00:00", "2026-06-05T00:00:00"]
        assert [d["hmm_regime"] for d in days] == ["bull", "neutral", "bear"]

    def test_days_param_limits_and_still_returns_most_recent(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        for i, r in enumerate(["bull", "bear", "neutral", "bull"]):
            _insert_regime(db_path, date(2026, 6, 1 + i), r, 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history", params={"days": 2})
        assert resp.status_code == 200
        days = resp.json()["days"]
        assert len(days) == 2
        assert [d["date"] for d in days] == ["2026-06-03T00:00:00", "2026-06-04T00:00:00"]

    def test_days_param_out_of_range_returns_422(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history", params={"days": 0})
        assert resp.status_code == 422
