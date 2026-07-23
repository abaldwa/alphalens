"""
tests/unit/test_system_router.py

A65: router-level tests for `datastore/api/routers/system.py` (SPEC-DS-002/
SPEC-PIPE-001/SPEC-SCHED-005/SPEC-SYS-002/SPEC-SCHED-013), previously
untested (34.00% coverage, no test file). Real seeded DuckDB (normalised
Store) + SQLite (pipeline log) fixtures via TestClient(app) — no mocks, per
this repo's no-stub/synthetic-data policy.

`datastore.api.utils.scheduler_status.get_scheduler_heartbeats` imports
PIPELINE_LOG_DB_PATH into its own module namespace at import time, same as
alert_store/feature_store did for test_technical_router.py — patched
separately from the router's own copy for the same reason.
"""

from datetime import date

from fastapi.testclient import TestClient

from config.timezone import now_ist
from datastore.api.db import close_all_connections, get_duckdb_connection, get_sqlite_connection
from datastore.api.main import app
from datastore.api.routers import system as system_router
from datastore.api.utils import scheduler_status as scheduler_status_module
from datastore.schema import create_normalised, create_signals


def _seed(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "normalised_test.duckdb"
    sqlite_path = tmp_path / "pipeline_log_test.db"
    create_normalised.create_schema(db_path=duckdb_path)
    create_signals.create_pipeline_runs_schema(db_path=sqlite_path)
    create_signals.create_pipeline_drift_log_schema(db_path=sqlite_path)
    create_signals.create_scheduler_heartbeats_schema(db_path=sqlite_path)
    close_all_connections()
    monkeypatch.setattr(system_router, "DUCKDB_PATH", duckdb_path)
    monkeypatch.setattr(system_router, "PIPELINE_LOG_DB_PATH", sqlite_path)
    monkeypatch.setattr(scheduler_status_module, "PIPELINE_LOG_DB_PATH", sqlite_path)
    return duckdb_path, sqlite_path


class TestHealthCheck:
    def test_no_data_at_all_returns_healthy_with_defaults(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["last_pipeline_run"] is None
        assert body["stock_count"] == 0
        assert body["drift"]["worst_status"] == "unknown"

    def test_with_pipeline_run_stock_count_and_drift(self, tmp_path, monkeypatch):
        duckdb_path, sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-01", "2026-07-01T06:00:00", "2026-07-01T06:10:00", "success", 500, None],
            )
            conn.execute(
                "INSERT INTO pipeline_drift_log (date, worst_feature, worst_psi, worst_status, "
                "n_features_checked, checked_at) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-01", "close_zscore", 0.12, "warning", 40, "2026-07-01T06:15:00"],
            )
            conn.commit()
        with get_duckdb_connection(duckdb_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["RELIANCE", date(2026, 7, 1), 100, 105, 99, 102, 1000000],
            )
            conn.execute(
                "INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["TCS", date(2026, 7, 1), 200, 205, 199, 202, 500000],
            )
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["last_pipeline_run"]["status"] == "success"
        assert body["last_pipeline_run"]["stocks_processed"] == 500
        assert body["stock_count"] == 2
        assert body["drift"]["worst_status"] == "warning"
        assert body["drift"]["worst_feature"] == "close_zscore"

    def test_scheduler_heartbeats_included(self, tmp_path, monkeypatch):
        _, sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO scheduler_heartbeats (job_id, last_attempt_at, last_status, "
                "last_error, last_success_at) VALUES (?, ?, ?, ?, ?)",
                ["daily_pipeline", now_ist().isoformat(), "success", None, now_ist().isoformat()],
            )
            conn.commit()
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        job_ids = {j["job_id"] for j in body["scheduler"]}
        assert "daily_pipeline" in job_ids


class TestListingDates:
    def test_no_listing_dates_returns_empty_dict(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/stock-master/listing-dates")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_returns_ticker_to_isoformat_date_map(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        with get_duckdb_connection(duckdb_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO stock_master (ticker, company_name, nse_series, listing_date) "
                "VALUES (?, ?, ?, ?)",
                ["ZOMATO", "Zomato Ltd", "EQ", date(2021, 7, 23)],
            )
            conn.execute(
                "INSERT INTO stock_master (ticker, company_name, nse_series, listing_date) "
                "VALUES (?, ?, ?, ?)",
                ["RELIANCE", "Reliance Industries", "EQ", None],
            )
        client = TestClient(app)
        resp = client.get("/stock-master/listing-dates")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"ZOMATO": "2021-07-23"}
