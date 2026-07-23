"""
tests/unit/test_pipeline_router.py

A65: router-level tests for `datastore/api/routers/pipeline.py` (SPEC-PIPE-001/
SPEC-SYS-002), previously untested (33.33% coverage, no test file). Real
seeded SQLite fixtures (pipeline_runs/pipeline_checkpoints) via TestClient(app)
— no mocks, per this repo's no-stub/synthetic-data policy.
"""

from fastapi.testclient import TestClient

from datastore.api.db import get_sqlite_connection
from datastore.api.main import app
from datastore.api.routers import pipeline as pipeline_router
from datastore.schema import create_signals


def _seed(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "pipeline_log_test.db"
    create_signals.create_pipeline_runs_schema(db_path=sqlite_path)
    create_signals.create_pipeline_checkpoints_schema(db_path=sqlite_path)
    monkeypatch.setattr(pipeline_router, "PIPELINE_LOG_DB_PATH", sqlite_path)
    return sqlite_path


class TestPipelineStatus:
    def test_no_run_for_date_returns_404(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/pipeline/status/2026-01-01")
        assert resp.status_code == 404

    def test_successful_run_returns_full_status(self, tmp_path, monkeypatch):
        sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-01", "2026-07-01T06:00:00", "2026-07-01T06:10:00", "success", 500, None],
            )
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status) "
                "VALUES (?, ?, ?, ?)",
                ["2026-07-01", "ingest_bhavcopy", 1, "success"],
            )
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status) "
                "VALUES (?, ?, ?, ?)",
                ["2026-07-01", "compute_features", 2, "success"],
            )
            conn.commit()
        client = TestClient(app)
        resp = client.get("/api/v1/pipeline/status/2026-07-01")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "success"
        assert body["stage"] == "compute_features"
        assert body["records_processed"] == 500
        assert body["records_failed"] == 0
        assert body["data_completeness_pct"] == 100.0
        assert body["duration_seconds"] == 600.0
        assert body["error_summary"] is None

    def test_partial_failure_run_reports_failed_steps_and_completeness(self, tmp_path, monkeypatch):
        sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-02", "2026-07-02T06:00:00", None, "failed", 200, "trendlyne scrape timed out"],
            )
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status) "
                "VALUES (?, ?, ?, ?)",
                ["2026-07-02", "ingest_bhavcopy", 1, "success"],
            )
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status) "
                "VALUES (?, ?, ?, ?)",
                ["2026-07-02", "ingest_trendlyne", 2, "failed"],
            )
            conn.commit()
        client = TestClient(app)
        resp = client.get("/api/v1/pipeline/status/2026-07-02")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "failed"
        assert body["records_failed"] == 1
        assert body["data_completeness_pct"] == 50.0
        assert body["duration_seconds"] is None
        assert body["error_summary"] == "trendlyne scrape timed out"

    def test_run_with_no_checkpoints_reports_unknown_stage_and_zero_completeness(
        self, tmp_path, monkeypatch
    ):
        sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-03", "2026-07-03T06:00:00", None, "running", 0, None],
            )
            conn.commit()
        client = TestClient(app)
        resp = client.get("/api/v1/pipeline/status/2026-07-03")
        assert resp.status_code == 200
        body = resp.json()
        assert body["stage"] == "unknown"
        assert body["data_completeness_pct"] == 0.0
        assert body["records_processed"] == 0

    def test_multiple_runs_same_date_returns_latest(self, tmp_path, monkeypatch):
        sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-04", "2026-07-04T06:00:00", "2026-07-04T06:05:00", "failed", 0, "first attempt failed"],
            )
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                ["2026-07-04", "2026-07-04T07:00:00", "2026-07-04T07:05:00", "success", 500, None],
            )
            conn.commit()
        client = TestClient(app)
        resp = client.get("/api/v1/pipeline/status/2026-07-04")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "success"
        assert body["records_processed"] == 500
