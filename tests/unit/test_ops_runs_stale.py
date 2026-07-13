"""
tests/unit/test_ops_runs_stale.py

Phase: Pipeline & Monitoring Remediation Phase 1
Owner: Platform / DataStore
Consumers: CI, pytest

Exercises GET /api/v1/ops/runs's new `is_stale` field (datastore/api/
routers/ops.py::get_ops_runs) against a real on-disk SQLite pipeline log,
same pattern as tests/unit/test_paper_trading_router.py — a 'running' row
older than config.settings.PIPELINE_STALE_RUN_THRESHOLD_MINUTES must be
flagged stale (the process that started it almost certainly crashed
without ever recording a final status), while a fresh 'running' row and
any terminal-status row must not be.
"""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from datastore.api.main import app
from datastore.api.routers import ops as ops_router
from datastore.schema.create_signals import create_pipeline_runs_schema
from ingestion.scheduler.checkpoint import CheckpointManager


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline_log.db"
    create_pipeline_runs_schema(db_path=db_path)
    CheckpointManager(db_path=db_path)  # also creates pipeline_checkpoints
    monkeypatch.setattr(ops_router, "PIPELINE_LOG_DB_PATH", db_path)
    monkeypatch.setattr("config.settings.PIPELINE_STALE_RUN_THRESHOLD_MINUTES", 60)
    return TestClient(app), db_path


def _insert_run(db_path, *, date, started_at, completed_at, status):
    with get_sqlite_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
            "stocks_processed, error_message) VALUES (?, ?, ?, ?, 0, NULL)",
            (date, started_at, completed_at, status),
        )
        conn.commit()


class TestOpsRunsIsStale:
    def test_old_running_row_flagged_stale(self, client):
        app_client, db_path = client
        old_start = (now_ist() - timedelta(minutes=120)).isoformat()
        _insert_run(db_path, date="2026-07-10", started_at=old_start, completed_at=None, status="running")

        response = app_client.get("/api/v1/ops/runs")
        assert response.status_code == 200
        runs = response.json()["runs"]
        assert len(runs) == 1
        assert runs[0]["status"] == "running"
        assert runs[0]["is_stale"] is True

    def test_recent_running_row_not_flagged_stale(self, client):
        app_client, db_path = client
        recent_start = (now_ist() - timedelta(minutes=5)).isoformat()
        _insert_run(db_path, date="2026-07-10", started_at=recent_start, completed_at=None, status="running")

        response = app_client.get("/api/v1/ops/runs")
        runs = response.json()["runs"]
        assert runs[0]["status"] == "running"
        assert runs[0]["is_stale"] is False

    def test_completed_row_never_flagged_stale_regardless_of_age(self, client):
        app_client, db_path = client
        old_start = (now_ist() - timedelta(days=30)).isoformat()
        _insert_run(
            db_path, date="2026-06-10", started_at=old_start,
            completed_at=old_start, status="success",
        )

        response = app_client.get("/api/v1/ops/runs")
        runs = response.json()["runs"]
        assert runs[0]["status"] == "success"
        assert runs[0]["is_stale"] is False
