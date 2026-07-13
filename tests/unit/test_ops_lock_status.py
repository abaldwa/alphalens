"""
tests/unit/test_ops_lock_status.py

Phase: Pipeline & Monitoring Remediation, Phase 2
Owner: Platform / Scheduler
Consumers: CI, pytest

Exercises GET /api/v1/ops/lock-status against the real FastAPI app with
isolated tmp_path lock files (never the production lock paths).
"""

import fcntl

import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    import config.settings as settings_mod

    monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")
    monkeypatch.setattr(settings_mod, "PUBLISH_RUN_LOCK_PATH", tmp_path / "publish_run.lock")
    return TestClient(app), tmp_path


class TestGetOpsLockStatus:
    def test_no_lock_files_yet(self, client):
        app_client, _ = client
        response = app_client.get("/api/v1/ops/lock-status")
        assert response.status_code == 200
        locks = {entry["name"]: entry for entry in response.json()["locks"]}
        assert set(locks) == {"pipeline_run_lock", "publish_run_lock"}
        assert locks["pipeline_run_lock"]["exists"] is False
        assert locks["pipeline_run_lock"]["locked"] is False

    def test_held_lock_reported_as_locked(self, client):
        app_client, tmp_path = client
        lock_path = tmp_path / "pipeline_run.lock"
        lock_path.write_text("")

        with open(lock_path, "r+") as holder:
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            response = app_client.get("/api/v1/ops/lock-status")
            locks = {entry["name"]: entry for entry in response.json()["locks"]}
            assert locks["pipeline_run_lock"]["locked"] is True

            fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
