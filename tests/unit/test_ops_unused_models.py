"""
tests/unit/test_ops_unused_models.py

Phase: Pipeline & Monitoring Remediation, Phase 5 (A53)
Owner: Platform / DataStore
Consumers: CI, pytest

Exercises GET /api/v1/ops/unused-models against the real FastAPI app with
an isolated registry.json (never the production models directory).
"""

import json

import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    import config.settings as settings_mod

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    monkeypatch.setattr(settings_mod, "MODELS_DIR", models_dir)
    return TestClient(app), models_dir


class TestGetOpsUnusedModels:
    def test_no_registry_returns_empty(self, client):
        app_client, _ = client
        response = app_client.get("/api/v1/ops/unused-models")
        assert response.status_code == 200
        assert response.json()["unused"] == []

    def test_trained_model_with_no_consumer_is_surfaced(self, client):
        app_client, models_dir = client
        (models_dir / "registry.json").write_text(json.dumps({
            "tft": {"last_trained_date": "2026-07-01", "training_interval_days": 28},
            "signal_5d": {"last_trained_date": "2026-07-01", "training_interval_days": 28},
        }))

        response = app_client.get("/api/v1/ops/unused-models")
        unused = response.json()["unused"]
        assert len(unused) == 1
        assert unused[0]["model_name"] == "tft"
        assert unused[0]["last_trained_date"] == "2026-07-01"
