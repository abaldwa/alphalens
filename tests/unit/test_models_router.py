"""
tests/unit/test_models_router.py

A65: router-level tests for `datastore/api/routers/models.py` (SPEC-DS-004),
previously untested (35.71% coverage, no test file). Reads a real JSON file
on tmp_path (no DuckDB/SQLite backs this endpoint), via TestClient(app) — no
mocks, per this repo's no-stub/synthetic-data policy.
"""

import json

from fastapi.testclient import TestClient

from datastore.api.main import app
from datastore.api.routers import models as models_router


def _write_registry(tmp_path, monkeypatch, registry: dict):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(registry))
    monkeypatch.setattr(models_router, "MODEL_REGISTRY_PATH", path)
    return path


class TestGetModels:
    def test_missing_registry_file_returns_404(self, tmp_path, monkeypatch):
        monkeypatch.setattr(models_router, "MODEL_REGISTRY_PATH", tmp_path / "does_not_exist.json")
        client = TestClient(app)
        resp = client.get("/api/v1/models")
        assert resp.status_code == 404

    def test_full_entry_maps_all_fields(self, tmp_path, monkeypatch):
        _write_registry(
            tmp_path, monkeypatch,
            {
                "signal_21d": {
                    "name": "signal_21d", "version": "v3", "model_type": "lightgbm",
                    "created_at": "2026-06-01T00:00:00", "feature_names": ["rsi_14", "adx_14"],
                    "accuracy_on_validation": 0.62, "hyperparams": {"n_estimators": 200},
                    "training_samples": 50000, "training_time_seconds": 120.5,
                }
            },
        )
        client = TestClient(app)
        resp = client.get("/api/v1/models")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_models"] == 1
        m = body["models"][0]
        assert m["name"] == "signal_21d"
        assert m["version"] == "v3"
        assert m["model_type"] == "lightgbm"
        assert m["features_used"] == ["rsi_14", "adx_14"]
        assert m["accuracy_on_validation"] == 0.62
        assert m["hyperparameters"] == {"n_estimators": 200}
        assert m["training_samples"] == 50000
        assert body["latest_model_by_name"]["signal_21d"]["version"] == "v3"

    def test_legacy_entry_missing_version_and_model_type_defaults_to_unknown(self, tmp_path, monkeypatch):
        _write_registry(
            tmp_path, monkeypatch,
            {"hmm_market": {"saved_path": "/models/hmm.pkl", "saved_at": "2026-05-01T00:00:00"}},
        )
        client = TestClient(app)
        resp = client.get("/api/v1/models")
        assert resp.status_code == 200
        body = resp.json()
        m = body["models"][0]
        assert m["name"] == "hmm_market"
        assert m["version"] == "unknown"
        assert m["model_type"] == "unknown"
        assert m["features_used"] == []

    def test_filter_by_model_name_returns_only_matching(self, tmp_path, monkeypatch):
        _write_registry(
            tmp_path, monkeypatch,
            {
                "signal_21d": {"name": "signal_21d", "version": "v1", "created_at": "2026-01-01T00:00:00"},
                "signal_63d": {"name": "signal_63d", "version": "v1", "created_at": "2026-01-01T00:00:00"},
            },
        )
        client = TestClient(app)
        resp = client.get("/api/v1/models", params={"model_name": "signal_21d"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_models"] == 1
        assert body["models"][0]["name"] == "signal_21d"

    def test_filter_by_unknown_model_name_returns_404(self, tmp_path, monkeypatch):
        _write_registry(
            tmp_path, monkeypatch,
            {"signal_21d": {"name": "signal_21d", "version": "v1", "created_at": "2026-01-01T00:00:00"}},
        )
        client = TestClient(app)
        resp = client.get("/api/v1/models", params={"model_name": "not_a_real_model"})
        assert resp.status_code == 404

    def test_multiple_versions_same_name_latest_by_created_at_wins(self, tmp_path, monkeypatch):
        _write_registry(
            tmp_path, monkeypatch,
            {
                "signal_21d_v1": {
                    "name": "signal_21d", "version": "v1", "created_at": "2026-01-01T00:00:00",
                },
                "signal_21d_v2": {
                    "name": "signal_21d", "version": "v2", "created_at": "2026-06-01T00:00:00",
                },
            },
        )
        client = TestClient(app)
        resp = client.get("/api/v1/models")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_models"] == 2
        assert body["latest_model_by_name"]["signal_21d"]["version"] == "v2"
