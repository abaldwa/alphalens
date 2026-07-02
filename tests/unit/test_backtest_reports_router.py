"""
tests/unit/test_backtest_reports_router.py

Phase: 3.x (Web UI — SPEC-UI-005 Backtest Results screen)
Owner: Platform / QA
Consumers: CI, pytest
"""

import json

import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app
from datastore.api.routers import backtest_reports as backtest_reports_router


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_reports_router, "REPORTS_DIR", tmp_path)
    return TestClient(app)


class TestListReports:
    def test_no_reports_dir_returns_empty(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr(backtest_reports_router, "REPORTS_DIR", tmp_path / "missing")
        response = client.get("/api/v1/backtest/reports")
        assert response.json() == {"reports": []}

    def test_lists_json_files_without_extension(self, client, tmp_path):
        (tmp_path / "phase2_20260624.json").write_text("{}")
        (tmp_path / "phase1_20260622.json").write_text("{}")
        response = client.get("/api/v1/backtest/reports")
        assert sorted(response.json()["reports"]) == ["phase1_20260622", "phase2_20260624"]


class TestGetReport:
    def test_returns_report_contents(self, client, tmp_path):
        (tmp_path / "phase2_20260624.json").write_text(json.dumps({"generated_at": "2026-06-24"}))
        response = client.get("/api/v1/backtest/reports/phase2_20260624")
        assert response.status_code == 200
        assert response.json() == {"generated_at": "2026-06-24"}

    def test_missing_report_returns_404(self, client):
        response = client.get("/api/v1/backtest/reports/nonexistent")
        assert response.status_code == 404

    def test_path_traversal_rejected(self, client):
        response = client.get("/api/v1/backtest/reports/..%2F..%2Fsecrets")
        assert response.status_code in (400, 404)
