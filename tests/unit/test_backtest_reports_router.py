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


class TestTaComparisons:
    """[2026-08-10] The collated TA comparison endpoints.

    /reports lists EVERY *.json in backtest/reports — 11,000+ per-job
    artifacts in the real directory — so these endpoints exist to give the
    Backtest screen a usable index of just the collated comparisons.
    """

    def _write(self, tmp_path, name, payload):
        (tmp_path / f"{name}.json").write_text(json.dumps(payload))

    def test_empty_when_no_comparisons(self, client, tmp_path):
        (tmp_path / "orchestrator_x_job0.json").write_text("{}")
        response = client.get("/api/v1/backtest/ta-comparisons")
        assert response.status_code == 200
        assert response.json() == {"comparisons": []}

    def test_lists_only_comparison_reports_newest_first(self, client, tmp_path):
        self._write(tmp_path, "ta_comparison_old", {
            "queue_suffix": "old", "tax_regime": "ltcg_10pct_1L",
            "generated_at": "2026-08-01T00:00:00", "n_strategies": 46, "failed_reports": [],
        })
        self._write(tmp_path, "ta_comparison_new", {
            "queue_suffix": "new", "tax_regime": "ltcg_12_5pct_1_25L",
            "generated_at": "2026-08-10T00:00:00", "n_strategies": 65,
            "failed_reports": [{"report": "a.json", "error": "boom"}],
        })
        # Must be ignored — not a comparison report.
        (tmp_path / "orchestrator_ta5y_job3.json").write_text("{}")

        body = client.get("/api/v1/backtest/ta-comparisons").json()["comparisons"]
        assert [c["name"] for c in body] == ["ta_comparison_new", "ta_comparison_old"]
        assert body[0]["n_strategies"] == 65
        assert body[0]["n_failed"] == 1
        assert body[1]["n_failed"] == 0

    def test_unreadable_report_does_not_break_listing(self, client, tmp_path):
        """The autopilot collates while the queue still runs, so a
        half-written file must not 500 the whole index."""
        (tmp_path / "ta_comparison_broken.json").write_text("{not json")
        self._write(tmp_path, "ta_comparison_ok", {
            "queue_suffix": "ok", "tax_regime": "r", "generated_at": "2026-08-10T00:00:00",
            "n_strategies": 1, "failed_reports": [],
        })
        body = client.get("/api/v1/backtest/ta-comparisons").json()["comparisons"]
        assert [c["name"] for c in body] == ["ta_comparison_ok"]

    def test_get_returns_full_payload(self, client, tmp_path):
        payload = {
            "queue_suffix": "ta_full_2007_2026", "tax_regime": "ltcg_12_5pct_1_25L",
            "generated_at": "2026-08-10T00:00:00", "basis": "realized",
            "n_strategies": 1, "failed_reports": [],
            "strategies": [{"template_name": "A1", "style": "Volatility"}],
        }
        self._write(tmp_path, "ta_comparison_ta_full_2007_2026", payload)
        response = client.get("/api/v1/backtest/ta-comparisons/ta_comparison_ta_full_2007_2026")
        assert response.status_code == 200
        assert response.json() == payload

    def test_non_comparison_name_rejected(self, client, tmp_path):
        (tmp_path / "phase2_20260624.json").write_text("{}")
        response = client.get("/api/v1/backtest/ta-comparisons/phase2_20260624")
        assert response.status_code == 400

    def test_missing_comparison_returns_404(self, client):
        assert client.get("/api/v1/backtest/ta-comparisons/ta_comparison_nope").status_code == 404

    @pytest.mark.parametrize("name", ["ta_comparison_../secret", "ta_comparison_..\\secret"])
    def test_path_traversal_blocked(self, client, name):
        assert client.get(f"/api/v1/backtest/ta-comparisons/{name}").status_code in (400, 404)
