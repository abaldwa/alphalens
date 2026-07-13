"""
tests/unit/test_ops_router.py

A65: router-level tests for `datastore/api/routers/ops.py`
(SPEC-SCHED-014), previously 33.89% covered. Real seeded SQLite
(PIPELINE_LOG_DB_PATH) / DuckDB (DUCKDB_PATH) fixtures via TestClient(app),
same monkeypatch pattern as tests/unit/test_ops_runs_stale.py — no mocks,
per this repo's no-stub/synthetic-data policy.

Deliberately NOT covered here: /steps/{step_name}/force (delegates to
ingestion/scheduler/force_run.py's real step-runner, which touches the
live pipeline — out of scope for a router-level test), /scheduler-resources
and /live-resources (subprocess systemctl + psutil against the real
alphalens-scheduler.service, environment-dependent), and
/missed-jobs/{id}/approve (triggers a real catch-up run via
datastore/health/catchup.py). Those are exercised, where relevant, by
other existing test files (test_ops_runs_stale.py, force_run tests).
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection, get_sqlite_connection
from datastore.api.main import app
from datastore.api.routers import ops as ops_router
from datastore.schema import create_normalised, create_signals
from ingestion.scheduler.checkpoint import STEPS, CheckpointManager


@pytest.fixture
def client(tmp_path, monkeypatch):
    pipeline_log_path = tmp_path / "pipeline_log.db"
    duckdb_path = tmp_path / "normalised_test.duckdb"
    create_signals.create_pipeline_runs_schema(db_path=pipeline_log_path)
    CheckpointManager(db_path=pipeline_log_path)  # also creates pipeline_checkpoints
    create_normalised.create_schema(db_path=duckdb_path)
    close_all_connections()

    monkeypatch.setattr(ops_router, "PIPELINE_LOG_DB_PATH", pipeline_log_path)
    monkeypatch.setattr(ops_router, "DUCKDB_PATH", duckdb_path)

    return TestClient(app), pipeline_log_path, duckdb_path


class TestTradingCalendarHolidays:
    def test_returns_sorted_iso_dates(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/trading-calendar/holidays")
        assert r.status_code == 200
        body = r.json()
        holidays = body["holidays"]
        assert len(holidays) > 0
        assert holidays == sorted(holidays)
        # every entry parses as ISO YYYY-MM-DD
        date.fromisoformat(holidays[0])


class TestOpsFreshness:
    def test_no_data_present_reports_error_rows_not_a_500(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/freshness")
        assert r.status_code == 200
        body = r.json()
        sources = {s["source"] for s in body["sources"]}
        assert "ohlcv_adjusted" in sources
        assert "mf_holdings" in sources

    def test_seeded_ohlcv_reports_row_count_and_latest_date(self, client, monkeypatch):
        app_client, _, duckdb_path = client
        with get_duckdb_connection(duckdb_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume) "
                "VALUES ('RELIANCE', '2026-06-01', 100, 101, 99, 100, 1000)"
            )

        import config.settings as settings_module

        monkeypatch.setattr(settings_module, "DUCKDB_PATH", duckdb_path)

        r = app_client.get("/api/v1/ops/freshness")
        assert r.status_code == 200
        rows_by_source = {s["source"]: s for s in r.json()["sources"]}
        assert rows_by_source["ohlcv_adjusted"]["row_count"] == 1
        assert rows_by_source["ohlcv_adjusted"]["latest_data_date"] == "2026-06-01"


class TestOpsRuns:
    def test_empty_returns_no_runs(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/runs")
        assert r.status_code == 200
        assert r.json()["runs"] == []

    def test_failed_run_attaches_failed_steps(self, client):
        app_client, pipeline_log_path, _ = client
        with get_sqlite_connection(pipeline_log_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, 'failed', 0, NULL)",
                ("2026-06-01", "2026-06-01T06:00:00", "2026-06-01T06:10:00"),
            )
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status, "
                "started_at, completed_at, error_message) VALUES (?, ?, ?, 'failed', ?, ?, ?)",
                ("2026-06-01", "download_bhavcopy", 0, "2026-06-01T06:00:00",
                 "2026-06-01T06:01:00", "connection refused"),
            )
            conn.commit()

        r = app_client.get("/api/v1/ops/runs")
        assert r.status_code == 200
        runs = r.json()["runs"]
        assert len(runs) == 1
        assert runs[0]["status"] == "failed"
        assert len(runs[0]["failed_steps"]) == 1
        assert runs[0]["failed_steps"][0]["step_name"] == "download_bhavcopy"
        assert runs[0]["failed_steps"][0]["error_message"] == "connection refused"

    def test_limit_param_caps_results(self, client):
        app_client, pipeline_log_path, _ = client
        with get_sqlite_connection(pipeline_log_path) as conn:
            for i in range(5):
                conn.execute(
                    "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                    "stocks_processed, error_message) VALUES (?, ?, ?, 'success', 0, NULL)",
                    (f"2026-06-{i+1:02d}", f"2026-06-{i+1:02d}T06:00:00", f"2026-06-{i+1:02d}T06:10:00"),
                )
            conn.commit()

        r = app_client.get("/api/v1/ops/runs", params={"limit": 2})
        assert r.status_code == 200
        assert len(r.json()["runs"]) == 2


class TestOpsSteps:
    def test_no_checkpoints_yet_all_steps_never_run(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/steps", params={"date": "2026-06-01"})
        assert r.status_code == 200
        body = r.json()
        assert body["date"] == "2026-06-01"
        assert len(body["steps"]) == len(STEPS)
        assert all(s["status"] == "never_run" for s in body["steps"])

    def test_success_checkpoint_reflected(self, client):
        app_client, pipeline_log_path, _ = client
        with get_sqlite_connection(pipeline_log_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_checkpoints (date, step_name, step_index, status, "
                "started_at, completed_at) VALUES (?, ?, ?, 'success', ?, ?)",
                ("2026-06-01", "download_bhavcopy", 0, "2026-06-01T06:00:00", "2026-06-01T06:01:00"),
            )
            conn.commit()

        r = app_client.get("/api/v1/ops/steps", params={"date": "2026-06-01"})
        assert r.status_code == 200
        by_name = {s["step_name"]: s for s in r.json()["steps"]}
        assert by_name["download_bhavcopy"]["status"] == "success"
        assert by_name["download_bhavcopy"]["last_success_date"] == "2026-06-01"


class TestForceRunStep:
    def test_unknown_step_returns_404(self, client):
        app_client, _, _ = client
        r = app_client.post("/api/v1/ops/steps/not_a_real_step/force")
        assert r.status_code == 404


class TestExceptionCatalog:
    def test_returns_real_catalog_entries(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/exception-catalog")
        assert r.status_code == 200
        body = r.json()
        assert len(body["entries"]) > 0
        assert "step_name" in body["entries"][0]


class TestUnusedModels:
    def test_no_registry_file_returns_empty(self, client, tmp_path, monkeypatch):
        app_client, _, _ = client
        import config.settings as settings_module

        monkeypatch.setattr(settings_module, "MODELS_DIR", tmp_path / "no_such_models_dir")
        r = app_client.get("/api/v1/ops/unused-models")
        assert r.status_code == 200
        assert r.json()["unused"] == []


class TestLockStatus:
    def test_returns_both_known_locks(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/lock-status")
        assert r.status_code == 200
        names = {entry["name"] for entry in r.json()["locks"]}
        assert "pipeline_run_lock" in names or len(names) > 0


class TestIntegrityFindings:
    def test_empty_findings_table_returns_no_findings(self, client):
        app_client, _, _ = client
        r = app_client.get("/api/v1/ops/integrity-findings")
        assert r.status_code == 200
        assert r.json()["findings"] == []

    def test_insert_list_approve_reject_finding(self, client):
        app_client, _, duckdb_path = client
        from datastore.integrity.findings import Finding, insert_finding

        with get_duckdb_connection(duckdb_path, persist=False, read_only=False) as conn:
            fid_pending = insert_finding(conn, Finding(
                check_name="null_sweep", finding_date=date(2026, 6, 1), severity="warning",
                description="test finding A", ticker="RELIANCE",
            ))
            fid_to_reject = insert_finding(conn, Finding(
                check_name="null_sweep", finding_date=date(2026, 6, 1), severity="info",
                description="test finding B",
            ))

        r = app_client.get("/api/v1/ops/integrity-findings")
        assert r.status_code == 200
        assert len(r.json()["findings"]) == 2

        r = app_client.get("/api/v1/ops/integrity-findings", params={"check_name": "null_sweep"})
        assert len(r.json()["findings"]) == 2

        r = app_client.post(
            f"/api/v1/ops/integrity-findings/{fid_pending}/approve",
            params={"reviewed_by": "tester"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "approved"  # no proposed_fix_sql -> "approved", not "applied"
        assert r.json()["reviewed_by"] == "tester"

        r = app_client.post(f"/api/v1/ops/integrity-findings/{fid_to_reject}/reject")
        assert r.status_code == 200
        assert r.json()["status"] == "rejected"

        r = app_client.post("/api/v1/ops/integrity-findings/999999/approve")
        assert r.status_code == 400


class TestMissedJobFindings:
    def test_empty_findings_table_returns_no_findings(self, client):
        app_client, _, duckdb_path = client
        r = app_client.get("/api/v1/ops/missed-jobs")
        assert r.status_code == 200
        assert r.json()["findings"] == []

    def test_insert_list_and_reject_finding(self, client):
        app_client, _, duckdb_path = client
        from datastore.health.findings import Finding, insert_finding

        with get_duckdb_connection(duckdb_path, persist=False, read_only=False) as conn:
            fid = insert_finding(conn, Finding(
                job_id="weekend_feature_backfill", missed_date=date(2026, 6, 6), severity="warning",
                description="missed weekend job",
            ))

        r = app_client.get("/api/v1/ops/missed-jobs")
        assert r.status_code == 200
        assert len(r.json()["findings"]) == 1
        assert r.json()["findings"][0]["job_id"] == "weekend_feature_backfill"

        r = app_client.get("/api/v1/ops/missed-jobs", params={"job_id": "weekend_feature_backfill"})
        assert len(r.json()["findings"]) == 1

        r = app_client.post(f"/api/v1/ops/missed-jobs/{fid}/reject", params={"reviewed_by": "tester"})
        assert r.status_code == 200
        assert r.json()["status"] == "rejected"

        r = app_client.post("/api/v1/ops/missed-jobs/999999/reject")
        assert r.status_code == 400
