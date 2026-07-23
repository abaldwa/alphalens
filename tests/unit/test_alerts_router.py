"""
tests/unit/test_alerts_router.py

A65: router-level tests for `datastore/api/routers/alerts.py` (SPEC-ALERT-001),
previously untested (0% coverage, no test file). Real seeded DuckDB (Store 4
ml_signals) + SQLite (pipeline_drift_log) fixtures via TestClient(app) — no
mocks, per this repo's no-stub/synthetic-data policy.

GET /api/v1/alerts/today keys off `config.timezone.now_ist().date()`, so
fixtures seed rows for "today" using the same `now_ist()` helper the router
calls, rather than a hardcoded date — avoids a flaky test that only passes on
the day it was written.
"""

from fastapi.testclient import TestClient

from config.timezone import now_ist
from datastore.api.db import close_all_connections, get_duckdb_connection, get_sqlite_connection
from datastore.api.main import app
from datastore.api.routers import alerts as alerts_router
from datastore.schema import create_signals

TODAY = now_ist().date()


def _seed(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "signals_test.duckdb"
    sqlite_path = tmp_path / "pipeline_log_test.db"
    create_signals.create_schema(sqlite_path=sqlite_path, duckdb_path=duckdb_path)
    close_all_connections()
    monkeypatch.setattr(alerts_router, "SIGNALS_DUCKDB_PATH", duckdb_path)
    monkeypatch.setattr(alerts_router, "PIPELINE_LOG_DB_PATH", sqlite_path)
    return duckdb_path, sqlite_path


def _insert_ml_signal(db_path, **kwargs):
    cols = ", ".join(kwargs.keys())
    placeholders = ", ".join(["?"] * len(kwargs))
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(f"INSERT INTO ml_signals ({cols}) VALUES ({placeholders})", list(kwargs.values()))


class TestAlertsToday:
    def test_no_data_returns_empty_alerts(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/alerts/today")
        assert resp.status_code == 200
        body = resp.json()
        assert body["alerts"] == []
        assert body["count"] == 0

    def test_pnd_blocked_row_produces_high_severity_block_alert(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="RELIANCE", model_name="pnd_detector",
            model_version="v1", pnd_score=85.0, pnd_phase="pump", pnd_block=True,
        )
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 1
        alert = body["alerts"][0]
        assert alert["ticker"] == "RELIANCE"
        assert alert["alert_type"] == "pnd_block"
        assert alert["severity"] == "high"
        assert "BLOCKED" in alert["message"]

    def test_pnd_flagged_not_blocked_is_medium_severity_flag(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="TCS", model_name="pnd_detector",
            model_version="v1", pnd_score=55.0, pnd_phase="watch", pnd_block=False,
        )
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 1
        alert = body["alerts"][0]
        assert alert["alert_type"] == "pnd_flag"
        assert alert["severity"] == "medium"
        assert "flagged" in alert["message"]

    def test_pnd_score_at_or_below_threshold_excluded(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="INFY", model_name="pnd_detector",
            model_version="v1", pnd_score=40.0, pnd_phase="none", pnd_block=False,
        )
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 0

    def test_exit_urgency_above_threshold_produces_urgent_alert(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        from config.settings import EXIT_URGENT_THRESHOLD

        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="HDFCBANK", model_name="exit_signal",
            model_version="v1", exit_urgency=EXIT_URGENT_THRESHOLD + 5.0, exit_type="stop_loss",
        )
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 1
        alert = body["alerts"][0]
        assert alert["alert_type"] == "exit_urgent"
        assert alert["severity"] == "high"
        assert "HDFCBANK" in alert["message"]

    def test_exit_urgency_at_or_below_threshold_excluded(self, tmp_path, monkeypatch):
        duckdb_path, _ = _seed(tmp_path, monkeypatch)
        from config.settings import EXIT_URGENT_THRESHOLD

        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="WIPRO", model_name="exit_signal",
            model_version="v1", exit_urgency=EXIT_URGENT_THRESHOLD, exit_type="stop_loss",
        )
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 0

    def test_drift_halt_row_produces_high_severity_alert(self, tmp_path, monkeypatch):
        _, sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_drift_log (date, worst_feature, worst_psi, worst_status, checked_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [TODAY.isoformat(), "close_zscore", 0.35, "halt", now_ist().isoformat()],
            )
            conn.commit()
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 1
        alert = body["alerts"][0]
        assert alert["alert_type"] == "drift_halt"
        assert alert["severity"] == "high"
        assert alert["ticker"] is None
        assert "close_zscore" in alert["message"]

    def test_drift_warning_row_produces_medium_severity_alert(self, tmp_path, monkeypatch):
        _, sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_drift_log (date, worst_feature, worst_psi, worst_status, checked_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [TODAY.isoformat(), "volume_zscore", 0.15, "warning", now_ist().isoformat()],
            )
            conn.commit()
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 1
        assert body["alerts"][0]["alert_type"] == "drift_warning"
        assert body["alerts"][0]["severity"] == "medium"

    def test_drift_ok_status_produces_no_alert(self, tmp_path, monkeypatch):
        _, sqlite_path = _seed(tmp_path, monkeypatch)
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_drift_log (date, worst_feature, worst_psi, worst_status, checked_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [TODAY.isoformat(), "close_zscore", 0.02, "ok", now_ist().isoformat()],
            )
            conn.commit()
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 0

    def test_missing_pipeline_drift_log_table_is_swallowed_not_500(self, tmp_path, monkeypatch):
        """PIPELINE_LOG_DB_PATH pointed at a file with no schema at all —
        exercises the router's `except Exception` fallback around the
        SQLite read, confirming a missing-table error degrades to "no
        drift alert" rather than a 500."""
        duckdb_path = tmp_path / "signals_test2.duckdb"
        create_signals.create_signal_tables_schema(db_path=duckdb_path)
        close_all_connections()
        monkeypatch.setattr(alerts_router, "SIGNALS_DUCKDB_PATH", duckdb_path)
        monkeypatch.setattr(alerts_router, "PIPELINE_LOG_DB_PATH", tmp_path / "does_not_exist.db")
        client = TestClient(app)
        resp = client.get("/api/v1/alerts/today")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_multiple_alert_types_combined_and_counted(self, tmp_path, monkeypatch):
        duckdb_path, sqlite_path = _seed(tmp_path, monkeypatch)
        from config.settings import EXIT_URGENT_THRESHOLD

        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="RELIANCE", model_name="pnd_detector",
            model_version="v1", pnd_score=85.0, pnd_phase="pump", pnd_block=True,
        )
        _insert_ml_signal(
            duckdb_path, date=TODAY, ticker="HDFCBANK", model_name="exit_signal",
            model_version="v1", exit_urgency=EXIT_URGENT_THRESHOLD + 5.0, exit_type="stop_loss",
        )
        with get_sqlite_connection(sqlite_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_drift_log (date, worst_feature, worst_psi, worst_status, checked_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [TODAY.isoformat(), "close_zscore", 0.35, "halt", now_ist().isoformat()],
            )
            conn.commit()
        client = TestClient(app)
        body = client.get("/api/v1/alerts/today").json()
        assert body["count"] == 3
        assert {a["alert_type"] for a in body["alerts"]} == {"pnd_block", "exit_urgent", "drift_halt"}
