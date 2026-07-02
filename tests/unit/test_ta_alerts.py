"""
tests/unit/test_ta_alerts.py

Phase: 3.x (Technical Analysis User-Defined Alerts)
Specs: SPEC-TA-009
Owner: QA / Platform
Consumers: pytest CI

Unit tests for systems/technical_analysis/alerts/alert_store.py: CRUD for
user-defined alerts and the check_alerts() state-change (newly-triggered)
logic. Uses a temp-file DuckDB (monkeypatched onto alert_store's module-
level SIGNALS_DUCKDB_PATH) so no real signals.duckdb is touched — per
SPEC-SYS-006's test-fixture exemption, hand-built ta_signals rows are
inserted directly to exercise check_alerts() without needing a real
feature Parquet / ScreenerEngine run.
"""

from datetime import date

import pytest

from systems.technical_analysis.alerts import alert_store


@pytest.fixture(autouse=True)
def _isolated_signals_db(tmp_path, monkeypatch):
    """Point alert_store at a throwaway DuckDB file for every test."""
    db_path = tmp_path / "test_signals.duckdb"
    monkeypatch.setattr(alert_store, "SIGNALS_DUCKDB_PATH", db_path)
    yield db_path


def _insert_ta_signal(db_path, run_date: str, ticker: str, template_name: str, score: float = 1.0):
    """Test fixture helper: insert one ta_signals row directly (SPEC-SYS-006 exemption)."""
    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(db_path, persist=False) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ta_signals (
                date DATE NOT NULL, ticker VARCHAR NOT NULL, template_name VARCHAR NOT NULL,
                category VARCHAR NOT NULL, score FLOAT NOT NULL,
                matched_conditions INTEGER NOT NULL, total_conditions INTEGER NOT NULL,
                key_values JSON, PRIMARY KEY (date, ticker, template_name)
            )
            """
        )
        conn.execute(
            "INSERT INTO ta_signals VALUES (?, ?, ?, 'A', ?, 2, 2, NULL)",
            [run_date, ticker, template_name, score],
        )


def test_create_list_delete_alert(_isolated_signals_db):
    """SPEC-TA-009: create_alert/list_alerts/delete_alert round-trip correctly."""
    alert_id = alert_store.create_alert("reliance", "A2")
    assert isinstance(alert_id, int)

    alerts = alert_store.list_alerts()
    assert len(alerts) == 1
    assert alerts[0].ticker == "RELIANCE"  # normalised to upper
    assert alerts[0].template_name == "A2"
    assert alerts[0].active is True
    assert alerts[0].last_triggered_date is None
    assert alerts[0].triggered_today is False

    assert alert_store.delete_alert(alert_id) is True
    assert alert_store.list_alerts() == []
    # Deleting again (already inactive) reports failure, not a crash
    assert alert_store.delete_alert(alert_id) is False


def test_create_alert_rejects_unknown_template(_isolated_signals_db):
    """SPEC-TA-009: create_alert raises ValueError for a template not in TEMPLATE_MAP."""
    with pytest.raises(ValueError):
        alert_store.create_alert("RELIANCE", "NOT_A_REAL_TEMPLATE")


def test_check_alerts_detects_new_trigger_and_is_idempotent(_isolated_signals_db):
    """SPEC-TA-009: check_alerts() reports an alert as newly-triggered exactly
    once per (alert_id, date) — a full ta_signals match on day 1 triggers it,
    re-running check_alerts for the same day reports no new triggers, and
    list_alerts() reflects triggered_today/last_triggered_date correctly."""
    db_path = _isolated_signals_db
    alert_id = alert_store.create_alert("JTEKTINDIA", "A2")

    # No ta_signals rows yet — nothing to trigger
    assert alert_store.check_alerts(date(2026, 7, 1)) == []

    # A full match appears for this ticker/template on 2026-07-01
    _insert_ta_signal(db_path, "2026-07-01", "JTEKTINDIA", "A2", score=1.0)

    newly = alert_store.check_alerts(date(2026, 7, 1))
    assert newly == [alert_id]

    # Re-running the same date must not re-report it as new (idempotent)
    assert alert_store.check_alerts(date(2026, 7, 1)) == []

    alerts = alert_store.list_alerts()
    assert alerts[0].last_triggered_date == "2026-07-01"
    assert alerts[0].triggered_today is True


def test_check_alerts_ignores_partial_matches(_isolated_signals_db):
    """SPEC-TA-009: a partial match (score < 1.0) never triggers an alert —
    only full matches (mirrors DailyAlertChecker only persisting score==1.0)."""
    db_path = _isolated_signals_db
    alert_store.create_alert("INFY", "A1")
    _insert_ta_signal(db_path, "2026-07-01", "INFY", "A1", score=0.5)

    assert alert_store.check_alerts(date(2026, 7, 1)) == []
    assert alert_store.list_alerts()[0].last_triggered_date is None
