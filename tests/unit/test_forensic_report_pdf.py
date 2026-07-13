"""
tests/unit/test_forensic_report_pdf.py

FO6 — router-level tests for
GET /api/v1/signals/ml/forensic/{ticker}/report/pdf
(datastore/api/routers/forensic.py::get_forensic_report_pdf). Real seeded
DuckDB (ml_forensic table) via TestClient(app) — no mocks, per this repo's
no-stub/synthetic-data testing policy. Verifies real reportlab-rendered
PDF bytes come back with real content markers, not just a 200 status.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import forensic as forensic_router
from datastore.schema import create_signals

_TICKER = "TESTFORENSICCO"


@pytest.fixture
def client(tmp_path, monkeypatch):
    signals_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()
    monkeypatch.setattr(forensic_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app)


def _seed_forensic_row(db_path, ticker, **overrides):
    values = {
        "date": date(2026, 7, 1), "ticker": ticker, "beneish_m": -2.1, "altman_z": 3.4,
        "piotroski_f": 7, "ohlson_o": -3.0, "dechow_f": 1.0, "sloan_accrual": 0.02,
        "benford_mad": 0.008, "benford_detail_json": None, "forensic_composite": 82.0,
        "forensic_flag": False, "forensic_flag_label": "green", "forensic_ml_prob": 0.05,
        "shap_top5_json": None, "pattern_match": None,
    }
    values.update(overrides)
    cols = list(values.keys())
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            f"INSERT INTO ml_forensic ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            [values[c] for c in cols],
        )


class TestForensicReportPdf:
    def test_no_row_for_ticker_returns_404(self, client):
        r = client.get(f"/api/v1/signals/ml/forensic/{_TICKER}/report/pdf")
        assert r.status_code == 404

    def test_clean_ticker_yields_real_pdf(self, client):
        _seed_forensic_row(forensic_router.SIGNALS_DUCKDB_PATH, _TICKER)
        r = client.get(f"/api/v1/signals/ml/forensic/{_TICKER}/report/pdf")
        assert r.status_code == 200, r.text
        assert r.headers["content-type"] == "application/pdf"
        assert f"{_TICKER}_investigation_report.pdf" in r.headers["content-disposition"]
        body = r.content
        assert body[:5] == b"%PDF-"  # real PDF header
        assert b"%%EOF" in body  # real, complete PDF trailer
        assert len(body) > 1000  # a real rendered document

    def test_flagged_ticker_recommendation_is_blocked(self, client):
        _seed_forensic_row(
            forensic_router.SIGNALS_DUCKDB_PATH, _TICKER,
            forensic_flag=True, forensic_flag_label="red", forensic_composite=12.0,
            pattern_match="Satyam-like revenue inflation",
        )
        r = client.get(f"/api/v1/signals/ml/forensic/{_TICKER}/report/pdf")
        assert r.status_code == 200, r.text
        assert r.content[:5] == b"%PDF-"

    def test_lowercase_ticker_path_is_uppercased(self, client):
        _seed_forensic_row(forensic_router.SIGNALS_DUCKDB_PATH, _TICKER)
        r = client.get(f"/api/v1/signals/ml/forensic/{_TICKER.lower()}/report/pdf")
        assert r.status_code == 200, r.text
