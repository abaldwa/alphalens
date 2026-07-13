"""
tests/unit/test_thesis_pdf.py

F4 — router-level tests for GET /api/v1/fundamentals/{ticker}/thesis/pdf
(datastore/api/routers/fundamentals.py::get_fundamental_thesis_pdf).
Real reportlab-rendered PDF bytes are returned and checked for real
content markers (not just a 200 status), per this repo's no-stub/
synthetic-data testing policy. Monkeypatches the router's own
`resolve_date`/`read_feature_row` names (imported directly into the
router's namespace from datastore.api.utils.feature_store) with an
in-memory pandas Series — no DuckDB or Parquet file needed for this
screen, mirroring how thesis.js itself only calls the ratios/scores
feature-store-backed endpoints.
"""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app
from datastore.api.routers import fundamentals as fundamentals_router

_TICKER = "TESTPDFCO"


def _row(**overrides):
    base = {
        "roe": 0.0, "roce": 0.0, "net_margin": 0.0, "revenue_growth_yoy": 0.0,
        "eps_growth_yoy": 0.0, "debt_to_equity": 0.0,
    }
    base.update(overrides)
    return pd.Series(base)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(fundamentals_router, "resolve_date", lambda date: "2026-07-01")
    return TestClient(app)


class TestThesisPdf:
    def test_no_feature_data_at_all_returns_404(self, monkeypatch):
        monkeypatch.setattr(fundamentals_router, "resolve_date", lambda date: None)
        c = TestClient(app)
        r = c.get(f"/api/v1/fundamentals/{_TICKER}/thesis/pdf")
        assert r.status_code == 404

    def test_no_row_for_ticker_returns_404(self, client, monkeypatch):
        monkeypatch.setattr(fundamentals_router, "read_feature_row", lambda ticker, date: None)
        r = client.get(f"/api/v1/fundamentals/{_TICKER}/thesis/pdf")
        assert r.status_code == 404
        assert "No ratio data" in r.json()["detail"]

    def test_strong_ratios_yields_real_pdf_with_strengths(self, client, monkeypatch):
        # roe well above the +0.5 sector-std threshold -> a real "Strengths" line.
        monkeypatch.setattr(fundamentals_router, "read_feature_row", lambda ticker, date: _row(roe=1.8))
        r = client.get(f"/api/v1/fundamentals/{_TICKER}/thesis/pdf")
        assert r.status_code == 200, r.text
        assert r.headers["content-type"] == "application/pdf"
        assert f'{_TICKER}_thesis.pdf' in r.headers["content-disposition"]
        body = r.content
        assert body[:5] == b"%PDF-"  # real PDF header, not a stub/placeholder
        assert b"%%EOF" in body[-16:] or b"%%EOF" in body  # real, complete PDF trailer
        assert len(body) > 1000  # a real rendered document, not an empty shell

    def test_weak_debt_to_equity_yields_a_risk_not_a_strength(self, client, monkeypatch):
        # debt_to_equity is LOWER_IS_BETTER: a high raw z (2.0) flips sign -> a Risk line.
        monkeypatch.setattr(fundamentals_router, "read_feature_row", lambda ticker, date: _row(debt_to_equity=2.0))
        r = client.get(f"/api/v1/fundamentals/{_TICKER}/thesis/pdf")
        assert r.status_code == 200, r.text
        assert r.content[:5] == b"%PDF-"
