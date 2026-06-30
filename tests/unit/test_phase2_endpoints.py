"""
tests/unit/test_phase2_endpoints.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-DS-001 through SPEC-DS-004, SPEC-PIPE-003 (CRITICAL), SPEC-UI-003
Owner: Platform / QA
Consumers: CI, pytest

Tests the P2.6 build prompt's literal new endpoints against the real
FastAPI app and real on-disk DuckDB files (same pattern as
test_shareholding_api.py): GET /api/v1/fundamentals/{ticker}/history,
GET /api/v1/governance/{ticker}, GET/POST /api/v1/signals/ml/forensic/*,
GET/POST /api/v1/signals/ml/multibagger/*, GET /api/v1/watchlist/current.
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import forensic as forensic_router
from datastore.api.routers import fundamentals as fundamentals_router
from datastore.api.routers import governance as governance_router
from datastore.api.routers import multibagger as multibagger_router
from datastore.api.routers import shareholding as shareholding_router
from datastore.api.routers import watchlist as watchlist_router
from datastore.schema import create_normalised, create_signals


@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(fundamentals_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(shareholding_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(governance_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(forensic_router, "SIGNALS_DUCKDB_PATH", signals_path)
    monkeypatch.setattr(multibagger_router, "SIGNALS_DUCKDB_PATH", signals_path)
    monkeypatch.setattr(watchlist_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app)


class TestFundamentalsHistory:
    def test_returns_most_recent_n_quarters_not_a_date_range(self, client):
        for fy, q, qed, ad, rev in [
            (2024, 1, "2024-06-30", "2024-08-14", 100.0),
            (2024, 2, "2024-09-30", "2024-11-14", 110.0),
            (2024, 3, "2024-12-31", "2025-02-14", 120.0),
            (2024, 4, "2025-03-31", "2025-05-15", 130.0),
        ]:
            r = client.post(
                "/api/v1/fundamentals/write",
                json={"ticker": "TESTCO", "fiscal_year": fy, "quarter": q,
                      "quarter_end_date": qed, "announcement_date": ad, "revenue": rev},
            )
            assert r.status_code == 200, r.text

        response = client.get("/api/v1/fundamentals/TESTCO/history", params={"quarters": 2})
        rows = response.json()["data"]
        assert len(rows) == 2
        assert [r["revenue"] for r in rows] == [120.0, 130.0]  # ascending by announcement_date

    def test_pit_filters_by_announcement_date(self, client):
        client.post(
            "/api/v1/fundamentals/write",
            json={"ticker": "FUTURECO", "fiscal_year": 2026, "quarter": 1,
                  "quarter_end_date": "2026-06-30", "announcement_date": "2026-08-14", "revenue": 999.0},
        )
        response = client.get(
            "/api/v1/fundamentals/FUTURECO/history",
            params={"quarters": 5, "as_of": "2026-01-01"},
        )
        assert response.json()["data"] == []


class TestFundamentalsWriteCoalesce:
    """[AS BUILT, P2.6] tijori.py and screener.py both write the same
    (ticker, fiscal_year, quarter) row — a NULL in one writer's payload
    must not clobber the other writer's previously-written value."""

    def test_partial_write_does_not_null_out_other_columns(self, client):
        client.post(
            "/api/v1/fundamentals/write",
            json={"ticker": "COALESCE_CO", "fiscal_year": 2026, "quarter": 1,
                  "quarter_end_date": "2026-06-30", "announcement_date": "2026-08-14", "revenue": 500.0},
        )
        # tijori.py-style partial write: only sector_specific_metric_1 set, everything else NULL
        r = client.post(
            "/api/v1/fundamentals/write",
            json={"ticker": "COALESCE_CO", "fiscal_year": 2026, "quarter": 1,
                  "quarter_end_date": "2026-06-30", "announcement_date": "2026-08-14",
                  "sector_specific_metric_1": 185.5},
        )
        assert r.status_code == 200

        response = client.get(
            "/api/v1/fundamentals/COALESCE_CO/history", params={"quarters": 1, "as_of": "2026-08-14"}
        )
        row = response.json()["data"][0]
        assert row["revenue"] == 500.0  # NOT clobbered by the second, partial write
        assert row["sector_specific_metric_1"] == 185.5


class TestGovernanceEndpoint:
    def test_reads_shareholding_table_under_governance_path(self, client):
        """`shareholding` IS the governance store — see governance.py's module docstring."""
        client.post(
            "/api/v1/shareholding/write",
            json={"ticker": "GOVCO", "quarter_end_date": "2025-03-31", "filing_date": "2025-04-21",
                  "promoter_pct": 55.0, "superstar_flag": True, "superstar_change": 0.4},
        )

        response = client.get(
            "/api/v1/governance/GOVCO",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-21"},
        )
        rows = response.json()["data"]
        assert len(rows) == 1
        assert rows[0]["promoter_pct"] == 55.0
        assert rows[0]["superstar_flag"] is True
        assert rows[0]["superstar_change"] == 0.4

    def test_pit_filtered_by_filing_date(self, client):
        client.post(
            "/api/v1/shareholding/write",
            json={"ticker": "FUTUREGOV", "quarter_end_date": "2026-06-30", "filing_date": "2026-07-21",
                  "promoter_pct": 60.0},
        )
        response = client.get("/api/v1/governance/FUTUREGOV", params={"as_of": "2026-01-01"})
        assert response.json()["data"] == []

    def test_partial_write_does_not_null_out_other_columns(self, client):
        """trendlyne.py-style partial write must not clobber screener.py's promoter_pct."""
        client.post(
            "/api/v1/shareholding/write",
            json={"ticker": "GOVCOALESCE", "quarter_end_date": "2025-03-31", "filing_date": "2025-04-21",
                  "promoter_pct": 48.0},
        )
        r = client.post(
            "/api/v1/shareholding/write",
            json={"ticker": "GOVCOALESCE", "quarter_end_date": "2025-03-31", "filing_date": "2025-04-21",
                  "superstar_flag": True, "superstar_change": 0.2},
        )
        assert r.status_code == 200

        response = client.get(
            "/api/v1/governance/GOVCOALESCE",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-21"},
        )
        row = response.json()["data"][0]
        assert row["promoter_pct"] == 48.0
        assert row["superstar_flag"] is True


class TestForensicEndpoint:
    def test_write_then_read_round_trip(self, client):
        r = client.post(
            "/api/v1/signals/ml/forensic/write",
            json={"date": "2026-06-01", "ticker": "FRAUDCO", "beneish_m": 0.5,
                  "forensic_composite": 68.0, "forensic_flag": True, "forensic_flag_label": "red"},
        )
        assert r.status_code == 200, r.text

        response = client.get("/api/v1/signals/ml/forensic/FRAUDCO")
        body = response.json()
        assert body["forensic_composite"] == 68.0
        assert body["forensic_flag_label"] == "red"

    def test_unknown_ticker_returns_none_not_404(self, client):
        response = client.get("/api/v1/signals/ml/forensic/NOPE")
        assert response.status_code == 200
        assert response.json() is None

    def test_as_of_returns_most_recent_row_at_or_before(self, client):
        for d, composite in [("2026-03-01", 30.0), ("2026-06-01", 70.0)]:
            client.post(
                "/api/v1/signals/ml/forensic/write",
                json={"date": d, "ticker": "MULTIDATE", "forensic_composite": composite},
            )
        response = client.get("/api/v1/signals/ml/forensic/MULTIDATE", params={"as_of": "2026-04-01"})
        assert response.json()["forensic_composite"] == 30.0

    def test_route_not_swallowed_by_signals_ml_ticker_date_wildcard(self, client):
        """Regression test for the route-ordering bug caught while wiring
        this router into main.py — see main.py's module comment."""
        response = client.get("/api/v1/signals/ml/forensic/ANYCO")
        assert response.status_code == 200  # not a 422 date-validation error


class TestMultibaggerEndpoint:
    def test_write_then_read_round_trip(self, client):
        r = client.post(
            "/api/v1/signals/ml/multibagger/write",
            json={"date": "2026-06-01", "ticker": "MBCO", "mb_probability": 0.65,
                  "mb_tier": "3x", "survival_18m": 0.7},
        )
        assert r.status_code == 200, r.text

        response = client.get("/api/v1/signals/ml/multibagger/MBCO")
        body = response.json()
        assert body["mb_probability"] == 0.65
        assert body["survival_18m"] == 0.7

    def test_unknown_ticker_returns_none(self, client):
        response = client.get("/api/v1/signals/ml/multibagger/NOPE")
        assert response.json() is None


class TestWatchlistCurrent:
    def test_empty_table_returns_honest_stub(self, client):
        response = client.get("/api/v1/watchlist/current")
        body = response.json()
        assert body["implemented"] is False
        assert body["tickers"] == []

    def test_top_n_ranked_by_probability_from_latest_date(self, client):
        rows = [
            ("2026-06-01", "LOWCO", 0.20), ("2026-06-01", "HIGHCO", 0.80),
            ("2026-05-01", "STALECO", 0.99),  # older date — must be excluded
        ]
        for d, ticker, prob in rows:
            client.post(
                "/api/v1/signals/ml/multibagger/write",
                json={"date": d, "ticker": ticker, "mb_probability": prob},
            )

        response = client.get("/api/v1/watchlist/current")
        body = response.json()
        assert body["implemented"] is True
        tickers = [t["ticker"] for t in body["tickers"]]
        assert tickers == ["HIGHCO", "LOWCO"]  # descending by mb_probability, latest date only


def test_close_all_connections_after_module():
    close_all_connections()
