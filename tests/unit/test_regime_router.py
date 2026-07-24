"""
tests/unit/test_regime_router.py

A65: router-level tests for `datastore/api/routers/regime.py` (SPEC-DS-002/
SPEC-DS-003), previously untested (57.69% coverage, no test file). Real
seeded DuckDB (ml_signals, 'hmm_market'/'MARKET' sentinel rows) via
TestClient(app) — no mocks.
"""

from datetime import date

from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import regime as regime_router
from datastore.schema import create_normalised, create_signals


def _seed(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=duckdb_path)
    close_all_connections()
    monkeypatch.setattr(regime_router, "SIGNALS_DUCKDB_PATH", duckdb_path)
    return duckdb_path


def _seed_market_regimes_db(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=duckdb_path)
    close_all_connections()
    monkeypatch.setattr(regime_router, "DUCKDB_PATH", duckdb_path)
    return duckdb_path


def _insert_market_regime(
    db_path, regime, start_date, end_date, confirmed_date, move_pct, index_name="Nifty 500", method=None
):
    # Default method matches GET /api/v1/macro/market_regimes' own default
    # (METHOD_NAME, the 20% threshold) so tests that don't care about the
    # multi-threshold `method` filter still get their rows back without
    # passing `method` explicitly on every request.
    from systems.regime.market_regime import METHOD_NAME as _DEFAULT_METHOD

    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO market_regimes (index_name, regime, start_date, end_date, confirmed_date, method, move_pct) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [index_name, regime, start_date, end_date, confirmed_date, method or _DEFAULT_METHOD, move_pct],
        )


def _insert_regime(db_path, d, regime, prob, stability):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO ml_signals (date, ticker, model_name, model_version, hmm_regime, "
            "hmm_regime_prob, hmm_stability) VALUES (?, 'MARKET', 'hmm_market', 'v1', ?, ?, ?)",
            [d, regime, prob, stability],
        )


class TestGetRegime:
    def test_no_data_returns_unavailable(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime")
        assert resp.status_code == 200
        body = resp.json()
        assert body["available"] is False

    def test_returns_latest_when_no_as_of(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime")
        assert resp.status_code == 200
        body = resp.json()
        assert body["available"] is True
        assert body["date"] == "2026-06-05T00:00:00"
        assert body["hmm_regime"] == "bear"

    def test_as_of_returns_pit_correct_row(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime", params={"as_of": "2026-06-03"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-06-01T00:00:00"
        assert body["hmm_regime"] == "bull"

    def test_as_of_before_any_data_returns_unavailable(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime", params={"as_of": "2026-05-01"})
        assert resp.status_code == 200
        assert resp.json()["available"] is False


class TestGetRegimeHistory:
    def test_no_data_returns_empty_list(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history")
        assert resp.status_code == 200
        assert resp.json()["days"] == []

    def test_returns_ascending_by_date(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        _insert_regime(db_path, date(2026, 6, 5), "bear", 0.6, 0.8)
        _insert_regime(db_path, date(2026, 6, 1), "bull", 0.7, 0.9)
        _insert_regime(db_path, date(2026, 6, 3), "neutral", 0.5, 0.85)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history")
        assert resp.status_code == 200
        days = resp.json()["days"]
        assert [d["date"] for d in days] == ["2026-06-01T00:00:00", "2026-06-03T00:00:00", "2026-06-05T00:00:00"]
        assert [d["hmm_regime"] for d in days] == ["bull", "neutral", "bear"]

    def test_days_param_limits_and_still_returns_most_recent(self, tmp_path, monkeypatch):
        db_path = _seed(tmp_path, monkeypatch)
        for i, r in enumerate(["bull", "bear", "neutral", "bull"]):
            _insert_regime(db_path, date(2026, 6, 1 + i), r, 0.6, 0.8)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history", params={"days": 2})
        assert resp.status_code == 200
        days = resp.json()["days"]
        assert len(days) == 2
        assert [d["date"] for d in days] == ["2026-06-03T00:00:00", "2026-06-04T00:00:00"]

    def test_days_param_out_of_range_returns_422(self, tmp_path, monkeypatch):
        _seed(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/regime/history", params={"days": 0})
        assert resp.status_code == 422


class TestGetMarketRegimes:
    def test_no_data_returns_empty_segments(self, tmp_path, monkeypatch):
        _seed_market_regimes_db(tmp_path, monkeypatch)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/market_regimes", params={"index_name": "Nifty 500"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["index_name"] == "Nifty 500"
        assert body["segments"] == []

    def test_returns_segments_ascending_by_start_date(self, tmp_path, monkeypatch):
        db_path = _seed_market_regimes_db(tmp_path, monkeypatch)
        _insert_market_regime(db_path, "bear", date(2020, 1, 17), date(2020, 3, 20), date(2020, 4, 17), -0.29)
        _insert_market_regime(db_path, "bull", date(2016, 2, 25), date(2020, 1, 16), date(2020, 3, 12), 0.74)
        client = TestClient(app)
        resp = client.get("/api/v1/macro/market_regimes", params={"index_name": "Nifty 500"})
        assert resp.status_code == 200
        segs = resp.json()["segments"]
        assert [s["regime"] for s in segs] == ["bull", "bear"]

    def test_as_of_excludes_segments_not_yet_confirmed(self, tmp_path, monkeypatch):
        db_path = _seed_market_regimes_db(tmp_path, monkeypatch)
        _insert_market_regime(db_path, "bull", date(2016, 2, 25), date(2020, 1, 16), date(2020, 3, 12), 0.74)
        client = TestClient(app)
        resp = client.get(
            "/api/v1/macro/market_regimes", params={"index_name": "Nifty 500", "as_of": "2018-01-01"}
        )
        assert resp.status_code == 200
        assert resp.json()["segments"] == []

        resp2 = client.get(
            "/api/v1/macro/market_regimes", params={"index_name": "Nifty 500", "as_of": "2020-06-01"}
        )
        assert len(resp2.json()["segments"]) == 1

    def test_date_range_filters_to_overlapping_segments(self, tmp_path, monkeypatch):
        db_path = _seed_market_regimes_db(tmp_path, monkeypatch)
        _insert_market_regime(db_path, "bull", date(2016, 2, 25), date(2020, 1, 16), date(2020, 3, 12), 0.74)
        _insert_market_regime(db_path, "bear", date(2020, 1, 17), date(2020, 3, 20), date(2020, 4, 17), -0.29)
        _insert_market_regime(db_path, "bull", date(2020, 3, 23), date(2026, 7, 21), date(2026, 7, 21), 2.74)
        client = TestClient(app)
        resp = client.get(
            "/api/v1/macro/market_regimes",
            params={"index_name": "Nifty 500", "start_date": "2020-02-01", "end_date": "2020-04-01"},
        )
        assert resp.status_code == 200
        segs = resp.json()["segments"]
        # The first bull segment (2016-02-25 -> 2020-01-16) ends before the
        # window starts, so only the bear and second bull segment overlap.
        assert [s["regime"] for s in segs] == ["bear", "bull"]

    def test_only_returns_segments_for_requested_index(self, tmp_path, monkeypatch):
        db_path = _seed_market_regimes_db(tmp_path, monkeypatch)
        _insert_market_regime(db_path, "bull", date(2016, 2, 25), date(2020, 1, 16), date(2020, 3, 12), 0.74)
        _insert_market_regime(
            db_path, "bear", date(2016, 2, 25), date(2020, 1, 16), date(2020, 3, 12), -0.5, index_name="Nifty 50"
        )
        client = TestClient(app)
        resp = client.get("/api/v1/macro/market_regimes", params={"index_name": "Nifty 500"})
        assert resp.status_code == 200
        segs = resp.json()["segments"]
        assert len(segs) == 1
        assert segs[0]["regime"] == "bull"

    def test_method_param_filters_to_requested_threshold(self, tmp_path, monkeypatch):
        # Same index_name, same start_date across two methods — exercises
        # the widened (index_name, method, start_date) PK: both rows must
        # persist side by side rather than one clobbering the other, and
        # `method` must correctly select between them.
        db_path = _seed_market_regimes_db(tmp_path, monkeypatch)
        _insert_market_regime(
            db_path, "bull", date(2020, 1, 1), date(2020, 6, 1), date(2020, 6, 1), 0.21, method="20pct_threshold_v1"
        )
        _insert_market_regime(
            db_path, "bear", date(2020, 1, 1), date(2020, 3, 1), date(2020, 3, 1), -0.06, method="5pct_threshold_v1"
        )
        client = TestClient(app)

        resp_20 = client.get(
            "/api/v1/macro/market_regimes", params={"index_name": "Nifty 500", "method": "20pct_threshold_v1"}
        )
        assert [s["regime"] for s in resp_20.json()["segments"]] == ["bull"]

        resp_5 = client.get(
            "/api/v1/macro/market_regimes", params={"index_name": "Nifty 500", "method": "5pct_threshold_v1"}
        )
        assert [s["regime"] for s in resp_5.json()["segments"]] == ["bear"]

        # Default (no method passed) preserves prior single-threshold behavior.
        resp_default = client.get("/api/v1/macro/market_regimes", params={"index_name": "Nifty 500"})
        assert [s["regime"] for s in resp_default.json()["segments"]] == ["bull"]
