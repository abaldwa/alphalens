"""
tests/unit/test_technical_router.py

A65: router-level tests for `datastore/api/routers/technical.py` (SPEC-TA-004/
005/006), previously untested (19.76% coverage, no test file). Real seeded
DuckDB fixtures via TestClient(app) — no mocks, per this repo's no-stub/
synthetic-data policy. Uses real high-ADTV tickers from the live
`config/nifty500_universe.csv` (same pattern as test_valuation_router.py) so
`filter_recommendable` (config/training_universe.py) doesn't drop every seeded
row — it keys off that CSV's real adtv_cr column, not computed ADTV.

Deliberately NOT covered here: /screener/run/{template_name} and
/screener/custom (both delegate to ScreenerEngine, which reads the real
Parquet feature store directly rather than a monkeypatchable path — exercising
them meaningfully would require writing a full 94-column Parquet fixture,
out of scope for this pass) and get_ops_scheduler_resources-style
subprocess/systemctl-dependent code (none in this router). market_overview
and watchlist/daily are covered since they only need ohlcv_adjusted + the
real universe CSV.
"""

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from config.universe import load_universe_raw
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import technical as technical_router
from datastore.api.utils import feature_store as feature_store_module
from datastore.schema import create_normalised, create_signals
from systems.technical_analysis.alerts import alert_store as alert_store_module

_REAL_UNIVERSE = load_universe_raw()
_HIGH_ADTV = _REAL_UNIVERSE.sort_values("adtv_cr", ascending=False)
_TICKER_A = str(_HIGH_ADTV.iloc[0]["ticker"])
_TICKER_B = str(_HIGH_ADTV.iloc[1]["ticker"])
_UNKNOWN_TICKER = "ZZZNOTINUNIVERSE"
assert _UNKNOWN_TICKER not in set(_REAL_UNIVERSE["ticker"])


@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(technical_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(technical_router, "SIGNALS_DUCKDB_PATH", signals_path)
    # alert_store and daily_alert_checker (used by /user-alerts and
    # /user-alerts/check-triggers) each import SIGNALS_DUCKDB_PATH into
    # their own module namespace at import time — patching only the
    # router's copy would leave alert_store writing to the real production
    # signals DuckDB file. Same reasoning as test_valuation_router.py's
    # dual router+engine patch.
    monkeypatch.setattr(alert_store_module, "SIGNALS_DUCKDB_PATH", signals_path)
    # /{ticker}/indicators and /{ticker}/patterns read the real production
    # Parquet feature store by default — point at an empty tmp dir so
    # "no feature data" tests don't accidentally see real production rows.
    monkeypatch.setattr(feature_store_module, "FEATURES_DAILY_DIR", tmp_path / "features_daily_empty")

    return TestClient(app)


def _seed_ohlcv(db_path, ticker, rows):
    """rows: list of (date_str, high, low, close) or (date_str, close) tuples."""
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for r in rows:
            if len(r) == 4:
                d, hi, lo, close = r
            else:
                d, close = r
                hi = lo = close
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [ticker, d, close, hi, lo, close, 100000],
            )


def _create_ta_signals_table(db_path):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ta_signals (
                date DATE NOT NULL, ticker VARCHAR NOT NULL, template_name VARCHAR NOT NULL,
                category VARCHAR NOT NULL, score FLOAT NOT NULL,
                matched_conditions INTEGER NOT NULL, total_conditions INTEGER NOT NULL,
                key_values VARCHAR,
                PRIMARY KEY (date, ticker, template_name)
            )
            """
        )


def _seed_ta_signal(db_path, *, d, ticker, template_name="A1", category="A", score=1.0,
                     matched=3, total=3, key_values=None):
    import json

    _create_ta_signals_table(db_path)
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO ta_signals (date, ticker, template_name, category, score,
                matched_conditions, total_conditions, key_values)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [d, ticker, template_name, category, score, matched, total,
             json.dumps(key_values) if key_values else None],
        )


class TestScreenerTemplates:
    def test_lists_all_templates(self, client):
        r = client.get("/api/v1/ta/screener/templates")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == len(body["templates"])
        assert body["count"] > 0
        names = {t["name"] for t in body["templates"]}
        assert "A1" in names

    def test_unknown_template_run_returns_404(self, client):
        r = client.get("/api/v1/ta/screener/run/NOT_A_TEMPLATE")
        assert r.status_code == 404


class TestAlertsToday:
    def test_no_ta_signals_table_returns_empty(self, client):
        r = client.get("/api/v1/ta/alerts/today")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 0
        assert body["rows"] == []

    def test_seeded_signal_for_high_adtv_ticker_is_returned(self, client):
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            key_values={"rsi_14": 25.5},
        )
        r = client.get("/api/v1/ta/alerts/today")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["as_of_date"] == "2026-06-01"
        assert body["count"] == 1
        assert body["rows"][0]["ticker"] == _TICKER_A
        assert body["rows"][0]["key_values"]["rsi_14"] == pytest.approx(25.5)

    def test_explicit_date_overrides_latest(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A)
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-02", ticker=_TICKER_B)
        r = client.get("/api/v1/ta/alerts/today", params={"date": "2026-06-01"})
        assert r.status_code == 200
        body = r.json()
        assert body["as_of_date"] == "2026-06-01"
        assert body["count"] == 1
        assert body["rows"][0]["ticker"] == _TICKER_A

    def test_category_filter(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A, category="A")
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_B,
            template_name="B1", category="B",
        )
        r = client.get("/api/v1/ta/alerts/today", params={"category": "B"})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["rows"][0]["category"] == "B"

    def test_low_adtv_ticker_excluded_by_recommendation_filter(self, client):
        low_adtv_ticker = str(
            _REAL_UNIVERSE.sort_values("adtv_cr").iloc[0]["ticker"]
        )
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=low_adtv_ticker)
        r = client.get("/api/v1/ta/alerts/today")
        assert r.status_code == 200
        assert r.json()["count"] == 0


class TestAlertsForTicker:
    def test_no_table_returns_empty(self, client):
        r = client.get(f"/api/v1/ta/alerts/{_TICKER_A}")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_ticker_lowercased_normalised(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A)
        r = client.get(f"/api/v1/ta/alerts/{_TICKER_A.lower()}")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["rows"][0]["ticker"] == _TICKER_A

    def test_explicit_date_with_no_match_returns_empty(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A)
        r = client.get(f"/api/v1/ta/alerts/{_TICKER_A}", params={"date": "2025-01-01"})
        assert r.status_code == 200
        assert r.json()["count"] == 0


class TestWatchlistDaily:
    def test_no_ta_signals_table_returns_empty(self, client):
        r = client.get("/api/v1/ta/watchlist/daily")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_seeded_signal_with_ohlcv_produces_row_with_levels(self, client):
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            template_name="A1",
        )
        base = date(2026, 1, 1)
        rows = []
        for i in range(60):
            d = (base + timedelta(days=i)).isoformat()
            price = 100.0 + i * 0.5
            rows.append((d, price + 2, price - 2, price))
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_A, rows)

        r = client.get("/api/v1/ta/watchlist/daily", params={"date": "2026-06-01"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["count"] == 1
        row = body["rows"][0]
        assert row["ticker"] == _TICKER_A
        assert row["current_price"] is not None

    def test_pools_recommendations_across_trailing_window_dedup_per_ticker(self, client):
        # Ticker A recommended earlier in the week (2026-06-01) and again
        # today (2026-06-03, higher score) — expect one row, taken from the
        # higher-scoring occurrence, with recommended_price sourced from
        # THAT occurrence's own date, not today's.
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            template_name="A1", score=0.5, matched=1, total=2,
        )
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-03", ticker=_TICKER_A,
            template_name="A2", score=1.0, matched=2, total=2,
        )
        # Ticker B only recommended earlier in the window (2026-06-02) —
        # should still surface even though it has no signal today.
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-02", ticker=_TICKER_B,
            template_name="B1", category="B", score=1.0, matched=1, total=1,
        )

        base = date(2026, 1, 1)
        rows = []
        for i in range(160):
            d = (base + timedelta(days=i)).isoformat()
            price = 100.0 + i * 0.5
            rows.append((d, price + 2, price - 2, price))
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_A, rows)
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_B, rows)

        r = client.get("/api/v1/ta/watchlist/daily", params={"date": "2026-06-03", "lookback_days": 5})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["count"] == 2

        by_ticker = {row["ticker"]: row for row in body["rows"]}
        row_a = by_ticker[_TICKER_A]
        assert row_a["recommendation_date"] == "2026-06-03"
        assert row_a["template_name"] == "A2"
        assert row_a["recommended_price"] == row_a["current_price"]

        row_b = by_ticker[_TICKER_B]
        assert row_b["recommendation_date"] == "2026-06-02"
        # Recommended earlier in the window at a lower price than today's —
        # recommended_price and current_price must diverge.
        assert row_b["recommended_price"] < row_b["current_price"]


class TestConsensusDaily:
    def test_no_ta_signals_table_returns_empty(self, client):
        r = client.get("/api/v1/ta/consensus/daily")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_multi_strategy_ticker_ranked_first(self, client):
        # _TICKER_A fires 3 templates on 2026-06-01, _TICKER_B fires only 1.
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            template_name="A1", category="A", score=1.0,
        )
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            template_name="B1", category="B", score=0.8,
        )
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A,
            template_name="C1", category="C", score=1.0,
        )
        _seed_ta_signal(
            technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_B,
            template_name="A1", category="A", score=1.0,
        )
        r = client.get("/api/v1/ta/consensus/daily", params={"date": "2026-06-01"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["date"] == "2026-06-01"
        assert body["count"] == 2
        top = body["rows"][0]
        assert top["ticker"] == _TICKER_A
        assert top["strategy_count"] == 3
        assert set(top["template_names"]) == {"A1", "B1", "C1"}
        assert set(top["categories"]) == {"A", "B", "C"}
        assert top["avg_score"] == pytest.approx((1.0 + 0.8 + 1.0) / 3, abs=1e-4)
        assert body["rows"][1]["ticker"] == _TICKER_B
        assert body["rows"][1]["strategy_count"] == 1

    def test_explicit_date_with_no_match_returns_empty(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A)
        r = client.get("/api/v1/ta/consensus/daily", params={"date": "2025-01-01"})
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_limit_param_respected(self, client):
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_A)
        _seed_ta_signal(technical_router.SIGNALS_DUCKDB_PATH, d="2026-06-01", ticker=_TICKER_B)
        r = client.get("/api/v1/ta/consensus/daily", params={"date": "2026-06-01", "limit": 1})
        assert r.status_code == 200
        assert r.json()["count"] == 1


class TestUserAlerts:
    def test_create_list_and_delete_alert(self, client):
        r = client.get("/api/v1/ta/user-alerts")
        assert r.status_code == 200
        assert r.json()["count"] == 0

        r = client.post(
            "/api/v1/ta/user-alerts",
            json={"ticker": _TICKER_A.lower(), "template_name": "A1"},
        )
        assert r.status_code == 200, r.text
        created = r.json()
        assert created["ticker"] == _TICKER_A
        assert created["active"] is True
        alert_id = created["alert_id"]

        r = client.get("/api/v1/ta/user-alerts")
        assert r.json()["count"] == 1

        r = client.delete(f"/api/v1/ta/user-alerts/{alert_id}")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

        r = client.delete(f"/api/v1/ta/user-alerts/{alert_id}")
        assert r.status_code == 404

    def test_create_alert_with_unknown_template_is_400(self, client):
        r = client.post(
            "/api/v1/ta/user-alerts",
            json={"ticker": _TICKER_A, "template_name": "NOT_REAL"},
        )
        assert r.status_code == 400


class TestSignalsWrite:
    def test_writes_rows_and_readable_via_alerts_today(self, client):
        r = client.post(
            "/api/v1/ta/signals/write",
            json={"rows": [
                {
                    "date": "2026-06-01", "ticker": _TICKER_A, "template_name": "A1",
                    "category": "A", "score": 1.0, "matched_conditions": 3, "total_conditions": 3,
                    "key_values": {"rsi_14": 22.1},
                },
            ]},
        )
        assert r.status_code == 200
        assert r.json()["written"] == 1

        r = client.get("/api/v1/ta/alerts/today")
        assert r.json()["count"] == 1

    def test_empty_rows_writes_nothing(self, client):
        r = client.post("/api/v1/ta/signals/write", json={"rows": []})
        assert r.status_code == 200
        assert r.json()["written"] == 0


class TestCheckUserAlertTriggers:
    def test_check_triggers_no_alerts_returns_empty_list(self, client):
        r = client.post(
            "/api/v1/ta/user-alerts/check-triggers", json={"date": "2026-06-01"}
        )
        assert r.status_code == 200
        assert r.json()["newly_triggered"] == []


class TestIndicatorsAndPatterns:
    def test_indicators_no_feature_data_unavailable(self, client):
        r = client.get(f"/api/v1/ta/{_TICKER_A}/indicators", params={"date": "2026-06-01"})
        assert r.status_code == 200
        body = r.json()
        assert body["available"] is False

    def test_patterns_no_feature_data_unavailable(self, client):
        r = client.get(f"/api/v1/ta/{_TICKER_A}/patterns", params={"date": "2026-06-01"})
        assert r.status_code == 200
        assert r.json()["available"] is False


class TestCompare:
    def test_single_ticker_no_correlation_matrix(self, client):
        r = client.get("/api/v1/ta/compare", params={"tickers": _TICKER_A})
        assert r.status_code == 200
        body = r.json()
        assert body["correlation"] == {}

    def test_two_tickers_with_ohlcv_produce_correlation_matrix(self, client):
        base = date(2026, 1, 1)
        rows_a, rows_b = [], []
        for i in range(40):
            d = (base + timedelta(days=i)).isoformat()
            rows_a.append((d, 100.0 + i))
            rows_b.append((d, 200.0 + i * 2))
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_A, rows_a)
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_B, rows_b)

        r = client.get(
            "/api/v1/ta/compare",
            params={"tickers": f"{_TICKER_A},{_TICKER_B}", "days": 500},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert _TICKER_A in body["correlation"]
        assert _TICKER_B in body["correlation"][_TICKER_A]


class TestMarketOverview:
    def test_no_ohlcv_data_unavailable(self, client):
        r = client.get("/api/v1/ta/market_overview")
        assert r.status_code == 200
        assert r.json()["available"] is False

    def test_two_days_ohlcv_produces_breadth(self, client):
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_A, [
            ("2026-06-01", 100.0), ("2026-06-02", 105.0),
        ])
        _seed_ohlcv(technical_router.DUCKDB_PATH, _TICKER_B, [
            ("2026-06-01", 200.0), ("2026-06-02", 195.0),
        ])
        r = client.get("/api/v1/ta/market_overview")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["available"] is True
        assert body["advances"] == 1
        assert body["declines"] == 1
        assert len(body["sector_breadth"]) >= 1
