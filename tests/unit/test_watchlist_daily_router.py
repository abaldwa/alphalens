"""
tests/unit/test_watchlist_daily_router.py

A65: router-level tests for `datastore/api/routers/watchlist.py`'s GET
/api/v1/watchlist/daily endpoint (SPEC-DS-002/SPEC-UI-003), the part of this
router not already covered by tests/unit/test_phase2_endpoints.py's
TestWatchlistCurrent (which only exercises /current). Real seeded DuckDB
fixtures + the real `config/nifty500_universe.csv` (same pattern as
test_technical_router.py, since `filter_recommendable` keys off its real
adtv_cr column) via TestClient(app) — no mocks.
"""

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from config.universe import load_universe_raw
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import watchlist as watchlist_router
from datastore.api.utils import feature_store as feature_store_module
from datastore.schema import create_normalised, create_signals

_REAL_UNIVERSE = load_universe_raw()
_HIGH_ADTV = _REAL_UNIVERSE.sort_values("adtv_cr", ascending=False)
_TICKER_A = str(_HIGH_ADTV.iloc[0]["ticker"])
_TICKER_B = str(_HIGH_ADTV.iloc[1]["ticker"])
_TICKER_C = str(_HIGH_ADTV.iloc[2]["ticker"])


def _client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()
    monkeypatch.setattr(watchlist_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(watchlist_router, "SIGNALS_DUCKDB_PATH", signals_path)
    monkeypatch.setattr(feature_store_module, "FEATURES_DAILY_DIR", tmp_path / "features_daily_empty")
    return TestClient(app), normalised_path, signals_path


def _insert_signal(db_path, **kwargs):
    cols = ", ".join(kwargs.keys())
    placeholders = ", ".join(["?"] * len(kwargs))
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(f"INSERT INTO ml_signals ({cols}) VALUES ({placeholders})", list(kwargs.values()))


def _insert_price(db_path, ticker, d, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [ticker, d, close, close, close, close, 1_000_000],
        )


class TestBuildPriceMap:
    """REV26 (2026-07-21 review): a NaN `close` (as fetchdf() would surface
    a NULL float, were ohlcv_adjusted.close ever not schema-NOT-NULL) must
    be cast to None before reaching DailyWatchlistRow's float fields."""

    def test_nan_close_becomes_none(self):
        df = pd.DataFrame({"ticker": ["A", "B"], "close": [100.0, np.nan]})
        price_map = watchlist_router._build_price_map(df)
        assert price_map == {"A": 100.0, "B": None}

    def test_empty_df_returns_empty_map(self):
        df = pd.DataFrame({"ticker": [], "close": []})
        assert watchlist_router._build_price_map(df) == {}


class TestDailyWatchlist:
    def test_no_signals_returns_empty_response(self, tmp_path, monkeypatch):
        client, _, _ = _client(tmp_path, monkeypatch)
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert body["count"] == 0

    def test_quantile_target_computed_from_q50_return(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_A, model_name="signal_5d",
            model_version="v1", buy_prob=0.75, signal_direction="buy",
            q10_return=-0.02, q50_return=0.05, q90_return=0.12,
        )
        _insert_price(normalised_path, _TICKER_A, "2026-06-01", 100.0)
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        row = body["rows"][0]
        assert row["ticker"] == _TICKER_A
        assert row["horizon"] == "5d"
        assert row["target_basis"] == "quantile"
        assert row["target_price"] == 105.0
        assert row["target_low"] == 98.0
        assert row["target_high"] == 112.0
        assert row["expected_return_pct"] == 5.0

    def test_pnd_blocked_ticker_excluded(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_A, model_name="signal_5d",
            model_version="v1", buy_prob=0.90, signal_direction="buy", q50_return=0.05,
        )
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_A, model_name="pnd_detector",
            model_version="v1", pnd_block=True,
        )
        _insert_price(normalised_path, _TICKER_A, "2026-06-01", 100.0)
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []

    def test_missing_price_gives_unavailable_basis(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_A, model_name="signal_5d",
            model_version="v1", buy_prob=0.80, signal_direction="buy", q50_return=0.05,
        )
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["target_basis"] == "unavailable"
        assert row["current_price"] is None
        assert row["target_price"] is None

    def test_no_quantile_and_no_feature_row_gives_unavailable_basis(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_A, model_name="signal_5d",
            model_version="v1", buy_prob=0.80, signal_direction="buy",
        )
        _insert_price(normalised_path, _TICKER_A, "2026-06-01", 100.0)
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["target_basis"] == "unavailable"
        assert row["current_price"] == 100.0

    def test_explicit_date_param_used_instead_of_latest(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        _insert_signal(
            signals_path, date="2026-05-01", ticker=_TICKER_A, model_name="signal_5d",
            model_version="v1", buy_prob=0.70, signal_direction="buy", q50_return=0.03,
        )
        _insert_signal(
            signals_path, date="2026-06-01", ticker=_TICKER_B, model_name="signal_5d",
            model_version="v1", buy_prob=0.90, signal_direction="buy", q50_return=0.05,
        )
        _insert_price(normalised_path, _TICKER_A, "2026-05-01", 50.0)
        _insert_price(normalised_path, _TICKER_B, "2026-06-01", 100.0)
        resp = client.get("/api/v1/watchlist/daily", params={"date": "2026-05-01"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-05-01"
        tickers = [r["ticker"] for r in body["rows"]]
        assert tickers == [_TICKER_A]

    def test_n_per_horizon_limits_results(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        for t, prob in [(_TICKER_A, 0.95), (_TICKER_B, 0.85), (_TICKER_C, 0.75)]:
            _insert_signal(
                signals_path, date="2026-06-01", ticker=t, model_name="signal_5d",
                model_version="v1", buy_prob=prob, signal_direction="buy", q50_return=0.05,
            )
            _insert_price(normalised_path, t, "2026-06-01", 100.0)
        resp = client.get("/api/v1/watchlist/daily", params={"n_per_horizon": 1})
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["rows"]) == 1
        assert body["rows"][0]["ticker"] == _TICKER_A

    def test_multibagger_data_included_alongside_signal_rows(self, tmp_path, monkeypatch):
        client, normalised_path, signals_path = _client(tmp_path, monkeypatch)
        with get_duckdb_connection(signals_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ml_multibagger (date, ticker, mb_probability) VALUES (?, ?, ?)",
                ["2026-06-01", _TICKER_A, 0.65],
            )
        resp = client.get("/api/v1/watchlist/daily")
        assert resp.status_code == 200
        body = resp.json()
        assert any(m["ticker"] == _TICKER_A for m in body["multibagger"])
