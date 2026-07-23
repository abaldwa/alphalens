"""
tests/unit/test_ta_screener_outcomes.py

Coverage for systems/technical_analysis/screener/outcomes.py — previously
untested (0% coverage). Real seeded DuckDB (ta_signals/ml_signals), real
liquidity-floor filtering via config.training_universe.filter_recommendable
(monkeypatched universe lookup, not mocked business logic).
"""

import pandas as pd
import pytest

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_signals
from systems.technical_analysis.alerts.daily_alert_checker import _CREATE_TA_SIGNALS_SQL
from systems.technical_analysis.screener import outcomes as outcomes_mod


@pytest.fixture
def signals_db(tmp_path):
    p = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=p)
    close_all_connections()
    with get_duckdb_connection(p, persist=False, read_only=False) as conn:
        conn.execute(_CREATE_TA_SIGNALS_SQL)
    return p


def _fake_universe(tickers, adtv_cr=50.0):
    return pd.DataFrame({"ticker": tickers, "adtv_cr": [adtv_cr] * len(tickers)})


class TestBuildSignalEvents:
    def test_no_ta_signals_table_returns_empty(self, tmp_path):
        empty_db = tmp_path / "empty.duckdb"
        with get_duckdb_connection(empty_db, persist=False, read_only=False) as conn:
            conn.execute("SELECT 1")  # just create the file
        events = outcomes_mod.build_signal_events(signals_db_path=empty_db)
        assert events == []

    def test_real_rows_become_signal_events_after_liquidity_filter(self, signals_db, monkeypatch):
        with get_duckdb_connection(signals_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ta_signals (date, ticker, template_name, category, score, "
                "matched_conditions, total_conditions) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["2024-01-05", "LIQUIDCO", "breakout_20d", "momentum", 1.0, 3, 3],
            )
        monkeypatch.setattr(outcomes_mod, "load_universe_raw", lambda: _fake_universe(["LIQUIDCO"]))

        events = outcomes_mod.build_signal_events(signals_db_path=signals_db)
        assert len(events) == 1
        assert events[0].ticker == "LIQUIDCO"
        assert events[0].strategy_id == "breakout_20d"
        assert events[0].direction == "long"

    def test_illiquid_ticker_filtered_out(self, signals_db, monkeypatch):
        with get_duckdb_connection(signals_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ta_signals (date, ticker, template_name, category, score, "
                "matched_conditions, total_conditions) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["2024-01-05", "ILLIQUIDCO", "breakout_20d", "momentum", 1.0, 3, 3],
            )
        monkeypatch.setattr(outcomes_mod, "load_universe_raw", lambda: _fake_universe(["ILLIQUIDCO"], adtv_cr=0.5))

        events = outcomes_mod.build_signal_events(signals_db_path=signals_db)
        assert events == []

    def test_since_filter_excludes_older_rows(self, signals_db, monkeypatch):
        with get_duckdb_connection(signals_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ta_signals (date, ticker, template_name, category, score, "
                "matched_conditions, total_conditions) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["2020-01-05", "OLDCO", "breakout_20d", "momentum", 1.0, 3, 3],
            )
        monkeypatch.setattr(outcomes_mod, "load_universe_raw", lambda: _fake_universe(["OLDCO"]))

        events = outcomes_mod.build_signal_events(signals_db_path=signals_db, since="2024-01-01")
        assert events == []


class TestLoadMarketRegime:
    def test_no_ml_signals_table_returns_empty_with_expected_columns(self, tmp_path):
        empty_db = tmp_path / "empty2.duckdb"
        with get_duckdb_connection(empty_db, persist=False, read_only=False) as conn:
            conn.execute("SELECT 1")
        df = outcomes_mod.load_market_regime(signals_db_path=empty_db)
        assert list(df.columns) == ["date", "hmm_regime"]
        assert df.empty

    def test_real_market_regime_rows_returned(self, signals_db):
        with get_duckdb_connection(signals_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO ml_signals (date, ticker, model_name, model_version, hmm_regime) VALUES (?, ?, ?, ?, ?)",
                ["2024-01-05", "MARKET", "hmm_market", "1.0", "bull"],
            )
        df = outcomes_mod.load_market_regime(signals_db_path=signals_db)
        assert len(df) == 1
        assert df.iloc[0]["hmm_regime"] == "bull"


class TestComputeAndStoreTaConfidence:
    def test_no_signals_returns_empty_dict(self, monkeypatch):
        # build_signal_events reads config.settings.SIGNALS_DUCKDB_PATH as a
        # bound default argument (evaluated once at function definition,
        # not looked up dynamically), so it can't be redirected via a
        # monkeypatched module attribute — patch the function itself
        # instead, isolating this test from any real production DB.
        monkeypatch.setattr(outcomes_mod, "build_signal_events", lambda since=None: [])

        result = outcomes_mod.compute_and_store_ta_confidence()
        assert result == {}
