"""
tests/unit/test_momentum_signal.py

ML38 — features/momentum_signal.py. DB-backed functions use a real seeded
DuckDB (no mocks over the DB layer); the in-memory panel functions
(trailing_momentum_from_panel) are pure pandas and tested directly against
constructed DataFrames.
"""

import numpy as np
import pandas as pd
import pytest

import features.momentum_signal as ms
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised


def _seed_ohlcv(db_path, ticker, date_str, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
            VALUES (?, ?, ?, ?, ?, ?, 1000, 500, 50.0)
            ON CONFLICT DO NOTHING
            """,
            [date_str, ticker, close, close, close, close],
        )


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestTrailingMomentumDB:
    def test_computes_pct_return_over_window(self, normalised_db):
        dates = pd.date_range("2026-01-01", periods=5, freq="D")
        closes = [100, 110, 90, 120, 130]
        for d, c in zip(dates, closes):
            _seed_ohlcv(normalised_db, "AAA", d.date().isoformat(), c)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = ms.trailing_momentum(conn, ["AAA"], dates[-1].date().isoformat(), lookback_days=4)

        assert result["AAA"] == pytest.approx((130 / 100) - 1.0)

    def test_insufficient_history_excluded(self, normalised_db):
        _seed_ohlcv(normalised_db, "AAA", "2026-01-01", 100)
        _seed_ohlcv(normalised_db, "AAA", "2026-01-02", 110)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = ms.trailing_momentum(conn, ["AAA"], "2026-01-02", lookback_days=10)

        assert result.empty


class TestTrailingMomentumFromPanel:
    def test_matches_manual_calc(self):
        dates = pd.date_range("2026-01-01", periods=5, freq="D")
        panel = pd.DataFrame(
            {"AAA": [100, 105, 110, 115, 120], "BBB": [200, 190, 180, 170, 160]},
            index=dates,
        )
        result = ms.trailing_momentum_from_panel(panel, ["AAA", "BBB"], str(dates[-1].date()), lookback_days=4)
        assert result["AAA"] == pytest.approx((120 / 100) - 1.0)
        assert result["BBB"] == pytest.approx((160 / 200) - 1.0)

    def test_excludes_ticker_not_in_panel(self):
        dates = pd.date_range("2026-01-01", periods=3, freq="D")
        panel = pd.DataFrame({"AAA": [100, 105, 110]}, index=dates)
        result = ms.trailing_momentum_from_panel(panel, ["AAA", "ZZZ"], str(dates[-1].date()), lookback_days=2)
        assert list(result.index) == ["AAA"]

    def test_insufficient_history_returns_empty(self):
        dates = pd.date_range("2026-01-01", periods=2, freq="D")
        panel = pd.DataFrame({"AAA": [100, 105]}, index=dates)
        result = ms.trailing_momentum_from_panel(panel, ["AAA"], str(dates[-1].date()), lookback_days=10)
        assert result.empty


class TestTopNByMomentum:
    def test_returns_highest_first(self, normalised_db):
        dates = pd.date_range("2026-01-01", periods=3, freq="D")
        for d, c in zip(dates, [100, 100, 200]):
            _seed_ohlcv(normalised_db, "AAA", d.date().isoformat(), c)
        for d, c in zip(dates, [100, 100, 105]):
            _seed_ohlcv(normalised_db, "BBB", d.date().isoformat(), c)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            top1 = ms.top_n_by_momentum(conn, ["AAA", "BBB"], dates[-1].date().isoformat(), lookback_days=2, top_n=1)

        assert top1 == ["AAA"]


class TestOrthogonalizeMomentumVsFactors:
    """2026-07-19 full-codebase-review Fix B3: cross-sectional
    residualization of momentum against log(market_cap) and beta."""

    def test_removes_pure_size_effect(self):
        # Momentum is EXACTLY a linear function of log(market_cap) here
        # (no beta effect, beta constant) -> residual should be ~0 for all.
        rng = pd.Series(range(20))
        market_cap = pd.Series((rng + 1).to_numpy() * 100.0, index=[f"T{i}" for i in range(20)])
        momentum = pd.Series(
            (0.05 * np.log(market_cap.to_numpy())), index=market_cap.index
        )
        beta = pd.Series([1.0] * 20, index=market_cap.index)

        residual = ms.orthogonalize_momentum_vs_factors(momentum, market_cap, beta, min_observations=10)

        assert residual.abs().max() < 1e-6

    def test_falls_back_to_raw_momentum_below_min_observations(self):
        momentum = pd.Series({"A": 0.1, "B": 0.2, "C": 0.15})
        market_cap = pd.Series({"A": 100.0, "B": 200.0, "C": 150.0})
        beta = pd.Series({"A": 1.0, "B": 1.1, "C": 0.9})

        result = ms.orthogonalize_momentum_vs_factors(momentum, market_cap, beta, min_observations=10)

        pd.testing.assert_series_equal(result, momentum)

    def test_missing_market_cap_or_beta_dropped_from_regression(self):
        rng = np.random.default_rng(5)
        n = 15
        tickers = [f"T{i}" for i in range(n)]
        momentum = pd.Series(rng.normal(0.1, 0.05, n), index=tickers)
        market_cap = pd.Series(rng.uniform(100, 1000, n), index=tickers)
        beta = pd.Series(rng.uniform(0.7, 1.3, n), index=tickers)
        # Drop market_cap for one ticker -> that ticker excluded from regression.
        market_cap = market_cap.drop(index="T0")

        residual = ms.orthogonalize_momentum_vs_factors(momentum, market_cap, beta, min_observations=10)

        assert "T0" not in residual.index
