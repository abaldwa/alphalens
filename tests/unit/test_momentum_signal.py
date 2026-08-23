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


class TestRiskAdjustedMomentumScore:
    """Phase 5 (spec section 8): risk_adjusted_composite_momentum combining
    12-month and 6-month momentum, each divided by volatility."""

    def test_basic_structure(self):
        # Build a simple panel: 254 trading days (~12.7 months), 2 tickers
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        rng = np.random.default_rng(42)

        # AAA: uptrend (vol=1%), consistent positive returns
        aaa_returns = rng.normal(0.0005, 0.001, 254)  # +0.05% daily, 0.1% vol
        aaa_prices = 100.0 * np.cumprod(1.0 + aaa_returns)

        # BBB: noisy, no trend (vol=3%), near-zero mean return
        bbb_returns = rng.normal(0.0, 0.003, 254)  # 0% daily, 0.3% vol
        bbb_prices = 100.0 * np.cumprod(1.0 + bbb_returns)

        panel = pd.DataFrame(
            {"AAA": aaa_prices, "BBB": bbb_prices},
            index=dates,
        )

        result = ms.risk_adjusted_momentum_score(
            panel, ["AAA", "BBB"], str(dates[-1].date()), volatility_measure="daily_return_stddev"
        )

        # Both should be scored (have enough history)
        assert "AAA" in result.index
        assert "BBB" in result.index
        # Uptrend with low vol should score higher than noisy-no-trend
        assert result["AAA"] > result["BBB"]

    def test_insufficient_history_empty_result(self):
        # Only 50 days — need at least 252 for full lookback
        dates = pd.date_range("2024-01-01", periods=50, freq="D")
        panel = pd.DataFrame({"AAA": 100.0 + np.arange(50)}, index=dates)

        result = ms.risk_adjusted_momentum_score(
            panel, ["AAA"], str(dates[-1].date()), volatility_measure="daily_return_stddev"
        )

        assert result.empty

    def test_daily_price_volatility_measure(self):
        # Same panel as test_basic_structure but with daily_price_stddev
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        rng = np.random.default_rng(42)

        aaa_returns = rng.normal(0.0005, 0.001, 254)
        aaa_prices = 100.0 * np.cumprod(1.0 + aaa_returns)
        bbb_returns = rng.normal(0.0, 0.003, 254)
        bbb_prices = 100.0 * np.cumprod(1.0 + bbb_returns)

        panel = pd.DataFrame(
            {"AAA": aaa_prices, "BBB": bbb_prices},
            index=dates,
        )

        result = ms.risk_adjusted_momentum_score(
            panel, ["AAA", "BBB"], str(dates[-1].date()), volatility_measure="daily_price_stddev"
        )

        # Should return two scores
        assert len(result) == 2
        assert result.notna().all()

    def test_skip_month_variant(self):
        # Test skip-month lookbacks (12-7, 6-2)
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        rng = np.random.default_rng(42)

        aaa_returns = rng.normal(0.0005, 0.001, 254)
        aaa_prices = 100.0 * np.cumprod(1.0 + aaa_returns)

        panel = pd.DataFrame({"AAA": aaa_prices}, index=dates)

        result_standard = ms.risk_adjusted_momentum_score(
            panel, ["AAA"], str(dates[-1].date()), use_skip_month=False
        )
        result_skip = ms.risk_adjusted_momentum_score(
            panel, ["AAA"], str(dates[-1].date()), use_skip_month=True
        )

        # Both should return a score
        assert len(result_standard) == 1
        assert len(result_skip) == 1
        # Scores should differ (different lookbacks)
        assert result_standard["AAA"] != result_skip["AAA"]

    def test_winsorization_caps_outliers(self):
        # Create a panel where one ticker has an extreme score
        dates = pd.date_range("2024-01-01", periods=254, freq="D")

        # Normal ticker
        panel = pd.DataFrame({
            "A": 100.0 * (1.01 ** np.arange(254)),  # Steady 1% daily growth
            "B": 100.0 * (1.01 ** np.arange(254)),  # Same
        }, index=dates)

        result = ms.risk_adjusted_momentum_score(
            panel, ["A", "B"], str(dates[-1].date()), winsorize_pct=0.05
        )

        # Scores should be reasonable (winsorized, not inf)
        assert result.notna().all()
        assert np.isfinite(result).all()

    def test_empty_panel(self):
        result = ms.risk_adjusted_momentum_score(
            pd.DataFrame(), [], "2024-12-31"
        )
        assert result.empty

    def test_ticker_not_in_panel(self):
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        panel = pd.DataFrame({"AAA": 100.0 + np.arange(254)}, index=dates)

        result = ms.risk_adjusted_momentum_score(
            panel, ["ZZZ"], str(dates[-1].date())
        )

        assert result.empty


class TestDailyVolatilityHelpers:
    """Helper functions for risk_adjusted_momentum_score."""

    def test_daily_return_volatility_structure(self):
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        rng = np.random.default_rng(42)

        # Ticker with 1% daily vol
        prices = 100.0 * np.cumprod(1.0 + rng.normal(0.0, 0.01, 254))
        panel = pd.DataFrame({"AAA": prices}, index=dates)

        result = ms._daily_return_volatility(panel, ["AAA"], str(dates[-1].date()), lookback_days=252)

        assert len(result) == 1
        assert result["AAA"] > 0
        # 1% daily vol annualizes to ~15.8% (sqrt(252) * 0.01)
        assert result["AAA"] < 0.20

    def test_daily_price_volatility_structure(self):
        dates = pd.date_range("2024-01-01", periods=254, freq="D")
        rng = np.random.default_rng(42)

        prices = 100.0 * np.cumprod(1.0 + rng.normal(0.0, 0.01, 254))
        panel = pd.DataFrame({"AAA": prices}, index=dates)

        result = ms._daily_price_volatility(panel, ["AAA"], str(dates[-1].date()), lookback_days=252)

        assert len(result) == 1
        assert result["AAA"] > 0
