"""
tests/unit/test_run_integrity_check.py

Pure-logic tests for backtest/run_integrity_check.py's fold-metric/
integrity-check wiring — synthetic equity curves only, no real backtest
run or DB access required.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.run_integrity_check import build_fold_metrics, run_strategy_integrity_check


def _equity_curve(n_years: int = 6, start="2020-01-01", drift=0.0005, vol=0.01, seed=0) -> pd.Series:
    dates = pd.bdate_range(start, periods=252 * n_years)
    rng = np.random.default_rng(seed)
    values = 100 * (1 + rng.normal(drift, vol, len(dates))).cumprod()
    return pd.Series(values, index=dates)


class TestBuildFoldMetrics:
    def test_returns_one_sharpe_and_return_per_fold(self):
        eq = _equity_curve(n_years=6)
        sharpes, returns, folds = build_fold_metrics(eq, n_folds=3)
        assert len(sharpes) == 3
        assert len(returns) == 3
        assert len(folds) == 3

    def test_folds_are_chronologically_ordered(self):
        eq = _equity_curve(n_years=6)
        _, _, folds = build_fold_metrics(eq, n_folds=3)
        for train_df, test_df in folds:
            assert train_df["date"].max() < test_df["date"].min()

    def test_uptrend_equity_curve_yields_positive_returns(self):
        eq = _equity_curve(n_years=6, drift=0.002, vol=0.001, seed=1)
        _, returns, _ = build_fold_metrics(eq, n_folds=3)
        assert all(r > 0 for r in returns)

    def test_too_few_fiscal_years_raises(self):
        eq = _equity_curve(n_years=1)
        with pytest.raises(ValueError):
            build_fold_metrics(eq, n_folds=3)


class TestRunStrategyIntegrityCheck:
    def test_raises_when_critical_context_missing(self):
        eq = _equity_curve(n_years=6, seed=0)
        bm = _equity_curve(n_years=6, seed=1)
        # No feature_df/ohlcv_df/universe_tickers/etc supplied -> check_02/03/04/06/07
        # (all CRITICAL_CHECKS) fail -> run_all_checks() raises, not returns partial.
        with pytest.raises(RuntimeError, match="CRITICAL backtest integrity check"):
            run_strategy_integrity_check(eq, bm, n_folds=3)

    def test_passes_with_full_context_on_a_healthy_synthetic_run(self):
        eq = _equity_curve(n_years=6, drift=0.001, vol=0.005, seed=2)
        bm = _equity_curve(n_years=6, drift=0.0002, vol=0.005, seed=3)
        dates = pd.to_datetime(eq.index)

        feature_df = pd.DataFrame({
            "date": dates,
            "announcement_date": dates - pd.Timedelta(days=1),
            "roce": np.random.default_rng(4).normal(0, 1, len(dates)),
        })
        ohlcv_df = pd.DataFrame({"date": dates, "adj_factor": 1.0})
        universe_tickers = {f"TICKER_{i}" for i in range(90)}
        historical_tickers = universe_tickers | {"DELISTED_1", "DELISTED_2"}

        result = run_strategy_integrity_check(
            eq, bm,
            feature_df=feature_df, ohlcv_df=ohlcv_df,
            universe_tickers=universe_tickers, historical_tickers=historical_tickers,
            applied_roundtrip_cost_pct=0.01, n_folds=3,
        )
        assert result["check_01_walk_forward"] is True
        assert result["check_02_pit"] is True
        assert result["check_03_corp_actions"] is True
        assert result["check_04_survivorship"] is True
        assert result["check_05_costs"] is True
        assert result["check_06_liquidity"] is True
        assert result["check_07_no_hpo_on_test"] is True

    def test_understated_cost_fails_check_05(self):
        eq = _equity_curve(n_years=6, seed=5)
        bm = _equity_curve(n_years=6, seed=6)
        dates = pd.to_datetime(eq.index)
        feature_df = pd.DataFrame({"date": dates, "announcement_date": dates})
        ohlcv_df = pd.DataFrame({"date": dates, "adj_factor": 1.0})
        universe_tickers = {"A", "B"}
        historical_tickers = {"A", "B", "DELISTED"}

        with pytest.raises(RuntimeError, match="check_05_costs"):
            run_strategy_integrity_check(
                eq, bm,
                feature_df=feature_df, ohlcv_df=ohlcv_df,
                universe_tickers=universe_tickers, historical_tickers=historical_tickers,
                applied_roundtrip_cost_pct=0.0001,  # far below TOTAL_ROUNDTRIP_COST
                n_folds=3,
            )
