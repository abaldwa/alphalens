"""
tests/unit/test_iterative_retrain.py

Unit tests for backtest/iterative_retrain.py's holdout fiscal-year
selection — the "leave out one full fiscal year, skipping any year whose
trades might not be fully resolved yet" mechanism.
"""

import pandas as pd
import pytest

import backtest.iterative_retrain as iterative_retrain_mod
from backtest.core.metrics import TRADING_DAYS_PER_YEAR
from backtest.iterative_retrain import RetrainLoop, select_holdout_fiscal_year


class TestSelectHoldoutFiscalYear:
    def test_long_horizon_skips_the_most_recent_complete_fy(self):
        # User's own worked example: run in July 2026 with a ~1-year-horizon
        # strategy (252 trading days) should leave out FY2024-25
        # (2024-04-01 to 2025-03-31), skipping FY2025-26 as unresolved.
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)

        assert result.holdout_start == pd.Timestamp("2024-04-01")
        assert result.holdout_end == pd.Timestamp("2025-03-31")
        assert result.skipped_fiscal_years == [2025]

    def test_short_horizon_uses_the_most_recent_complete_fy(self):
        # A 5-day-horizon strategy's trades from FY2025-26 (ended 2026-03-31)
        # are long since resolved by July 2026 — no buffer year needed.
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=5)

        assert result.holdout_start == pd.Timestamp("2025-04-01")
        assert result.holdout_end == pd.Timestamp("2026-03-31")
        assert result.skipped_fiscal_years == []

    def test_holdout_end_is_always_before_as_of_date(self):
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)
        assert result.holdout_end < pd.Timestamp("2026-07-21")

    def test_raises_when_horizon_exceeds_lookback_window(self):
        with pytest.raises(ValueError):
            select_holdout_fiscal_year(
                pd.Timestamp("2026-07-21"), resolution_buffer_days=100_000, max_lookback_years=3,
            )

    def test_explain_mentions_skipped_years(self):
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)
        assert "FY2025-26" in result.explain()
        assert "FY2024-25" in result.explain()


class _FakeCombined:
    """Stand-in for BacktestEngine._combined — only needs a "date" column
    for the excluded_buffer_rows count."""

    def __init__(self):
        self["date"] = pd.Series([], dtype="datetime64[ns]")

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)


class _FakeResults:
    def __init__(self, sharpe_mean):
        self.aggregate = {
            "sharpe_mean": sharpe_mean, "sortino_mean": 1.0, "calmar_mean": 1.0, "win_rate_mean": 0.5,
        }
        self.fold_returns = pd.Series([0.01, -0.005, 0.02, 0.0])


class _FakeEngine:
    def __init__(self, sharpe_mean, **kwargs):
        self._sharpe_mean = sharpe_mean
        self._combined = _FakeCombined()

    def run_full_backtest(self, strategy_id, to_date=None, folds=4, collect_fold_returns=False, collect_fold_models=False):
        return _FakeResults(self._sharpe_mean)


class TestRetrainLoopDeannualizesSharpeBeforeDsr:
    """[BUG FIX, 5th fundamental-strategies review, item 2] RetrainLoop.run
    fed the ANNUALIZED results.aggregate["sharpe_mean"] straight into
    deflated_sharpe_ratio (which expects a per-period Sharpe) — the 4th
    call site missed when run_strategy_queue.py/backfill_dsr.py/engine.py
    were fixed for the identical bug. Confirms the value now reaching
    deflated_sharpe_ratio is de-annualized."""

    def test_dsr_receives_per_period_not_annualized_sharpe(self, monkeypatch):
        annualized_sharpe = 3.0  # deliberately "impossible looking" annualized value
        raw_sharpe_expected = annualized_sharpe / (TRADING_DAYS_PER_YEAR ** 0.5)

        monkeypatch.setattr(
            iterative_retrain_mod, "BacktestEngine",
            lambda **kwargs: _FakeEngine(annualized_sharpe, **kwargs),
        )

        captured = {}

        def _fake_dsr(sharpe, n_trials, n_obs, returns=None):
            captured["sharpe"] = sharpe
            return 0.0  # below min_dsr_threshold -> rejected, no random_feature_test/holdout needed

        monkeypatch.setattr(iterative_retrain_mod, "deflated_sharpe_ratio", _fake_dsr)

        loop = RetrainLoop(
            engine_kwargs={"horizon_days": 5}, strategy_id="test_strategy",
            conn=None, hyperparam_grid=[{"param": 1}], max_iterations=1,
        )
        result = loop.run(combined_ohlcv_max_date=pd.Timestamp("2026-07-01"))

        assert captured["sharpe"] == pytest.approx(raw_sharpe_expected)
        assert captured["sharpe"] != annualized_sharpe
        assert result.iterations[0].sharpe_mean == annualized_sharpe
