"""tests/unit/test_core_metrics.py — backtest/core/metrics.py.

Per BacktestUmbrellaPlan.md's No-Mock-Data Policy: pure-arithmetic unit
tests here use small hand-computed numeric fixtures (that's testing
formulas, not market realism). The real-data integration check
(TestComputeMetricsAgainstRealData) reads an actual historical OHLCV
slice from datastore/normalised/alphalens.duckdb — no fabricated prices.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.core.metrics import (
    apply_tax_to_curve, calendar_cagr, calmar_ratio, compute_metrics, max_drawdown,
    reconstruct_pre_tax_curve, sortino_ratio, trading_day_cagr, turnover_ratio,
    win_rate_and_profit_factor,
)


class TestCalendarCagr:
    def test_doubling_over_one_year(self):
        assert calendar_cagr(100.0, 200.0, "2020-01-01", "2021-01-01") == pytest.approx(1.0, abs=0.01)

    def test_returns_none_for_non_positive_starting_capital(self):
        assert calendar_cagr(0.0, 200.0, "2020-01-01", "2021-01-01") is None

    def test_returns_none_when_end_date_not_after_start_date(self):
        assert calendar_cagr(100.0, 200.0, "2021-01-01", "2020-01-01") is None


class TestTradingDayCagr:
    def test_doubling_over_252_trading_days(self):
        equity = pd.Series([100.0] + [100.0] * 251 + [200.0])
        result = trading_day_cagr(equity)
        assert result == pytest.approx(1.0, abs=0.05)

    def test_returns_none_for_series_too_short(self):
        assert trading_day_cagr(pd.Series([100.0])) is None


class TestMaxDrawdown:
    def test_computes_peak_to_trough_decline(self):
        equity = pd.Series([100.0, 120.0, 90.0, 110.0])
        # peak 120 -> trough 90 = -25%
        assert max_drawdown(equity) == pytest.approx(-0.25)

    def test_empty_series_returns_zero(self):
        assert max_drawdown(pd.Series(dtype=float)) == 0.0


class TestSortinoRatio:
    def test_no_downside_returns_none_with_reason(self):
        returns = pd.Series([0.01, 0.02, 0.015])
        value, reason = sortino_ratio(returns)
        assert value is None
        assert reason == "no_downside_periods"

    def test_positive_with_some_downside(self):
        returns = pd.Series([0.02, -0.01, 0.03, -0.02, 0.01])
        value, reason = sortino_ratio(returns)
        assert value is not None
        assert reason is None

    def test_insufficient_returns_reason(self):
        value, reason = sortino_ratio(pd.Series([0.01]))
        assert value is None
        assert reason == "insufficient_returns"

    def test_zero_downside_std_reason(self):
        returns = pd.Series([0.01, -0.02, 0.015, -0.02, 0.03])  # identical downside values -> std 0
        value, reason = sortino_ratio(returns)
        assert value is None
        assert reason == "zero_downside_std"


class TestCalmarRatio:
    def test_ratio_of_cagr_to_abs_drawdown(self):
        value, reason = calmar_ratio(0.20, -0.10)
        assert value == pytest.approx(2.0)
        assert reason is None

    def test_none_when_cagr_is_none(self):
        value, reason = calmar_ratio(None, -0.10)
        assert value is None
        assert reason == "no_cagr"

    def test_none_when_drawdown_is_zero(self):
        value, reason = calmar_ratio(0.20, 0.0)
        assert value is None
        assert reason == "zero_or_undefined_drawdown"


class TestWinRateAndProfitFactor:
    def test_mixed_wins_and_losses(self):
        win_rate, profit_factor = win_rate_and_profit_factor([100.0, -50.0, 200.0, -25.0])
        assert win_rate == pytest.approx(0.5)
        assert profit_factor == pytest.approx(300.0 / 75.0)

    def test_empty_trades_returns_none_none(self):
        assert win_rate_and_profit_factor([]) == (None, None)

    def test_all_losses_profit_factor_is_zero_over_positive_denominator(self):
        win_rate, profit_factor = win_rate_and_profit_factor([-10.0, -20.0])
        assert win_rate == 0.0
        assert profit_factor == pytest.approx(0.0)


class TestTurnoverRatio:
    def test_ratio_of_traded_value_to_avg_portfolio(self):
        assert turnover_ratio([100_000.0, 100_000.0], 1_000_000.0) == pytest.approx(0.2)

    def test_none_for_non_positive_avg_portfolio(self):
        assert turnover_ratio([100_000.0], 0.0) is None


class TestComputeMetricsStandaloneFixture:
    def test_lump_sum_run_produces_all_required_fields(self):
        equity = pd.Series(
            [1_000_000.0, 1_050_000.0, 980_000.0, 1_200_000.0],
            index=pd.date_range("2020-01-01", periods=4, freq="6ME"),
        )
        # Contributions only. compute_metrics appends the closing value from
        # the equity curve itself — see _xirr_cash_flows.
        cash_flows = [("2020-01-01", -1_000_000.0)]
        result = compute_metrics(
            equity_curve=equity, cash_flows=cash_flows,
            trade_pnls=[50_000.0, -70_000.0, 220_000.0],
            trade_values=[500_000.0, 400_000.0, 600_000.0],
            distinct_tickers=["RELIANCE", "TCS", "RELIANCE"],
            start_date="2020-01-01", end_date="2021-06-01",
            total_contributed=1_000_000.0,
        )
        assert result.final_capital == pytest.approx(1_200_000.0)
        assert result.n_distinct_tickers_traded == 2
        assert result.n_trades == 3
        assert result.benchmark_status == "insufficient_benchmark_history"  # start_date predates 2023-07
        assert result.cagr is not None
        assert result.xirr is not None
        assert result.max_drawdown < 0
        assert result.avg_days_held is None  # no holding_days passed


class TestAvgDaysHeld:
    """avg_days_held: mean (exit_date - entry_date).days across closed trades
    (backtest/core/engine.py's BacktestOrchestrator._finalize derives this
    list from portfolio.trades and passes it through as holding_days)."""

    def _base_kwargs(self, **overrides):
        kwargs = dict(
            equity_curve=pd.Series([1_000_000.0, 1_100_000.0], index=pd.date_range("2020-01-01", periods=2, freq="6ME")),
            cash_flows=[("2020-01-01", -1_000_000.0)],
            trade_pnls=[10_000.0, 20_000.0, -5_000.0],
            trade_values=[100_000.0, 100_000.0, 100_000.0],
            distinct_tickers=["A", "B", "C"],
            start_date="2020-01-01", end_date="2020-06-01",
            total_contributed=1_000_000.0,
        )
        kwargs.update(overrides)
        return kwargs

    def test_mean_of_holding_days_across_closed_trades(self):
        result = compute_metrics(**self._base_kwargs(holding_days=[10, 20, 30]))
        assert result.avg_days_held == pytest.approx(20.0)

    def test_none_when_no_trades(self):
        result = compute_metrics(**self._base_kwargs(holding_days=[]))
        assert result.avg_days_held is None

    def test_none_when_holding_days_not_supplied(self):
        result = compute_metrics(**self._base_kwargs())
        assert result.avg_days_held is None


class TestComputeMetricsAgainstRealData:
    """No-Mock-Data Policy: exercises compute_metrics() against a real,
    read-only OHLCV slice rather than a fabricated equity curve, so the
    metrics pipeline is validated against actual data shapes (gaps,
    non-uniform trading calendars) it will see in production."""

    def test_real_ohlcv_slice_produces_sane_metrics(self):
        import duckdb

        db_path = "datastore/normalised/alphalens.duckdb"
        try:
            con = duckdb.connect(db_path, read_only=True)
        except duckdb.IOException:
            pytest.skip("alphalens.duckdb locked by another process (e.g. the API server) — skipping real-data check")
            return

        try:
            df = con.execute(
                """
                SELECT date, close FROM ohlcv_adjusted
                WHERE ticker = 'RELIANCE' AND date BETWEEN '2015-01-01' AND '2015-12-31'
                ORDER BY date
                """
            ).fetchdf()
        finally:
            con.close()

        if df.empty:
            pytest.skip("no real RELIANCE 2015 OHLCV rows available in this environment")
            return

        equity = pd.Series(df["close"].values * 1000, index=pd.to_datetime(df["date"]))  # simulate a fixed 1000-share position
        cash_flows = [(str(df["date"].iloc[0]), -float(equity.iloc[0]))]
        result = compute_metrics(
            equity_curve=equity, cash_flows=cash_flows,
            trade_pnls=[float(equity.iloc[-1] - equity.iloc[0])],
            trade_values=[float(equity.iloc[0])],
            distinct_tickers=["RELIANCE"],
            start_date=str(df["date"].iloc[0]), end_date=str(df["date"].iloc[-1]),
            total_contributed=float(equity.iloc[0]),
        )
        assert result.final_capital == pytest.approx(float(equity.iloc[-1]))
        assert -1.0 <= result.max_drawdown <= 0.0


class TestXirrIncludesTheClosingValue:
    """Regression, 2026-08-19.

    XIRR was computed on the contribution side only — the liquidation inflow
    at end_date was never appended. With a plain lump sum that left every flow
    the same sign and xirr() correctly refused to bracket a root, so the field
    came back None. With annual tax deduction it was worse: the FY tax events
    were positive flows, which DID bracket a root, and the engine reported an
    arbitrary negative rate. A live momentum run with a 21.4%/yr CAGR was
    published as XIRR -3.3%/yr on exactly this path.

    For a single lump sum in and nothing out, XIRR is the same measurement as
    CAGR and must agree with it.
    """

    def _equity(self):
        # 10 lakh -> 20 lakh over four calendar years: ~18.9%/yr.
        return pd.Series(
            [1_000_000.0, 1_300_000.0, 1_500_000.0, 1_700_000.0, 2_000_000.0],
            index=pd.date_range("2016-01-01", periods=5, freq="YE-DEC"),
        )

    def _metrics(self, cash_flows):
        return compute_metrics(
            equity_curve=self._equity(),
            cash_flows=cash_flows,
            trade_pnls=[1_000_000.0],
            trade_values=[1_000_000.0],
            distinct_tickers=["RELIANCE"],
            start_date="2016-01-01",
            end_date="2019-12-31",
            total_contributed=1_000_000.0,
        )

    def test_lump_sum_xirr_equals_cagr(self):
        result = self._metrics([("2016-01-01", -1_000_000.0)])
        assert result.xirr is not None, "a lump-sum run has a well-defined XIRR"
        assert result.xirr == pytest.approx(result.cagr, abs=1e-3)

    def test_xirr_is_positive_when_the_book_doubled(self):
        """The specific shape of the bug: a doubling book reporting a negative
        money-weighted return."""
        result = self._metrics([("2016-01-01", -1_000_000.0)])
        assert result.xirr > 0

    def test_a_mid_run_contribution_pulls_xirr_below_cagr(self):
        """Money added late earns less of the run, so the money-weighted
        return must fall below the time-weighted one — the property that makes
        XIRR worth reporting alongside CAGR at all."""
        lump = self._metrics([("2016-01-01", -1_000_000.0)])
        topped_up = self._metrics(
            [("2016-01-01", -1_000_000.0), ("2019-06-01", -900_000.0)]
        )
        assert topped_up.xirr is not None
        assert topped_up.xirr < lump.xirr


class TestVolatility:
    """Regression, 2026-08-19: the Risk screen has always had a Volatility
    column and BacktestMetrics had no field behind it, so every row rendered
    an explained em dash."""

    def test_annualised_volatility_is_reported(self):
        rng = np.random.default_rng(7)
        daily = rng.normal(0.0006, 0.012, 800)
        equity = pd.Series(
            1_000_000.0 * np.cumprod(1.0 + daily),
            index=pd.bdate_range("2020-01-01", periods=800),
        )
        result = compute_metrics(
            equity_curve=equity,
            cash_flows=[("2020-01-01", -1_000_000.0)],
            trade_pnls=[10_000.0],
            trade_values=[100_000.0],
            distinct_tickers=["A"],
            start_date="2020-01-01",
            end_date=str(equity.index[-1].date()),
            total_contributed=1_000_000.0,
        )
        # 1.2% daily over 252 sessions is ~19%/yr; the assertion is loose
        # because the point is that a real number is emitted, not that the
        # generator hit its mean exactly.
        assert result.volatility == pytest.approx(0.012 * (252 ** 0.5), rel=0.15)

    def test_a_flat_curve_reports_none_not_zero(self):
        """An unmeasurable volatility and a genuinely riskless one are
        different facts, and a 0.00 in the column would claim the second."""
        flat = pd.Series([1_000_000.0] * 50, index=pd.bdate_range("2020-01-01", periods=50))
        result = compute_metrics(
            equity_curve=flat,
            cash_flows=[("2020-01-01", -1_000_000.0)],
            trade_pnls=[],
            trade_values=[],
            distinct_tickers=[],
            start_date="2020-01-01",
            end_date=str(flat.index[-1].date()),
            total_contributed=1_000_000.0,
        )
        assert result.volatility is None


class TestPostTaxIsNeverAbovePreTax:
    """Regression, 2026-08-19.

    A pre-tax run used to have its ENTIRE cumulative FY tax docked off the
    final equity point, which over a long high-churn run is a large multiple
    of any single year's bill. The result was reported as the run's headline
    CAGR under the label `pre_tax`, and came out BELOW the post-tax run of the
    same strategy — 4.9%/yr against 19.0%/yr on a live momentum sweep. Tax is
    an annual event, so apply_tax_to_curve charges each year on the day it
    fell due.
    """

    def _curve(self):
        return pd.Series(
            [1_000_000.0, 1_400_000.0, 1_900_000.0, 2_600_000.0],
            index=pd.to_datetime(["2016-04-01", "2017-03-31", "2018-03-31", "2019-03-31"]),
        )

    def test_charging_tax_annually_lands_between_zero_and_the_pre_tax_curve(self):
        curve = self._curve()
        ledger = [
            {"fy_end": "2017-03-31", "paid": 40_000.0},
            {"fy_end": "2018-03-31", "paid": 50_000.0},
        ]
        post = apply_tax_to_curve(curve, ledger)
        assert post is not None
        # Nothing is charged before the first FY end...
        assert post.iloc[0] == pytest.approx(curve.iloc[0])
        # ...each payment applies from its own date onwards, cumulatively...
        assert post.iloc[1] == pytest.approx(curve.iloc[1] - 40_000.0)
        assert post.iloc[3] == pytest.approx(curve.iloc[3] - 90_000.0)
        # ...and the post-tax path is never above the pre-tax one.
        assert (post <= curve + 1e-9).all()

    def test_it_is_the_exact_inverse_of_the_pre_tax_reconstruction(self):
        curve = self._curve()
        ledger = [{"fy_end": "2017-03-31", "paid": 40_000.0}]
        post = apply_tax_to_curve(curve, ledger)
        assert post is not None
        back = reconstruct_pre_tax_curve(post, ledger)
        assert back is not None
        pd.testing.assert_series_equal(back, curve, check_names=False)

    def test_no_ledger_means_no_other_basis_rather_than_a_fabricated_one(self):
        assert apply_tax_to_curve(self._curve(), []) is None
        assert apply_tax_to_curve(self._curve(), None) is None
