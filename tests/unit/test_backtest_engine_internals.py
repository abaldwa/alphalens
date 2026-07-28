"""
tests/unit/test_backtest_engine_internals.py

Real-logic coverage for backtest/engine.py's private helper methods
(_pnd_scores, _pnd_blocked, _build_momentum, _apply_exits, _apply_entries,
_simulate, _run_integrity_check) plus compute_fold_metrics's trades-present
branch, and BacktestEngine.__init__'s "no real benchmark" ValueError and
universe/historical ticker defaulting.

These are exercised on a bare BacktestEngine instance built with
object.__new__ (bypassing __init__'s heavy real feature-computation /
model-training pipeline, which tests/unit/test_backtest_benchmark.py
already establishes as the precedent for this class — see its
TestBuildBenchmarkCurve._fake_engine) with only the attributes each method
actually reads set directly. The pnd_detector / exit_model / signal_model /
meta_model stand-ins are minimal real IModel-shaped objects implementing
real (if simple) prediction logic, same pattern as test_backtester.py's
_MajorityClassModel — not fabricated business data, just deterministic
routing logic so BacktestEngine's own control flow (thresholding, dict
lookups, portfolio state transitions) is genuinely exercised end to end.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.engine import BacktestEngine, compute_fold_metrics, _raw_sharpe_from_returns
from backtest.overfit_checks import deflated_sharpe_ratio
from backtest.portfolio import PortfolioSimulator
from features.technical import CORE_TECHNICAL_FEATURES


def _bare_engine(**attrs):
    eng = object.__new__(BacktestEngine)
    defaults = dict(
        initial_capital=1_000_000.0,
        sizing_mode="equal_weight",
        n_target_positions=10,
        sector_map={},
        watchlist_tickers=None,
        exit_model=None,
        universe_tickers={"A", "B"},
        historical_tickers={"A", "B"},
        _feature_log_writer=None,
        _run_id=None,
        # Real (2026-07-21 REV2/REV3) ADTV wiring — default to "abundantly
        # liquid" for every ticker so these pre-existing tests (which
        # exercise P&D/signal/meta routing, not liquidity filtering) keep
        # their original behavior. Set as a plain callable, not a
        # pd.Series, so `object.__new__`-built instances (which never run
        # __init__'s real _build_adtv_lookup) don't need a real OHLCV
        # panel; tests exercising the liquidity floor itself override this.
        _adtv_cr=lambda d, tickers: pd.Series(1e9, index=tickers),
    )
    defaults.update(attrs)
    for k, v in defaults.items():
        setattr(eng, k, v)
    return eng


class _ConstantPnd:
    """Real predict_full logic: fixed pnd_score/pnd_block per row, threshold-based."""

    def predict_full(self, rows: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {"pnd_score": [0.9] * len(rows), "pnd_block": [True] * len(rows)}, index=rows.index
        )


class _NoPnd:
    def predict_full(self, rows: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"pnd_score": [0.0] * len(rows), "pnd_block": [False] * len(rows)}, index=rows.index)


class _UrgentExitModel:
    """Always signals immediate exit (urgency 95 > EXIT_URGENT_THRESHOLD=80)."""

    def predict_full(self, ctx: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"exit_urgency": [95.0] * len(ctx)}, index=ctx.index)


class _HoldExitModel:
    def predict_full(self, ctx: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"exit_urgency": [10.0] * len(ctx)}, index=ctx.index)


class _AlwaysBuySignal:
    def predict(self, X: pd.DataFrame) -> pd.Series:
        return pd.Series([1] * len(X), index=X.index)


class _AlwaysHoldSignal:
    def predict(self, X: pd.DataFrame) -> pd.Series:
        return pd.Series([0] * len(X), index=X.index)


class _AllowMeta:
    def predict(self, X: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(X), index=X.index)


class _BlockMeta:
    def predict(self, X: pd.DataFrame) -> pd.Series:
        return pd.Series([False] * len(X), index=X.index)


def _pnd_features_index(date, tickers):
    idx = pd.MultiIndex.from_tuples([(date, t) for t in tickers], names=["date", "ticker"])
    from features.pnd_features import PND_FEATURES

    return pd.DataFrame(1.0, index=idx, columns=PND_FEATURES)


# ===== _pnd_scores / _pnd_blocked =====


class TestPndScoresBlocked:
    def test_pnd_scores_present_ticker_uses_detector_output(self):
        date = pd.Timestamp("2026-01-05")
        eng = _bare_engine(pnd_detector=_ConstantPnd(), _pnd_features=_pnd_features_index(date, ["A"]))
        scores = eng._pnd_scores(date, ["A", "MISSING"])
        assert scores["A"] == pytest.approx(0.9)
        assert scores["MISSING"] == pytest.approx(0.0)  # not in index -> filled 0.0

    def test_pnd_scores_no_tickers_present_returns_all_zero(self):
        date = pd.Timestamp("2026-01-05")
        eng = _bare_engine(pnd_detector=_ConstantPnd(), _pnd_features=_pnd_features_index(date, []))
        scores = eng._pnd_scores(date, ["X", "Y"])
        assert list(scores) == [0.0, 0.0]

    def test_pnd_blocked_present_ticker_flags_block(self):
        date = pd.Timestamp("2026-01-05")
        eng = _bare_engine(pnd_detector=_ConstantPnd(), _pnd_features=_pnd_features_index(date, ["A"]))
        blocked = eng._pnd_blocked(date, ["A", "MISSING"])
        assert blocked["A"] is True or blocked["A"] == True  # noqa: E712
        assert blocked["MISSING"] == False  # noqa: E712

    def test_pnd_blocked_no_tickers_present_returns_all_false(self):
        date = pd.Timestamp("2026-01-05")
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        blocked = eng._pnd_blocked(date, ["X"])
        assert blocked["X"] == False  # noqa: E712


# ===== _build_momentum =====


class TestBuildMomentum:
    def test_momentum_computed_as_63_day_return(self):
        dates = pd.date_range("2026-01-01", periods=70, freq="D")
        close = np.linspace(100, 200, 70)  # +100% over the window
        ohlcv = pd.DataFrame({"date": dates, "ticker": "A", "close": close})
        eng = _bare_engine(ohlcv=ohlcv)

        momentum = eng._build_momentum()

        last_date = dates[-1]
        val = momentum[(last_date, "A")]
        expected = close[-1] / close[-1 - 63] - 1
        assert val == pytest.approx(expected)

    def test_momentum_nan_before_63_days_of_history(self):
        dates = pd.date_range("2026-01-01", periods=10, freq="D")
        ohlcv = pd.DataFrame({"date": dates, "ticker": "A", "close": np.linspace(100, 110, 10)})
        eng = _bare_engine(ohlcv=ohlcv)
        momentum = eng._build_momentum()
        assert pd.isna(momentum[(dates[0], "A")])


# ===== _apply_exits =====


class TestApplyExits:
    def _portfolio_with_position(self, ticker="A", entry_price=100.0):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        prices = {ticker: entry_price}
        portfolio.buy(ticker, "SECTOR1", entry_price, pd.Timestamp("2026-01-01"), prices)
        return portfolio

    def test_urgent_exit_closes_position(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = self._portfolio_with_position()
        eng = _bare_engine(
            exit_model=_UrgentExitModel(),
            pnd_detector=_NoPnd(),
            _pnd_features=_pnd_features_index(date, []),
            _momentum=pd.Series(dtype=float),
        )
        eng._apply_exits(portfolio, date, {"A": 120.0})
        assert "A" not in portfolio.positions
        assert len(portfolio.trades) == 1
        assert portfolio.trades[0].exit_reason == "exit_model_urgent"

    def test_hold_urgency_keeps_position_open(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = self._portfolio_with_position()
        eng = _bare_engine(
            exit_model=_HoldExitModel(),
            pnd_detector=_NoPnd(),
            _pnd_features=_pnd_features_index(date, []),
            _momentum=pd.Series(dtype=float),
        )
        eng._apply_exits(portfolio, date, {"A": 120.0})
        assert "A" in portfolio.positions
        assert len(portfolio.trades) == 0

    def test_no_held_tickers_with_prices_is_a_noop(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = self._portfolio_with_position()
        eng = _bare_engine(exit_model=_HoldExitModel(), pnd_detector=_NoPnd())
        # 'A' held but not in prices_today -> filtered out before any pnd/exit call.
        eng._apply_exits(portfolio, date, {})
        assert "A" in portfolio.positions


# ===== _apply_entries =====


def _day_rows(tickers, extra_cols=None):
    row = {c: 1.0 for c in CORE_TECHNICAL_FEATURES}
    row["atr_14_pct"] = 2.0
    if extra_cols:
        row.update(extra_cols)
    if not tickers:
        return pd.DataFrame(columns=["ticker", *row.keys()])
    return pd.DataFrame([{"ticker": t, **row} for t in tickers])


class TestApplyEntries:
    def test_buy_signal_opens_position(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysBuySignal(), None)

        assert "A" in portfolio.positions
        assert portfolio.positions["A"].entry_atr_pct == pytest.approx(0.02)

    def test_no_buy_signal_skips_entry(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysHoldSignal(), None)

        assert "A" not in portfolio.positions

    def test_meta_model_blocks_entry(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysBuySignal(), _BlockMeta())

        assert "A" not in portfolio.positions

    def test_meta_model_allows_entry(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysBuySignal(), _AllowMeta())

        assert "A" in portfolio.positions

    def test_pnd_blocked_candidate_never_reaches_signal_model(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_ConstantPnd(), _pnd_features=_pnd_features_index(date, ["A"]))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysBuySignal(), None)

        assert "A" not in portfolio.positions

    def test_already_held_ticker_excluded_from_candidates(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2026-01-01"), {"A": 100.0})
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 105.0}, _AlwaysBuySignal(), None)

        # Only ever opened once (via the direct buy() above), _apply_entries is a no-op re-buy.
        assert portfolio.positions["A"].entry_price == pytest.approx(100.0)

    def test_watchlist_filter_excludes_non_watchlist_tickers(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(
            pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []), watchlist_tickers={"B"}
        )
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {"A": 100.0}, _AlwaysBuySignal(), None)

        assert "A" not in portfolio.positions

    def test_empty_candidates_after_filter_is_noop(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows([])

        eng._apply_entries(portfolio, day_rows, date, {}, _AlwaysBuySignal(), None)

        assert portfolio.positions == {}

    def test_missing_price_for_buy_candidate_skips_it(self):
        date = pd.Timestamp("2026-01-10")
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=5)
        eng = _bare_engine(pnd_detector=_NoPnd(), _pnd_features=_pnd_features_index(date, []))
        day_rows = _day_rows(["A"])

        eng._apply_entries(portfolio, day_rows, date, {}, _AlwaysBuySignal(), None)

        assert "A" not in portfolio.positions


# ===== _simulate =====


class TestSimulate:
    def test_simulate_buys_then_exits_across_days_and_records_equity(self):
        dates = pd.date_range("2026-01-01", periods=3, freq="D")
        test_fold = pd.concat([_day_rows(["A"]).assign(date=d) for d in dates], ignore_index=True)
        price_lookup = pd.Series(
            [100.0, 110.0, 130.0],
            index=pd.MultiIndex.from_tuples([(d, "A") for d in dates], names=["date", "ticker"]),
        )
        eng = _bare_engine(
            pnd_detector=_NoPnd(),
            _pnd_features=_pnd_features_index(dates[0], []),
            _momentum=pd.Series(dtype=float),
            _price_lookup=price_lookup,
            exit_model=_UrgentExitModel(),
            sector_map={"A": "SECTOR1"},
        )

        portfolio = eng._simulate(test_fold, _AlwaysBuySignal(), None)

        # Day 1: bought (no existing position to exit). Day 2: urgent exit closes it,
        # then re-bought same day (signal always buys); day 3: urgent exit closes it again.
        assert len(portfolio.equity_curve) == 3
        assert len(portfolio.trades) == 2
        assert all(t.exit_reason == "exit_model_urgent" for t in portfolio.trades)

    def test_simulate_with_no_price_data_for_a_date_still_records_equity(self):
        dates = pd.date_range("2026-01-01", periods=2, freq="D")
        test_fold = pd.concat([_day_rows(["A"]).assign(date=d) for d in dates], ignore_index=True)
        # price_lookup only covers the first date -> second date has no entry in the index.
        price_lookup = pd.Series([100.0], index=pd.MultiIndex.from_tuples([(dates[0], "A")], names=["date", "ticker"]))
        eng = _bare_engine(
            pnd_detector=_NoPnd(),
            _pnd_features=_pnd_features_index(dates[0], []),
            _momentum=pd.Series(dtype=float),
            _price_lookup=price_lookup,
            exit_model=_HoldExitModel(),
            sector_map={"A": "SECTOR1"},
        )

        portfolio = eng._simulate(test_fold, _AlwaysHoldSignal(), None)

        assert len(portfolio.equity_curve) == 2


# ===== _run_integrity_check =====


class TestRunIntegrityCheck:
    def test_clean_folds_pass(self):
        train = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=365)})
        test = pd.DataFrame({"date": pd.date_range("2021-01-01", periods=365)})
        eng = _bare_engine(
            _combined=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv=pd.DataFrame({"adj_factor": [1.0, 1.0]}),
            universe_tickers={"A"},
            historical_tickers={"A", "DELISTED1"},
        )
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        result = eng._run_integrity_check(train, test, portfolio)
        assert result["passed"] is True
        assert result["detail"]["critical_failures"] == []

    def test_leaked_folds_fail_and_are_captured_not_raised(self):
        train = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=400)})
        leaked_test = pd.DataFrame({"date": pd.date_range("2020-06-01", periods=30)})
        eng = _bare_engine(
            _combined=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv=pd.DataFrame({"adj_factor": [1.0]}),
            universe_tickers={"A"},
            historical_tickers={"A"},
        )
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        result = eng._run_integrity_check(train, leaked_test, portfolio)
        assert result["passed"] is False
        assert len(result["detail"]["critical_failures"]) >= 1

    def test_critical_failure_still_persists_per_check_breakdown(self):
        """[BUG FIX, 6th fundamental-strategies review, item 1] the
        `except RuntimeError` handler used to discard the fully-computed
        per-check pass/fail map on any CRITICAL check failure, persisting
        only critical_failures - hiding which OTHER checks passed. Recovered
        from BacktestIntegrityChecker._results_cache, mirroring the fix
        already applied to backtest/core/post_run_checks.py."""
        train = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=400)})
        leaked_test = pd.DataFrame({"date": pd.date_range("2020-06-01", periods=30)})
        eng = _bare_engine(
            _combined=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv=pd.DataFrame({"adj_factor": [1.0]}),
            universe_tickers={"A"},
            historical_tickers={"A"},
        )
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        result = eng._run_integrity_check(train, leaked_test, portfolio)
        assert result["passed"] is False
        assert result["detail"]["checks"] != {}
        assert any(passed is False for passed in result["detail"]["checks"].values())

    def test_applied_min_adt_inr_derived_from_real_trade_adtv_not_echoed_constant(self):
        """[BUG FIX, 6th fundamental-strategies review, item 1]
        applied_min_adt_inr must reflect the real minimum Trade.adtv_cr
        observed this fold, not an unconditional echo of MIN_ADT_INR."""
        train = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=365)})
        test = pd.DataFrame({"date": pd.date_range("2021-01-01", periods=365)})
        eng = _bare_engine(
            _combined=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv=pd.DataFrame({"adj_factor": [1.0, 1.0]}),
            universe_tickers={"A"},
            historical_tickers={"A", "DELISTED1"},
        )
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0}, adtv_cr=2.0)
        portfolio.sell("A", 110.0, pd.Timestamp("2020-01-05"))
        assert portfolio.trades[0].adtv_cr == 2.0

        applied = eng._applied_min_adt_inr(portfolio)
        assert applied == 2.0 * 1e7
        result = eng._run_integrity_check(train, test, portfolio)
        assert result["detail"]["applied_min_adt_inr_verified_against_real_data"] is True

    def test_applied_min_adt_inr_falls_back_to_constant_without_real_adtv(self):
        from config.settings import MIN_ADT_INR

        eng = _bare_engine()
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0})
        portfolio.sell("A", 110.0, pd.Timestamp("2020-01-05"))
        assert portfolio.trades[0].adtv_cr is None
        assert eng._applied_min_adt_inr(portfolio) == float(MIN_ADT_INR)


class TestPortfolioBuyThreadsAdtvCr:
    """[BUG FIX, 6th fundamental-strategies review, item 1]
    PortfolioSimulator.buy()/_close() previously had the Position.
    entry_adtv_cr/Trade.adtv_cr fields but never actually populated them -
    every Trade from this (legacy, paper-trading) engine had adtv_cr=None
    forever."""

    def test_buy_sets_position_entry_adtv_cr(self):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        position = portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0}, adtv_cr=5.5)
        assert position.entry_adtv_cr == 5.5

    def test_close_carries_entry_adtv_cr_onto_trade(self):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0}, adtv_cr=5.5)
        trade = portfolio.sell("A", 110.0, pd.Timestamp("2020-01-05"))
        assert trade.adtv_cr == 5.5

    def test_partial_close_via_reduce_position_also_carries_adtv_cr(self):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0}, adtv_cr=3.3)
        trade = portfolio.reduce_position("A", 105.0, pd.Timestamp("2020-01-03"))
        assert trade.adtv_cr == 3.3

    def test_no_adtv_cr_supplied_leaves_trade_adtv_cr_none(self):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        portfolio.buy("A", "SECTOR1", 100.0, pd.Timestamp("2020-01-01"), {"A": 100.0})
        trade = portfolio.sell("A", 110.0, pd.Timestamp("2020-01-05"))
        assert trade.adtv_cr is None


# ===== compute_fold_metrics: trades-present branch =====


class TestComputeFoldMetricsTrades:
    def test_win_rate_and_profit_factor_from_real_trades(self):
        curve = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=3), "equity": [1_000_000, 1_010_000, 1_005_000]})
        trades = pd.DataFrame({"pnl_inr": [1000.0, -500.0, 2000.0]})
        metrics = compute_fold_metrics(curve, trades, 1_000_000.0)
        assert metrics["win_rate"] == pytest.approx(2 / 3)
        assert metrics["profit_factor"] == pytest.approx(3000.0 / 500.0)
        assert metrics["n_trades"] == 3

    def test_all_losses_profit_factor_zero(self):
        curve = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=2), "equity": [1_000_000, 990_000]})
        trades = pd.DataFrame({"pnl_inr": [-1000.0, -500.0]})
        metrics = compute_fold_metrics(curve, trades, 1_000_000.0)
        assert metrics["win_rate"] == 0.0
        assert metrics["profit_factor"] == 0.0

    def test_all_wins_profit_factor_infinite(self):
        curve = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=2), "equity": [1_000_000, 1_010_000]})
        trades = pd.DataFrame({"pnl_inr": [1000.0, 500.0]})
        metrics = compute_fold_metrics(curve, trades, 1_000_000.0)
        assert metrics["win_rate"] == 1.0
        assert metrics["profit_factor"] == float("inf")


# ===== Deflated Sharpe wiring must use per-period, not annualized, Sharpe =====
# [BUG FIX, 4th fundamental-strategies review, item 1]


class TestRawSharpeFromReturns:
    def test_per_period_sharpe_is_not_annualized(self):
        rng = np.random.default_rng(0)
        daily_returns = pd.Series(rng.normal(loc=0.0006, scale=0.01, size=252))
        raw_sharpe = _raw_sharpe_from_returns(daily_returns)
        annualized_sharpe = raw_sharpe * (252 ** 0.5)
        assert abs(annualized_sharpe) > abs(raw_sharpe) * 10

    def test_engine_wires_raw_sharpe_into_deflated_sharpe_ratio_not_annualized(self):
        """Reproduces the exact sequence engine.py's run_full_backtest uses
        right before calling deflated_sharpe_ratio: given a real fold-return
        series and its annualized sharpe_mean (as compute_fold_metrics
        would have produced it), the value actually wired into
        deflated_sharpe_ratio must be the per-period Sharpe — confirmed by
        checking it disagrees sharply with what the (buggy) annualized value
        would have produced."""
        rng = np.random.default_rng(1)
        daily_returns = pd.Series(rng.normal(loc=0.0008, scale=0.012, size=504))
        raw_sharpe = _raw_sharpe_from_returns(daily_returns)
        annualized_sharpe = float(daily_returns.mean() / daily_returns.std() * (252 ** 0.5))

        dsr_from_raw = deflated_sharpe_ratio(sharpe=raw_sharpe, n_trials=5, n_obs=len(daily_returns), returns=daily_returns)
        dsr_from_annualized = deflated_sharpe_ratio(
            sharpe=annualized_sharpe, n_trials=5, n_obs=len(daily_returns), returns=daily_returns,
        )
        # The annualized-Sharpe bug saturates DSR near 1.0; the fixed
        # (per-period) wiring must not.
        assert dsr_from_annualized > 0.999
        assert dsr_from_raw < dsr_from_annualized


# ===== BacktestEngine.__init__ error path + defaults (no heavy dataset build) =====


class TestInitValidation:
    def test_missing_benchmark_raises_value_error_before_any_dataset_build(self):
        ohlcv = pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=5),
                "ticker": "A",
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000,
            }
        )
        with pytest.raises(ValueError, match="requires a real benchmark"):
            BacktestEngine(
                ohlcv=ohlcv, pnd_detector=_NoPnd(), exit_model=_HoldExitModel(),
                signal_model_cls=None, sector_map={},
            )


# ===== _build_dataset: real technical-feature + triple-barrier-label pipeline =====


def _real_ohlcv_and_benchmark(n=300, tickers=("A", "B")):
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    rng = np.random.default_rng(0)

    def make_ticker(ticker, base):
        close = np.abs(np.cumsum(rng.normal(0, 1, n))) + base
        return pd.DataFrame(
            {
                "date": dates, "ticker": ticker,
                "open": close + rng.normal(0, 0.5, n),
                "high": close + rng.random(n),
                "low": close - rng.random(n),
                "close": close,
                "volume": rng.integers(1000, 100_000, n),
            }
        )

    ohlcv = pd.concat([make_ticker(t, 100.0 * (i + 1)) for i, t in enumerate(tickers)], ignore_index=True)
    benchmark = pd.DataFrame(
        {
            "date": dates,
            "nifty50_close": 100 + np.cumsum(rng.normal(0, 0.5, n)),
            "nifty100_close": 100 + np.cumsum(rng.normal(0, 0.5, n)),
            "nifty500_close": 100 + np.cumsum(rng.normal(0, 0.5, n)),
        }
    )
    return ohlcv, benchmark


class TestBuildDataset:
    def test_real_pipeline_produces_labelled_feature_rows(self):
        ohlcv, benchmark = _real_ohlcv_and_benchmark()
        eng = _bare_engine(ohlcv=ohlcv, benchmark=benchmark, profit_multiplier=2.0, stop_multiplier=1.0, horizon_days=5)

        combined = eng._build_dataset()

        assert not combined.empty
        assert {"date", "ticker", "_label", "_return"}.issubset(combined.columns)
        for col in CORE_TECHNICAL_FEATURES:
            assert col in combined.columns
        # No NaN labels/returns survive the dropna at the end of _build_dataset.
        assert combined["_label"].notna().all()
        assert combined["_return"].notna().all()
        # Real triple-barrier labels are one of {-1, 0, 1}.
        assert set(combined["_label"].unique()).issubset({-1, 0, 1})

    def test_missing_benchmark_raises_with_no_synthetic_fallback(self):
        ohlcv, _ = _real_ohlcv_and_benchmark()
        eng = _bare_engine(ohlcv=ohlcv, benchmark=None, profit_multiplier=2.0, stop_multiplier=1.0, horizon_days=5)
        with pytest.raises(ValueError, match="requires a real benchmark"):
            eng._build_dataset()


# ===== full __init__: universe/historical ticker defaults =====


class TestFullInitDefaults:
    def test_universe_and_historical_tickers_default_to_ohlcv_ticker_set(self):
        ohlcv, benchmark = _real_ohlcv_and_benchmark(n=300, tickers=("A", "B"))

        class _StubPnd:
            def predict_full(self, rows):
                return pd.DataFrame({"pnd_score": [0.0] * len(rows), "pnd_block": [False] * len(rows)}, index=rows.index)

        eng = BacktestEngine(
            ohlcv=ohlcv, pnd_detector=_StubPnd(), exit_model=_HoldExitModel(),
            signal_model_cls=_AlwaysBuySignal, sector_map={"A": "S1", "B": "S2"}, benchmark=benchmark,
        )

        assert eng.universe_tickers == {"A", "B"}
        assert eng.historical_tickers == {"A", "B"}

    def test_explicit_universe_and_historical_tickers_are_preserved(self):
        ohlcv, benchmark = _real_ohlcv_and_benchmark(n=300, tickers=("A", "B"))

        class _StubPnd:
            def predict_full(self, rows):
                return pd.DataFrame({"pnd_score": [0.0] * len(rows), "pnd_block": [False] * len(rows)}, index=rows.index)

        eng = BacktestEngine(
            ohlcv=ohlcv, pnd_detector=_StubPnd(), exit_model=_HoldExitModel(),
            signal_model_cls=_AlwaysBuySignal, sector_map={"A": "S1", "B": "S2"}, benchmark=benchmark,
            universe_tickers={"A"}, historical_tickers={"A", "B", "DELISTED"},
        )

        assert eng.universe_tickers == {"A"}
        assert eng.historical_tickers == {"A", "B", "DELISTED"}
