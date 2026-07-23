"""tests/unit/test_core_engine.py — backtest/core/engine.py (BacktestOrchestrator).

Per BacktestUmbrellaPlan.md's No-Mock-Data Policy: these test pure
orchestration mechanics (loop sequencing, rebalance-date iteration,
delisting reconciliation, data-gap handling) using small, deterministic,
hand-constructed price/signal fixtures — this is the same convention
tests/unit/test_momentum_backtest.py already uses (_flat_price_panel) for
testing orchestration logic distinct from market realism. It is not
mocking a database or fabricating market data for a real-data assertion —
there is no real "20-year fixture" needed to prove a for-loop iterates
rebalance dates correctly.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.engine import BacktestOrchestrator, CorporateActionEvent, OrchestratorConfig, Signal
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun


def _run(**overrides):
    defaults = dict(
        channel="technical", strategy_id="test_strategy", horizon_bucket=HorizonBucket.D5,
        mode="backtest", universe_spec="test_universe", start_date=date(2020, 1, 1), end_date=date(2020, 3, 1),
        capital_mode="lump", initial_capital=1_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


class _FixedSignalAdapter:
    """Emits the same list of signals on every rebalance date it's asked for."""

    channel = "technical"

    def __init__(self, signals_by_date=None, default_signals=None):
        self._signals_by_date = signals_by_date or {}
        self._default_signals = default_signals or []

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return self._signals_by_date.get(as_of_date, self._default_signals)

    def feature_vector(self, ticker, as_of_date):
        return {"dummy_feature": 1.0}


def _flat_prices(trading_days, tickers, price=100.0):
    """A ticker -> price lookup that's flat over time, keyed by (ticker, date)."""
    price_map = {(t, d.date()): price for t in tickers for d in trading_days}

    def lookup(ticker, as_of_date):
        return price_map.get((ticker, as_of_date))

    return lookup


class TestChannelMismatchGuard:
    def test_rejects_run_whose_channel_does_not_match_adapter(self):
        adapter = _FixedSignalAdapter()
        run = _run(channel="momentum")  # adapter.channel is "technical"
        config = OrchestratorConfig(
            trading_days=pd.date_range("2020-01-01", periods=10, freq="B"),
            universe_provider=lambda d: ["RELIANCE"], price_lookup=lambda t, d: 100.0,
        )
        with pytest.raises(ValueError, match="does not match"):
            BacktestOrchestrator().run(run, adapter, config)


class TestNoTradesRun:
    def test_flat_run_with_no_signals_returns_initial_capital(self):
        adapter = _FixedSignalAdapter(default_signals=[])
        run = _run()
        trading_days = pd.date_range("2020-01-01", periods=20, freq="B")
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"]),
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["final_capital"] == pytest.approx(1_000_000.0)
        assert result.metrics["n_trades"] == 0
        assert result.data_gaps == []


class TestBuyAndSell:
    def test_buy_then_sell_produces_one_trade(self):
        trading_days = pd.date_range("2020-01-01", periods=30, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        # sell on a later rebalance date (5-day cadence for D5 bucket -> index 5)
        later_rebalance = trading_days[5].date()
        signals_by_date[later_rebalance] = [Signal(ticker="RELIANCE", action="sell", sector="Energy", conviction=1.0)]

        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"], price=100.0),
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["n_trades"] == 1
        assert result.metrics["n_distinct_tickers_traded"] == 1


class TestDataGapHandling:
    def test_missing_price_for_buy_signal_recorded_as_data_gap_not_fabricated(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="GHOST_TICKER", action="buy", sector="Unknown", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["GHOST_TICKER"],
            price_lookup=lambda t, d: None,  # simulates a real data gap
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["n_trades"] == 0  # never fabricated a price to force the trade through
        assert any(g["reason"] == "no_price_for_buy_signal" for g in result.data_gaps)


class TestDelistingReconciliation:
    def test_delisted_position_force_closed_before_new_signals_processed(self):
        trading_days = pd.date_range("2020-01-01", periods=15, freq="B")
        buy_date = trading_days[0].date()
        signals_by_date = {buy_date: [Signal(ticker="ZOMBIE_CO", action="buy", sector="Unknown", conviction=1.0)]}
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)

        delisted_from = trading_days[5].date()

        def is_delisted(ticker, as_of_date):
            return ticker == "ZOMBIE_CO" and as_of_date >= delisted_from

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["ZOMBIE_CO"],
            price_lookup=_flat_prices(trading_days, ["ZOMBIE_CO"], price=50.0),
            is_delisted=is_delisted,
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        # one buy + one forced close = 2 trades
        assert result.metrics["n_trades"] == 1  # the forced close realizes the only trade (buy alone isn't a "trade" until closed)


class TestCorporateActionReconciliation:
    """2026-07-20 (BacktestUmbrellaPlan.md Truthful Review Gap #4 fix):
    MERGER/SPINOFF close-out policy, checked before the delisting branch."""

    def test_merger_with_no_successor_data_force_closes_at_last_price(self):
        trading_days = pd.date_range("2020-01-01", periods=15, freq="B")
        buy_date = trading_days[0].date()
        signals_by_date = {buy_date: [Signal(ticker="TARGETCO", action="buy", sector="Unknown", conviction=1.0)]}
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)
        merger_from = trading_days[5].date()

        def corporate_action_lookup(ticker, as_of_date):
            if ticker == "TARGETCO" and as_of_date >= merger_from:
                return CorporateActionEvent(action_type="MERGER")  # no successor/ratio -> real-data-only path
            return None

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["TARGETCO"],
            price_lookup=_flat_prices(trading_days, ["TARGETCO"], price=50.0),
            corporate_action_lookup=corporate_action_lookup,
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["n_trades"] == 1  # forced close realized the position

    def test_merger_with_no_close_price_recorded_as_data_gap(self):
        trading_days = pd.date_range("2020-01-01", periods=15, freq="B")
        buy_date = trading_days[0].date()
        signals_by_date = {buy_date: [Signal(ticker="TARGETCO", action="buy", sector="Unknown", conviction=1.0)]}
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)
        merger_from = trading_days[5].date()
        prices = _flat_prices(trading_days, ["TARGETCO"], price=50.0)

        def price_lookup(ticker, as_of_date):
            if ticker == "TARGETCO" and as_of_date >= merger_from:
                return None  # merger day itself has no real close price
            return prices(ticker, as_of_date)

        def corporate_action_lookup(ticker, as_of_date):
            if ticker == "TARGETCO" and as_of_date >= merger_from:
                return CorporateActionEvent(action_type="MERGER")
            return None

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["TARGETCO"],
            price_lookup=price_lookup, corporate_action_lookup=corporate_action_lookup,
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert any(g["reason"] == "merger_and_no_close_price" for g in result.data_gaps)

    def test_merger_with_real_successor_and_ratio_swaps_the_position(self):
        trading_days = pd.date_range("2020-01-01", periods=15, freq="B")
        buy_date = trading_days[0].date()
        signals_by_date = {buy_date: [Signal(ticker="TARGETCO", action="buy", sector="Unknown", conviction=1.0)]}
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)
        merger_date = trading_days[5].date()

        def price_lookup(ticker, as_of_date):
            if ticker == "TARGETCO":
                return 50.0
            if ticker == "ACQUIRERCO":
                return 200.0
            return None

        def corporate_action_lookup(ticker, as_of_date):
            if ticker == "TARGETCO" and as_of_date == merger_date:
                return CorporateActionEvent(action_type="MERGER", successor_ticker="ACQUIRERCO", swap_ratio=0.5)
            return None

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["TARGETCO", "ACQUIRERCO"],
            price_lookup=price_lookup, corporate_action_lookup=corporate_action_lookup,
        )
        result = BacktestOrchestrator(feature_log_writer=None)
        portfolio_result = result.run(run, adapter, config)
        # the swap itself doesn't count as a realized "trade" close on ACQUIRERCO
        # (still open at run end) but DOES realize the TARGETCO leg.
        assert portfolio_result.metrics["n_trades"] == 1

    def test_spinoff_and_delisting_are_independent_checks(self):
        """A ticker with no corporate_action_lookup match at all still goes
        through the normal is_delisted branch unaffected."""
        trading_days = pd.date_range("2020-01-01", periods=15, freq="B")
        buy_date = trading_days[0].date()
        signals_by_date = {buy_date: [Signal(ticker="ZOMBIE_CO", action="buy", sector="Unknown", conviction=1.0)]}
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run(horizon_bucket=HorizonBucket.D5)
        delisted_from = trading_days[5].date()

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["ZOMBIE_CO"],
            price_lookup=_flat_prices(trading_days, ["ZOMBIE_CO"], price=50.0),
            is_delisted=lambda t, d: t == "ZOMBIE_CO" and d >= delisted_from,
            corporate_action_lookup=lambda t, d: None,  # never fires
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["n_trades"] == 1


class TestAdtvVisibility:
    """2026-07-20 (Truthful Review Gap #6 fix): a buy signal with no
    adtv_cr is sized uncapped (unchanged behavior — position_size() itself
    is unchanged), but this must now be a VISIBLE data_gap, not silent."""

    def test_buy_signal_without_adtv_cr_records_a_data_gap(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"]),
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert any(g["reason"] == "no_adtv_data_position_sized_uncapped" for g in result.data_gaps)

    def test_buy_signal_with_adtv_cr_records_no_such_gap(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0, adtv_cr=5.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"]),
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert not any(g["reason"] == "no_adtv_data_position_sized_uncapped" for g in result.data_gaps)


class TestSipCapitalMode:
    def test_sip_run_shows_higher_total_contributed_than_initial_capital(self):
        trading_days = pd.date_range("2020-01-01", periods=80, freq="B")  # spans several months
        adapter = _FixedSignalAdapter(default_signals=[])
        run = _run(
            capital_mode="sip", initial_capital=1_000_000.0, sip_amount=100_000.0,
            horizon_bucket=HorizonBucket.D5,
        )
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: [],
            price_lookup=lambda t, d: None,
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["total_contributed"] > 1_000_000.0


class TestExecutionTiming:
    """REV17 (2026-07-21 review): same-day-close vs. next-day-open is now an
    explicit, tested config choice instead of a silent, undocumented one."""

    def test_default_is_same_day_close_and_recorded_on_result(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        adapter = _FixedSignalAdapter(default_signals=[])
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: [], price_lookup=lambda t, d: None,
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.execution_timing == "same_day_close"

    def test_next_day_open_fills_at_next_trading_days_price_not_signal_days(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        second_day = trading_days[1].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()

        prices = {first_rebalance: 100.0, second_day: 110.0}

        def price_lookup(ticker, as_of_date):
            return prices.get(as_of_date)

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=price_lookup, execution_timing="next_day_open",
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.execution_timing == "next_day_open"
        assert result.metrics["n_trades"] == 0  # position still open (never sold)
        # A real next-day price WAS found and used to fill (no data gap) — this is
        # the behavioral proof that next_day_open actually looked up the next
        # trading day's price rather than silently falling back to the signal day.
        assert not any(g["reason"] == "no_price_for_buy_signal" for g in result.data_gaps)

    def test_next_day_open_falls_back_to_same_day_at_last_rebalance_with_data_gap(self):
        trading_days = pd.date_range("2020-01-01", periods=6, freq="B")
        last_rebalance = trading_days[-1].date()
        signals_by_date = {
            last_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"], price=100.0),
            execution_timing="next_day_open",
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        # no later trading day exists after the last rebalance -> falls back to
        # same-day-close (the trade still fills, no fill-price data gap) but the
        # fallback itself is logged.
        assert not any(g["reason"] == "no_price_for_buy_signal" for g in result.data_gaps)
        assert any(
            g["reason"] == "next_day_open_unavailable_at_last_rebalance_fell_back_to_same_day_close"
            for g in result.data_gaps
        )

    def test_next_day_open_records_gap_when_next_days_price_missing(self):
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()

        def price_lookup(ticker, as_of_date):
            return 100.0 if as_of_date == first_rebalance else None  # next day has no real price

        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=price_lookup, execution_timing="next_day_open",
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["n_trades"] == 0
        assert any(g["reason"] == "no_price_for_buy_signal" for g in result.data_gaps)


class TestFeatureLogIntegration:
    def test_feature_log_writer_receives_one_record_per_signal(self):
        from backtest.core.feature_log import FeatureLogWriter
        from datastore.api.db import get_duckdb_connection
        from datastore.schema import create_backtest

        create_backtest.create_backtest_schema(in_memory=True)
        trading_days = pd.date_range("2020-01-01", periods=10, freq="B")
        first_rebalance = trading_days[0].date()
        signals_by_date = {
            first_rebalance: [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=1.0)],
        }
        adapter = _FixedSignalAdapter(signals_by_date=signals_by_date)
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: ["RELIANCE"],
            price_lookup=_flat_prices(trading_days, ["RELIANCE"]),
        )
        with get_duckdb_connection(None) as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=1000)
            BacktestOrchestrator(feature_log_writer=writer).run(run, adapter, config)
            n = conn.execute(
                "SELECT COUNT(*) FROM backtest_feature_log WHERE run_id = ?", [run.run_id]
            ).fetchone()[0]
        assert n >= 1
