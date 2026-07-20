"""tests/unit/test_momentum_adapter.py — backtest/adapters/momentum_adapter.py.

Deterministic-fixture tests for rank-rotation mechanics (same convention
as tests/unit/test_momentum_backtest.py's _flat_price_panel — orchestration
logic, not market realism). TestRealPricePanelIntegration exercises the
adapter against real OHLCV via features/momentum_signal.load_price_panel,
per the No-Mock-Data Policy.
"""


import pandas as pd
import pytest

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.horizon import HorizonBucket


def _panel(prices: dict, n_days: int, start="2020-01-01"):
    """prices: {ticker: [close, close, ...]} of length n_days, one row per business day."""
    dates = pd.bdate_range(start, periods=n_days)
    return pd.DataFrame({t: v for t, v in prices.items()}, index=dates)


class TestInitialization:
    def test_rejects_non_positive_top_n(self):
        with pytest.raises(ValueError):
            MomentumAdapter(price_panel=pd.DataFrame(), top_n=0)


class TestGenerateSignals:
    def test_empty_momentum_series_returns_no_signals_not_fabricated(self):
        panel = _panel({"A": [100.0] * 3}, 3)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6)  # lookback >> panel length
        signals = adapter.generate_signals(["A"], panel.index[-1].date(), HorizonBucket.D21)
        assert signals == []

    def test_first_call_buys_top_n_tickers(self):
        # 200 trading days, enough for a 6-month (~126d) lookback; C has the highest trailing return
        n_days = 200
        prices = {
            "A": [100.0] * n_days,
            "B": [100.0 + i * 0.1 for i in range(n_days)],
            "C": [100.0 + i * 0.5 for i in range(n_days)],  # strongest uptrend
        }
        panel = _panel(prices, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6)
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "B", "C"], as_of, HorizonBucket.D21)
        buys = {s.ticker for s in signals if s.action == "buy"}
        assert buys == {"B", "C"}  # top 2 by trailing momentum
        assert all(s.action == "buy" for s in signals)  # nothing held yet, so no sells

    def test_second_call_rotates_out_tickers_that_fell_out_of_top_n(self):
        n_days = 200
        # A stays flat, B trends up strongly, C trends up moderately then reverses
        prices = {
            "A": [100.0 + i * 0.05 for i in range(n_days)],
            "B": [100.0 + i * 0.5 for i in range(n_days)],
            "C": [100.0 + i * 0.3 for i in range(n_days // 2)] + [100.0 + (n_days // 2) * 0.3 - i * 0.4 for i in range(n_days - n_days // 2)],
        }
        panel = _panel(prices, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=1)  # short lookback reacts to the reversal
        first_date = panel.index[n_days // 2 + 20].date()
        adapter.generate_signals(["A", "B", "C"], first_date, HorizonBucket.D21)
        held_after_first = set(adapter._currently_held)

        later_date = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "B", "C"], later_date, HorizonBucket.D21)
        sells = {s.ticker for s in signals if s.action == "sell"}
        # whatever fell out of the new top-2 relative to what was held should be sold
        assert sells == held_after_first - adapter._currently_held

    def test_no_churn_when_top_n_unchanged_between_calls(self):
        n_days = 200
        prices = {"A": [100.0] * n_days, "B": [100.0 + i * 0.5 for i in range(n_days)]}
        panel = _panel(prices, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6)
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B"], as_of, HorizonBucket.D21)
        second = adapter.generate_signals(["A", "B"], as_of, HorizonBucket.D21)
        assert second == []  # same top-2 both times ({A, B} is the whole universe) -> no buys or sells


class TestFeatureVector:
    def test_reports_trailing_momentum_and_membership(self):
        n_days = 200
        prices = {"A": [100.0] * n_days, "B": [100.0 + i * 0.5 for i in range(n_days)]}
        panel = _panel(prices, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6)
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B"], as_of, HorizonBucket.D21)
        fv_b = adapter.feature_vector("B", as_of)
        assert fv_b["in_top_n"] is True
        assert fv_b["trailing_momentum"] > 0

    def test_ticker_with_no_momentum_value_reports_none_not_zero(self):
        panel = _panel({"A": [100.0] * 3}, 3)
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6)
        fv = adapter.feature_vector("GHOST", panel.index[-1].date())
        assert fv["trailing_momentum"] is None


class TestOrchestratorIntegration:
    """Proves the adapter actually plugs into backtest/core/engine.py's
    BacktestOrchestrator end to end, not just in isolation."""

    def test_runs_end_to_end_through_the_shared_orchestrator(self):
        from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig
        from backtest.core.run_context import BacktestRun

        n_days = 250
        dates = pd.bdate_range("2020-01-01", periods=n_days)
        prices = {
            "A": [100.0 + i * 0.05 for i in range(n_days)],
            "B": [100.0 + i * 0.4 for i in range(n_days)],
            "C": [100.0 + i * 0.2 for i in range(n_days)],
        }
        panel = pd.DataFrame(prices, index=dates)
        volumes = pd.DataFrame({t: [10_000.0] * n_days for t in prices}, index=dates)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6, volume_panel=volumes)
        run = BacktestRun(
            channel="momentum", strategy_id="mom_21d_top2", horizon_bucket=HorizonBucket.D21,
            mode="backtest", universe_spec="test", start_date=dates[0].date(), end_date=dates[-1].date(),
            capital_mode="lump", initial_capital=1_000_000.0,
        )
        price_map = {(t, d.date()): panel.loc[d, t] for t in panel.columns for d in panel.index}
        config = OrchestratorConfig(
            trading_days=dates, universe_provider=lambda d: list(panel.columns),
            price_lookup=lambda t, d: price_map.get((t, d)),
        )
        result = BacktestOrchestrator().run(run, adapter, config)
        assert result.metrics["final_capital"] > 0
        assert result.metrics["n_distinct_tickers_traded"] == 2  # B and C, the consistent top-2
        assert result.data_gaps == []


class TestAdtvCrPopulation:
    """2026-07-20 (Truthful Review Gap #6 fix): Signal.adtv_cr is populated
    from a real trailing average-daily-traded-value formula when
    volume_panel is supplied — same formula as MomentumBacktester._adtv_cr,
    never fabricated."""

    def test_no_volume_panel_leaves_adtv_cr_none_unchanged_behavior(self):
        n_days = 200
        prices = {"A": [100.0] * n_days, "C": [100.0 + i * 0.5 for i in range(n_days)]}
        panel = _panel(prices, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6)
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "C"], as_of, HorizonBucket.D21)
        assert all(s.adtv_cr is None for s in signals)

    def test_volume_panel_populates_a_real_adtv_cr(self):
        n_days = 200
        prices = {"A": [100.0] * n_days, "C": [100.0 + i * 0.5 for i in range(n_days)]}
        panel = _panel(prices, n_days)
        # C: 10,000 shares/day @ ~100-200 -> real ADTV in the low crores.
        volumes = _panel({"A": [10_000.0] * n_days, "C": [10_000.0] * n_days}, n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6, volume_panel=volumes)
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "C"], as_of, HorizonBucket.D21)
        buys = [s for s in signals if s.action == "buy"]
        assert buys and all(s.adtv_cr is not None and s.adtv_cr > 0 for s in buys)

    def test_ticker_missing_from_volume_panel_gets_none_not_fabricated(self):
        n_days = 200
        prices = {"A": [100.0] * n_days, "C": [100.0 + i * 0.5 for i in range(n_days)]}
        panel = _panel(prices, n_days)
        volumes = _panel({"A": [10_000.0] * n_days}, n_days)  # C has no real volume data
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6, volume_panel=volumes)
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "C"], as_of, HorizonBucket.D21)
        c_signals = [s for s in signals if s.ticker == "C"]
        assert c_signals and all(s.adtv_cr is None for s in c_signals)


class TestRealPricePanelIntegration:
    """No-Mock-Data Policy: exercises the adapter against real OHLCV rather
    than a fabricated panel, catching real data-shape issues (gaps,
    non-uniform calendars, delistings) a synthetic panel would hide."""

    def test_real_price_panel_produces_a_valid_signal_list(self):
        import duckdb

        from features.momentum_signal import load_price_panel

        try:
            con = duckdb.connect("datastore/normalised/alphalens.duckdb", read_only=True)
        except duckdb.IOException:
            pytest.skip("alphalens.duckdb locked by another process — skipping real-data check")
            return

        try:
            panel = load_price_panel(con, ["RELIANCE", "TCS", "INFY", "HDFCBANK"], "2018-01-01", "2019-12-31")
        finally:
            con.close()

        if panel.empty or len(panel) < 150:
            pytest.skip("insufficient real price panel rows available in this environment")
            return

        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6)
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(list(panel.columns), as_of, HorizonBucket.D21)
        assert all(s.ticker in panel.columns for s in signals)
        assert len(adapter._currently_held) <= 2
