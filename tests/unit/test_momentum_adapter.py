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


def _rising_panel(n_days=200, start="2020-01-01"):
    """A/B/C/D with strictly ordered trailing momentum (D > C > B > A),
    the workhorse fixture for the ported-filter tests below."""
    prices = {
        "A": [100.0] * n_days,
        "B": [100.0 + i * 0.1 for i in range(n_days)],
        "C": [100.0 + i * 0.3 for i in range(n_days)],
        "D": [100.0 + i * 0.5 for i in range(n_days)],
    }
    return _panel(prices, n_days, start)


class TestGracePeriod:
    """2026-08-05 Phase 1 item 2: dropout no longer sells immediately —
    grace is decided by momentum_backtest.decide_grace_transitions."""

    def test_dropout_is_not_sold_until_grace_is_exhausted(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6, grace_cycles=2)
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert adapter._currently_held == {"C", "D"}

        # Shrink the target to {D} only: C drops out and enters grace (2).
        adapter.top_n = 1
        first = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert [s.action for s in first] == []  # no sell yet — grace 2
        assert adapter._held_grace["C"] == 2

        second = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert second == []  # grace 2 -> 1
        assert adapter._held_grace["C"] == 1

        third = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        # grace 1 -> 0, which is exhausted: the sell fires on this call
        assert [(s.ticker, s.action) for s in third] == [("C", "sell")]
        assert "C" not in adapter._currently_held

    def test_reentering_target_resets_grace_with_no_sell_rebuy_churn(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6, grace_cycles=2)
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        adapter.top_n = 1
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert adapter._held_grace["C"] == 2

        adapter.top_n = 2  # C is back in the top-N
        signals = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert signals == []  # no sell, and no re-buy either — it was never sold
        assert adapter._held_grace["C"] is None  # core again

    def test_grace_cycles_zero_sells_on_the_next_rebalance(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6, grace_cycles=0)
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        adapter.top_n = 1
        signals = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert [(s.ticker, s.action) for s in signals] == [("C", "sell")]


class TestAdtvFloor:
    def test_illiquid_ticker_is_excluded_from_selection(self):
        n_days = 200
        panel = _rising_panel(n_days)
        # D is the strongest but barely trades; C is liquid.
        volumes = _panel(
            {"A": [10_000.0] * n_days, "B": [10_000.0] * n_days,
             "C": [10_000.0] * n_days, "D": [1.0] * n_days},
            n_days,
        )
        adapter = MomentumAdapter(
            price_panel=panel, volume_panel=volumes, top_n=1, lookback_months=6, min_adtv_cr=0.001,
        )
        as_of = panel.index[-1].date()
        buys = {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)}
        assert buys == {"C"}

    def test_ticker_with_no_volume_data_is_excluded_never_assumed_liquid(self):
        n_days = 200
        panel = _rising_panel(n_days)
        volumes = _panel({"C": [10_000.0] * n_days}, n_days)  # D has no volume rows at all
        adapter = MomentumAdapter(
            price_panel=panel, volume_panel=volumes, top_n=1, lookback_months=6, min_adtv_cr=0.001,
        )
        as_of = panel.index[-1].date()
        buys = {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)}
        assert buys == {"C"}

    def test_floor_never_forces_a_sell_of_an_already_held_ticker(self):
        n_days = 200
        panel = _rising_panel(n_days)
        volumes = _panel({t: [10_000.0] * n_days for t in "ABCD"}, n_days)
        adapter = MomentumAdapter(
            price_panel=panel, volume_panel=volumes, top_n=1, lookback_months=6, grace_cycles=5,
        )
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert adapter._currently_held == {"D"}
        adapter.min_adtv_cr = 1e9  # nothing can pass now
        signals = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert signals == []  # D enters grace, is NOT immediately liquidated
        assert adapter._currently_held == {"D"}


class TestCircuitLock:
    def _panel_with_spike(self, n_days=200):
        prices = {
            "A": [100.0] * n_days,
            "D": [100.0 + i * 0.5 for i in range(n_days)],
        }
        prices["D"][-1] = prices["D"][-2] * 1.30  # +30% on the final day
        return _panel(prices, n_days)

    def test_locked_ticker_is_not_bought(self):
        panel = self._panel_with_spike()
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6, circuit_band_pct=0.20)
        as_of = panel.index[-1].date()
        buys = {s.ticker for s in adapter.generate_signals(["A", "D"], as_of, HorizonBucket.D21)}
        assert "D" not in buys

    def test_locked_ticker_is_not_force_sold_and_sells_once_unlocked(self):
        panel = self._panel_with_spike()
        adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6, grace_cycles=0)
        earlier = panel.index[-5].date()
        adapter.generate_signals(["A", "D"], earlier, HorizonBucket.D21)
        assert adapter._currently_held == {"D"}

        adapter.circuit_band_pct = 0.20
        as_of = panel.index[-1].date()
        # On the spike day D is dropped from the pool (locked) so grace hits 0,
        # but the sell is withheld because the close isn't trustworthy.
        signals = adapter.generate_signals(["A", "D"], as_of, HorizonBucket.D21)
        assert all(s.ticker != "D" for s in signals)
        assert "D" in adapter._currently_held

        adapter.circuit_band_pct = None
        later = adapter.generate_signals(["A"], as_of, HorizonBucket.D21)
        assert ("D", "sell") in [(s.ticker, s.action) for s in later]
        assert "D" not in adapter._currently_held


class TestDowntrendFilter:
    def test_sharply_reversing_ticker_is_excluded_from_selection(self):
        n_days = 200
        # D has the best 6-month momentum but has fallen hard over the last 20 days.
        d = [100.0 + i * 3.0 for i in range(n_days - 20)]
        d += [d[-1] * (1 - 0.01 * (i + 1)) for i in range(20)]  # ~-18% over the window
        prices = {"A": [100.0] * n_days, "C": [100.0 + i * 0.3 for i in range(n_days)], "D": d}
        panel = _panel(prices, n_days)
        as_of = panel.index[-1].date()

        unfiltered = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6)
        assert {s.ticker for s in unfiltered.generate_signals(["A", "C", "D"], as_of, HorizonBucket.D21)} == {"D"}

        filtered = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6,
            downtrend_filter_pct=0.05, downtrend_lookback_days=20,
        )
        assert {s.ticker for s in filtered.generate_signals(["A", "C", "D"], as_of, HorizonBucket.D21)} == {"C"}

    def test_ticker_without_short_window_history_stays_eligible(self):
        n_days = 200
        panel = _rising_panel(n_days)
        # Blank out D's recent history: no short-window return can be computed.
        panel.loc[panel.index[-30:], "D"] = float("nan")
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6,
            downtrend_filter_pct=0.05, downtrend_lookback_days=20,
        )
        # D's own long-window momentum is also unavailable, so C should win —
        # the point is that the filter itself never raises or excludes on NaN.
        signals = adapter.generate_signals(["A", "B", "C", "D"], panel.index[-1].date(), HorizonBucket.D21)
        assert {s.ticker for s in signals} == {"C"}


class TestQualityGate:
    def test_failing_f_score_is_excluded_and_missing_scores_pass(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6,
            quality_scores={"D": {"f_score": 2.0}},  # C absent entirely
            quality_gate={"min_f_score": 5},
        )
        as_of = panel.index[-1].date()
        assert {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"C"}

    def test_failing_m_score_is_excluded(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6,
            quality_scores={"D": {"m_score": -1.0}},  # above the -1.78 manipulator threshold
            quality_gate={"max_m_score": -1.78},
        )
        as_of = panel.index[-1].date()
        assert {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"C"}

    def test_no_gate_configured_excludes_nothing(self):
        n_days = 200
        panel = _rising_panel(n_days)
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6, quality_scores={"D": {"f_score": 0.0}},
        )
        as_of = panel.index[-1].date()
        assert {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"D"}


class TestRegimeConditioning:
    """Uses the same self-fetched regime_conn pattern TechnicalAdapter has —
    _regime_segments_cache is pre-seeded here so no DB is touched."""

    def _adapter(self, panel, regime, **kw):
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6,
            disable_buys_in_regime={"bear"}, grace_cycles=0, **kw,
        )
        adapter._regime_conn = object()  # non-None so _regime_for_date consults the cache
        adapter._regime_segments_cache = [
            # confirmed_date is NOT NULL in market_regimes and is what the PIT
            # gate (regime_store.regime_known_as_of) keys on — a fixture
            # without it isn't a shape the real table can produce. Set to the
            # panel start so the segment counts as already-confirmed for every
            # date these tests exercise.
            {
                "start_date": panel.index[0].date(),
                "end_date": panel.index[-1].date(),
                "confirmed_date": panel.index[0].date(),
                "regime": regime,
            },
        ]
        return adapter

    def test_new_buys_suppressed_in_a_disabled_regime(self):
        panel = _rising_panel()
        adapter = self._adapter(panel, "bear")
        signals = adapter.generate_signals(["A", "B", "C", "D"], panel.index[-1].date(), HorizonBucket.D21)
        assert signals == []
        assert adapter._currently_held == set()

    def test_buys_allowed_in_a_non_disabled_regime(self):
        panel = _rising_panel()
        adapter = self._adapter(panel, "bull")
        signals = adapter.generate_signals(["A", "B", "C", "D"], panel.index[-1].date(), HorizonBucket.D21)
        assert {s.ticker for s in signals} == {"D"}

    def test_existing_holdings_still_sell_normally_when_buys_are_disabled(self):
        panel = _rising_panel()
        adapter = self._adapter(panel, "bull")
        as_of = panel.index[-1].date()
        adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        assert adapter._currently_held == {"D"}

        adapter._regime_segments_cache[0]["regime"] = "bear"
        adapter.top_n = 1
        # Force D out of the target by ranking a universe it isn't in.
        signals = adapter.generate_signals(["A", "B", "C"], as_of, HorizonBucket.D21)
        assert [(s.ticker, s.action) for s in signals] == [("D", "sell")]


class TestOrthogonalization:
    def test_ranking_uses_the_size_beta_residual_not_raw_momentum(self):
        n_days = 200
        # 12 tickers so orthogonalize_momentum_vs_factors' min_observations=10
        # is satisfied; momentum is a pure linear function of log(market cap),
        # so the residual is ~0 everywhere and ranking must change.
        tickers = [f"T{i}" for i in range(12)]
        prices = {t: [100.0 + i * (0.05 * (n + 1)) for i in range(n_days)] for n, t in enumerate(tickers)}
        panel = _panel(prices, n_days)
        mcaps = pd.DataFrame(
            {t: [float(10 ** (1 + 0.2 * n))] * n_days for n, t in enumerate(tickers)}, index=panel.index,
        )
        as_of = panel.index[-1].date()

        raw = MomentumAdapter(price_panel=panel, top_n=3, lookback_months=6)
        raw_picks = {s.ticker for s in raw.generate_signals(tickers, as_of, HorizonBucket.D21)}
        assert raw_picks == {"T9", "T10", "T11"}  # highest raw momentum

        residual = MomentumAdapter(
            price_panel=panel, top_n=3, lookback_months=6, orthogonalize_vs_size_beta=True,
            market_cap_panel=mcaps, beta_map={t: 1.0 for t in tickers},
        )
        residual_picks = {s.ticker for s in residual.generate_signals(tickers, as_of, HorizonBucket.D21)}
        assert residual_picks != raw_picks

    def test_no_market_cap_panel_silently_falls_back_to_raw_ranking(self):
        panel = _rising_panel()
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6, orthogonalize_vs_size_beta=True,
        )
        as_of = panel.index[-1].date()
        assert {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"D"}


class TestExcludeApproximatedMcap:
    def test_approximated_ticker_is_excluded_when_opted_in(self):
        panel = _rising_panel()
        year_start = str(panel.index[0].date())
        flags = {year_start: {"D": True, "C": False}}
        as_of = panel.index[-1].date()

        off = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=6, approximation_flags=flags)
        assert {s.ticker for s in off.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"D"}

        on = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=6, approximation_flags=flags,
            exclude_approximated_mcap=True,
        )
        assert {s.ticker for s in on.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)} == {"C"}


class TestMinMomentumFloor:
    def test_floor_is_strict_and_never_pads_to_top_n(self):
        n_days = 200
        # A is flat (momentum exactly 0.0), B/C rise.
        panel = _rising_panel(n_days)
        as_of = panel.index[-1].date()
        adapter = MomentumAdapter(price_panel=panel, top_n=4, lookback_months=6, min_momentum=0.0)
        buys = {s.ticker for s in adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)}
        assert buys == {"B", "C", "D"}  # A's 0.0 is NOT > 0.0, and no padding happens
        assert len(buys) < 4


class TestVolumeWeightedSizing:
    def test_size_multiplier_reflects_relative_adtv(self):
        n_days = 200
        panel = _rising_panel(n_days)
        volumes = _panel(
            {"A": [10_000.0] * n_days, "B": [10_000.0] * n_days,
             "C": [10_000.0] * n_days, "D": [30_000.0] * n_days},
            n_days,
        )
        adapter = MomentumAdapter(
            price_panel=panel, volume_panel=volumes, top_n=2, lookback_months=6, volume_weighted=True,
        )
        as_of = panel.index[-1].date()
        signals = adapter.generate_signals(["A", "B", "C", "D"], as_of, HorizonBucket.D21)
        by_ticker = {s.ticker: s.size_multiplier for s in signals}
        assert by_ticker["D"] > 1.0 > by_ticker["C"]
        assert abs((by_ticker["D"] + by_ticker["C"]) / 2 - 1.0) < 1e-9  # mean multiplier is 1.0

    def test_no_multiplier_when_not_opted_in(self):
        panel = _rising_panel()
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6)
        signals = adapter.generate_signals(["A", "B", "C", "D"], panel.index[-1].date(), HorizonBucket.D21)
        assert all(s.size_multiplier is None for s in signals)

    def test_missing_volume_panel_falls_back_to_equal_weight_with_a_warning(self, caplog):
        panel = _rising_panel()
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=6, volume_weighted=True)
        with caplog.at_level("WARNING"):
            signals = adapter.generate_signals(["A", "B", "C", "D"], panel.index[-1].date(), HorizonBucket.D21)
        assert all(s.size_multiplier is None for s in signals)
        assert any("volume_weighted=True but no volume_panel" in r.message for r in caplog.records)


class TestStickyPromotion:
    """2026-08-05 Momentum engine consolidation Phase 3.

    A rank-band universe fixes each year's membership on the first trading
    day of that year, so a holding that GREW out of its band (promoted to a
    smaller-numbered / higher-market-cap band) silently disappears from
    `universe` at the year boundary and would be force-sold by grace
    expiry purely for having done well. With rank_start + yearly_rank_lookup
    supplied, such a holding stays rankable and exits only via the normal
    Exit Criteria. Demoted / delisted holdings get no such treatment.

    Band under test is rank 51-100 (RANK_BANDS band 2), so rank_start=51:
    a rank STRICTLY below 51 is a promotion.
    """

    N_DAYS = 200
    # A: strongest for the first ~150 days, then falls hard.
    # B/C: steady risers, B always ahead of C.
    _PRICES = {
        "A": [100.0 + i * 0.5 for i in range(150)] + [100.0 + 150 * 0.5 - i * 2.0 for i in range(50)],
        "B": [100.0 + i * 0.4 for i in range(200)],
        "C": [100.0 + i * 0.1 for i in range(200)],
    }
    # Keyed before the panel starts, so it's the active year_start on every
    # date under test — same {year_start: {ticker: rank}} shape
    # features.momentum_universe.yearly_rank_lookup_from_rankings produces.
    PROMOTED_LOOKUP = {"2019-01-01": {"A": 10, "B": 60, "C": 70}}
    DEMOTED_LOOKUP = {"2019-01-01": {"A": 150, "B": 60, "C": 70}}

    @pytest.fixture
    def panel(self):
        return _panel(self._PRICES, self.N_DAYS)

    def _adapter(self, panel, lookup, **kwargs):
        return MomentumAdapter(
            price_panel=panel, top_n=2, lookback_months=1,
            rank_start=51, yearly_rank_lookup=lookup, **kwargs,
        )

    def test_promoted_holding_is_not_force_sold_while_still_competitive(self, panel):
        adapter = self._adapter(panel, self.PROMOTED_LOOKUP, grace_cycles=0)
        # Year 1: A is in the band's list and is bought.
        first = panel.index[140].date()
        buys = {s.ticker for s in adapter.generate_signals(["A", "B", "C"], first, HorizonBucket.D21) if s.action == "buy"}
        assert buys == {"A", "B"}

        # Year 2: A has been promoted out of the band, so it is absent from
        # `universe` — but it is still held and still the strongest name.
        second = panel.index[145].date()
        signals = adapter.generate_signals(["B", "C"], second, HorizonBucket.D21)
        assert [s.ticker for s in signals if s.action == "sell"] == []
        # Still a CORE holding (grace None), not merely surviving on grace.
        assert adapter._held_grace["A"] is None

    def test_promoted_holding_still_exits_normally_once_momentum_drops(self, panel):
        adapter = self._adapter(panel, self.PROMOTED_LOOKUP, grace_cycles=0)
        adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        adapter.generate_signals(["B", "C"], panel.index[145].date(), HorizonBucket.D21)
        assert "A" in adapter._currently_held

        # A has now been falling for weeks — it loses the top-2 cut on its
        # own merits and, with grace exhausted, is sold. Sticky eligibility
        # buys a promoted name a fair ranking, never immunity.
        late = panel.index[-1].date()
        signals = adapter.generate_signals(["B", "C"], late, HorizonBucket.D21)
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}
        assert "A" not in adapter._currently_held

    def test_promoted_holding_survives_grace_that_would_otherwise_have_sold_it(self, panel):
        """Direct A/B against an otherwise-identical adapter with the rule
        off — the ONLY difference is rank_start/yearly_rank_lookup."""
        sticky = self._adapter(panel, self.PROMOTED_LOOKUP, grace_cycles=0)
        plain = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=1, grace_cycles=0)
        first, second = panel.index[140].date(), panel.index[145].date()
        for adapter in (sticky, plain):
            adapter.generate_signals(["A", "B", "C"], first, HorizonBucket.D21)

        sticky_sells = {s.ticker for s in sticky.generate_signals(["B", "C"], second, HorizonBucket.D21) if s.action == "sell"}
        plain_sells = {s.ticker for s in plain.generate_signals(["B", "C"], second, HorizonBucket.D21) if s.action == "sell"}
        assert sticky_sells == set()
        assert plain_sells == {"A"}  # dropped purely because it left the band's list

    def test_demoted_holding_gets_no_special_treatment(self, panel):
        # Same setup, but A's rank is WORSE than the band start -> demoted.
        adapter = self._adapter(panel, self.DEMOTED_LOOKUP, grace_cycles=0)
        adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        signals = adapter.generate_signals(["B", "C"], panel.index[145].date(), HorizonBucket.D21)
        # Sold on grace expiry exactly as it would have been before Phase 3,
        # despite still having the strongest momentum of the three.
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}
        assert "A" not in adapter._currently_held

    def test_holding_with_no_rank_at_all_gets_no_special_treatment(self, panel):
        # A dropped out of the tracked ranking entirely (delisted / fell
        # below MAX_TRACKED_RANK) — never assigned a fabricated rank.
        adapter = self._adapter(panel, {"2019-01-01": {"B": 60, "C": 70}}, grace_cycles=0)
        adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        signals = adapter.generate_signals(["B", "C"], panel.index[145].date(), HorizonBucket.D21)
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}

    def test_promoted_ticker_already_exited_is_never_rebought(self, panel):
        adapter = self._adapter(panel, self.PROMOTED_LOOKUP, grace_cycles=0)
        adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        adapter.generate_signals(["B", "C"], panel.index[145].date(), HorizonBucket.D21)
        adapter.generate_signals(["B", "C"], panel.index[-1].date(), HorizonBucket.D21)
        assert "A" not in adapter._currently_held  # fully exited

        # A is STILL promoted (rank 10) — but sticky eligibility is a
        # hold-only rule, never a new-buy source, so a fully-exited name
        # cannot re-enter through it while it's outside `universe`.
        for idx in (60, 100, 140):
            signals = adapter.generate_signals(["B", "C"], panel.index[idx].date(), HorizonBucket.D21)
            assert "A" not in {s.ticker for s in signals}
            assert "A" not in adapter._currently_held

    def test_promoted_ticker_still_in_universe_is_bought_normally(self, panel):
        """Sanity check on the "by construction" argument: the rule only
        ever ADDS held tickers, so a promoted ticker that is independently
        present in `universe` is unaffected and buys as usual."""
        adapter = self._adapter(panel, self.PROMOTED_LOOKUP, grace_cycles=0)
        signals = adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        assert "A" in {s.ticker for s in signals if s.action == "buy"}

    def test_both_params_none_reproduces_phase1_behavior_exactly(self, panel):
        """Regression guard: omitting rank_start/yearly_rank_lookup must
        produce a byte-identical signal sequence to an adapter built before
        Phase 3 existed."""
        with_defaults = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=1, grace_cycles=1)
        explicit_none = MomentumAdapter(
            price_panel=panel, top_n=2, lookback_months=1, grace_cycles=1,
            rank_start=None, yearly_rank_lookup=None,
        )
        dates = [panel.index[i].date() for i in (140, 145, 160, 180, 199)]
        universes = [["A", "B", "C"], ["B", "C"], ["B", "C"], ["A", "B", "C"], ["B", "C"]]

        def _trace(adapter):
            out = []
            for as_of, universe in zip(dates, universes):
                signals = adapter.generate_signals(universe, as_of, HorizonBucket.D21)
                out.append(sorted((s.ticker, s.action) for s in signals))
            return out

        assert _trace(explicit_none) == _trace(with_defaults)

    def test_rule_is_inert_without_a_rank_lookup(self, panel):
        """rank_start alone (no lookup) must not change anything."""
        adapter = MomentumAdapter(price_panel=panel, top_n=2, lookback_months=1, grace_cycles=0, rank_start=51)
        adapter.generate_signals(["A", "B", "C"], panel.index[140].date(), HorizonBucket.D21)
        signals = adapter.generate_signals(["B", "C"], panel.index[145].date(), HorizonBucket.D21)
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}


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


@pytest.fixture(autouse=True)
def _a94_ledger_never_touches_the_real_db(tmp_path, monkeypatch):
    """A94: OrchestratorConfig.persist_signals defaults True, so any run in
    this module now writes to strategy_signals. Project policy forbids a
    test writing to the real DuckDB even transiently — redirect the default
    path instead of relying on each test to opt out."""
    import config.settings as settings

    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", tmp_path / "a94_ledger.duckdb")


class TestAsymmetricExitRank:
    """exit_rank (ML40, 2026-08-14) — the last selection-side knob that lived
    only in MomentumBacktester, now shared via
    features.momentum_strategy.keep_set_for_exit.

    Fixture: 4 tickers whose 1-day trailing returns rank D > C > B > A. With
    top_n=1 the target set is always {D}, so B/C's fate is decided purely by
    whether exit_rank keeps them out of grace.
    """

    def _ranked_panel(self):
        # Each ticker rises at a distinct constant rate, so the 1-day
        # trailing-return ranking is stable and D > C > B > A every day.
        n = 6
        return _panel({
            "A": [100.0 * (1.001 ** i) for i in range(n)],
            "B": [100.0 * (1.002 ** i) for i in range(n)],
            "C": [100.0 * (1.003 ** i) for i in range(n)],
            "D": [100.0 * (1.004 ** i) for i in range(n)],
        }, n)

    def _run(self, exit_rank, cycles=3):
        panel = self._ranked_panel()
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=1, grace_cycles=0, exit_rank=exit_rank,
        )
        adapter.lookback_days = 1  # 1-day momentum so the short fixture has real history
        universe = ["A", "B", "C", "D"]
        # Seed a holding in B, which is rank 3 of 4 — inside exit_rank=3 but
        # outside top_n=1.
        adapter._held_grace = {"B": None}
        emitted = []
        for i in range(1, cycles + 1):
            emitted.append(adapter.generate_signals(universe, panel.index[i].date(), HorizonBucket.D21))
        return adapter, emitted

    def test_held_name_inside_the_exit_band_is_not_sold(self):
        """B ranks 3rd; with exit_rank=3 it stays 'kept' and never enters
        grace, so no sell is ever emitted for it."""
        adapter, emitted = self._run(exit_rank=3)
        sells = {s.ticker for batch in emitted for s in batch if s.action == "sell"}
        assert "B" not in sells
        assert adapter._held_grace.get("B") is None  # core, grace never started

    def test_symmetric_default_sells_the_same_name(self):
        """Same fixture, exit_rank=None: B leaves the top_n immediately and,
        with grace_cycles=0, is sold. This is the control proving the test
        above measures exit_rank and not the fixture."""
        _, emitted = self._run(exit_rank=None)
        sells = {s.ticker for batch in emitted for s in batch if s.action == "sell"}
        assert "B" in sells

    def test_name_outside_the_exit_band_still_exits(self):
        """A ranks 4th, outside exit_rank=3, so the band does not protect it —
        exit_rank rides winners, it does not disable exits."""
        panel = self._ranked_panel()
        adapter = MomentumAdapter(
            price_panel=panel, top_n=1, lookback_months=1, grace_cycles=0, exit_rank=3,
        )
        adapter.lookback_days = 1
        adapter._held_grace = {"A": None}
        sells = set()
        for i in range(1, 4):
            for s in adapter.generate_signals(["A", "B", "C", "D"], panel.index[i].date(), HorizonBucket.D21):
                if s.action == "sell":
                    sells.add(s.ticker)
        assert "A" in sells
