"""
tests/unit/test_momentum_strategy.py

Phase: FeatureBacklog.md ML38 — momentum strategy consolidation (2026-08-09)

Unit tests for features/momentum_strategy.py, the shared strategy-decision
module extracted from backtest/momentum_backtest.py's MomentumBacktester.
Every function here is pure (panels/config in, decision out) so these
tests use small synthetic price/volume panels directly, the same pattern
tests/unit/test_momentum_backtest.py already uses -- no DB, no mocks of
real market data.
"""

import pandas as pd
import pytest

import features.momentum_strategy as ms


def _panel(tickers, n_days, values):
    """values: {ticker: [prices...]} of length n_days."""
    dates = pd.date_range("2026-01-01", periods=n_days, freq="B")
    return pd.DataFrame({t: values[t] for t in tickers}, index=dates)


class TestDecideGraceTransitions:
    def test_ticker_in_target_set_resets_to_core(self):
        result = ms.decide_grace_transitions({"A": 1}, {"A"}, grace_cycles=2)
        assert result == {"A": None}

    def test_ticker_dropped_starts_grace_countdown(self):
        result = ms.decide_grace_transitions({"A": None}, set(), grace_cycles=2)
        assert result == {"A": 2}

    def test_ticker_in_grace_counts_down(self):
        result = ms.decide_grace_transitions({"A": 2}, set(), grace_cycles=2)
        assert result == {"A": 1}

    def test_ticker_grace_reaches_zero(self):
        result = ms.decide_grace_transitions({"A": 1}, set(), grace_cycles=2)
        assert result == {"A": 0}


class TestPassesQualityGate:
    def test_no_gate_always_passes(self):
        assert ms.passes_quality_gate("A", {}, {}) is True

    def test_missing_scores_always_passes(self):
        assert ms.passes_quality_gate("A", {}, {"min_f_score": 4}) is True

    def test_fails_min_f_score(self):
        scores = {"A": {"f_score": 2}}
        assert ms.passes_quality_gate("A", scores, {"min_f_score": 4}) is False

    def test_passes_min_f_score(self):
        scores = {"A": {"f_score": 6}}
        assert ms.passes_quality_gate("A", scores, {"min_f_score": 4}) is True

    def test_fails_max_m_score(self):
        scores = {"A": {"m_score": -1.0}}
        assert ms.passes_quality_gate("A", scores, {"max_m_score": -1.78}) is False

    def test_passes_max_m_score(self):
        scores = {"A": {"m_score": -2.0}}
        assert ms.passes_quality_gate("A", scores, {"max_m_score": -1.78}) is True


class TestIsCircuitLocked:
    def test_none_threshold_never_locked(self):
        panel = _panel(["A"], 3, {"A": [100, 100, 100]})
        assert ms.is_circuit_locked(panel, panel.index[2], "A", None) is False

    def test_large_move_locked(self):
        panel = _panel(["A"], 3, {"A": [100, 130, 130]})
        assert bool(ms.is_circuit_locked(panel, panel.index[1], "A", 0.20)) is True

    def test_small_move_not_locked(self):
        panel = _panel(["A"], 3, {"A": [100, 105, 105]})
        assert bool(ms.is_circuit_locked(panel, panel.index[1], "A", 0.20)) is False

    def test_missing_ticker_never_locked(self):
        panel = _panel(["A"], 3, {"A": [100, 100, 100]})
        assert ms.is_circuit_locked(panel, panel.index[1], "ZZZ", 0.20) is False

    def test_first_row_never_locked(self):
        panel = _panel(["A"], 3, {"A": [100, 100, 100]})
        assert ms.is_circuit_locked(panel, panel.index[0], "A", 0.20) is False


class TestIsRegimeDisabled:
    def test_no_regime_series_never_disabled(self):
        assert ms.is_regime_disabled(None, {"high_vol"}, pd.Timestamp("2026-01-05")) is False

    def test_no_disable_set_never_disabled(self):
        series = pd.Series({pd.Timestamp("2026-01-01"): "high_vol"})
        assert ms.is_regime_disabled(series, set(), pd.Timestamp("2026-01-05")) is False

    def test_matching_regime_disabled(self):
        series = pd.Series({pd.Timestamp("2026-01-01"): "high_vol"})
        assert ms.is_regime_disabled(series, {"high_vol"}, pd.Timestamp("2026-01-05")) is True

    def test_non_matching_regime_not_disabled(self):
        series = pd.Series({pd.Timestamp("2026-01-01"): "low_vol"})
        assert ms.is_regime_disabled(series, {"high_vol"}, pd.Timestamp("2026-01-05")) is False

    def test_no_entry_on_or_before_date_never_disabled(self):
        series = pd.Series({pd.Timestamp("2026-02-01"): "high_vol"})
        assert ms.is_regime_disabled(series, {"high_vol"}, pd.Timestamp("2026-01-05")) is False


class TestTrailingStopCheck:
    def test_first_mark_sets_peak_to_price(self):
        new_peak, should_stop = ms.trailing_stop_check(100.0, None, 0.10)
        assert new_peak == 100.0
        assert should_stop is False

    def test_price_above_prior_peak_updates_peak(self):
        new_peak, should_stop = ms.trailing_stop_check(110.0, 100.0, 0.10)
        assert new_peak == 110.0
        assert should_stop is False

    def test_price_at_floor_stops(self):
        new_peak, should_stop = ms.trailing_stop_check(90.0, 100.0, 0.10)
        assert new_peak == 100.0
        assert should_stop is True

    def test_price_above_floor_does_not_stop(self):
        new_peak, should_stop = ms.trailing_stop_check(95.0, 100.0, 0.10)
        assert should_stop is False

    def test_acts_as_stop_loss_from_entry(self):
        # peak == entry_price (never risen) -- a straight drop from entry
        # should still fire once it breaches the floor.
        new_peak, should_stop = ms.trailing_stop_check(89.0, 100.0, 0.10)
        assert should_stop is True


class TestAdtvCr:
    def test_no_volume_panel_returns_empty(self):
        price = _panel(["A"], 3, {"A": [100, 100, 100]})
        result = ms.adtv_cr(price, None, price.index[2], ["A"], 20)
        assert result.empty

    def test_computes_mean_traded_value(self):
        price = _panel(["A"], 2, {"A": [100.0, 100.0]})
        volume = _panel(["A"], 2, {"A": [100000, 100000]})
        result = ms.adtv_cr(price, volume, price.index[1], ["A"], 20)
        # price*volume = 1e7 per day -> /1e7 = 1.0 Cr
        assert result["A"] == pytest.approx(1.0)

    def test_no_matching_tickers_returns_empty(self):
        price = _panel(["A"], 2, {"A": [100.0, 100.0]})
        volume = _panel(["A"], 2, {"A": [1000, 1000]})
        result = ms.adtv_cr(price, volume, price.index[1], ["ZZZ"], 20)
        assert result.empty


class TestSelectBuyPool:
    def _base_kwargs(self, tickers=("A", "B"), n_days=25):
        price = _panel(list(tickers), n_days, {t: [100.0] * n_days for t in tickers})
        return price, price.ffill()

    def test_no_filters_returns_pool_unchanged(self):
        price, ffilled = self._base_kwargs()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled)
        assert set(result.index) == {"A", "B"}

    def test_quality_gate_excludes_failing_ticker(self):
        price, ffilled = self._base_kwargs()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled,
            quality_scores={"A": {"f_score": 1}}, quality_gate={"min_f_score": 4},
        )
        assert "A" not in result.index
        assert "B" in result.index

    def test_circuit_lock_excludes_ticker(self):
        n_days = 5
        price = _panel(["A", "B"], n_days, {"A": [100, 100, 100, 100, 140], "B": [100, 100, 100, 100, 105]})
        ffilled = price.ffill()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled, circuit_band_pct=0.20,
        )
        assert "A" not in result.index
        assert "B" in result.index

    def test_min_adtv_cr_excludes_illiquid_ticker(self):
        n_days = 25
        price = _panel(["A", "B"], n_days, {"A": [100.0] * n_days, "B": [100.0] * n_days})
        volume = _panel(["A", "B"], n_days, {"A": [1000] * n_days, "B": [1_000_000] * n_days})
        ffilled = price.ffill()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled,
            volume_panel=volume, min_adtv_cr=1.0,
        )
        assert "A" not in result.index  # 100*1000/1e7 = 0.01 Cr < 1.0 Cr floor
        assert "B" in result.index

    def test_downtrend_filter_excludes_sharp_drop(self):
        n_days = 25
        a_prices = [100.0] * (n_days - 1) + [90.0]  # -10% on the last day
        b_prices = [100.0] * n_days
        price = _panel(["A", "B"], n_days, {"A": a_prices, "B": b_prices})
        ffilled = price.ffill()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled,
            downtrend_filter_pct=0.05, downtrend_lookback_days=20,
        )
        assert "A" not in result.index
        assert "B" in result.index

    def test_hmm_bearish_regime_excludes_ticker(self):
        price, ffilled = self._base_kwargs()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        hmm = {"A": pd.DataFrame({"hmm_regime": [0.0]}, index=[price.index[-1]])}
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled,
            per_ticker_hmm_regime=hmm,
        )
        assert "A" not in result.index
        assert "B" in result.index

    def test_missing_data_never_excludes(self):
        # No data for "A" in any optional filter's inputs -- should stay
        # eligible (never-exclude-on-missing-data convention).
        price, ffilled = self._base_kwargs()
        pool = pd.Series({"A": 0.3, "B": 0.1})
        result = ms.select_buy_pool(
            pool, price.index[-1], price_panel=price, price_panel_ffilled=ffilled,
            quality_scores={}, quality_gate={"min_f_score": 4},
        )
        assert set(result.index) == {"A", "B"}


class TestStickyPromotedHoldings:
    def test_off_when_rank_start_none(self):
        result = ms.sticky_promoted_holdings({"A": None}, ["B"], pd.Timestamp("2026-01-05"), None, {})
        assert result == []

    def test_off_when_no_held_grace(self):
        result = ms.sticky_promoted_holdings({}, ["B"], pd.Timestamp("2026-01-05"), 50, {pd.Timestamp("2026-01-01"): {"A": 10}})
        assert result == []

    def test_promoted_held_ticker_returned(self):
        held = {"A": None}
        ranks = {pd.Timestamp("2026-01-01"): {"A": 10}}  # A ranks 10, better than rank_start=50
        result = ms.sticky_promoted_holdings(held, ["B", "C"], pd.Timestamp("2026-01-05"), 50, ranks)
        assert result == ["A"]

    def test_held_ticker_still_in_universe_not_returned(self):
        held = {"A": None}
        ranks = {pd.Timestamp("2026-01-01"): {"A": 10}}
        result = ms.sticky_promoted_holdings(held, ["A", "B"], pd.Timestamp("2026-01-05"), 50, ranks)
        assert result == []

    def test_demoted_ticker_not_returned(self):
        held = {"A": None}
        ranks = {pd.Timestamp("2026-01-01"): {"A": 80}}  # worse than rank_start=50
        result = ms.sticky_promoted_holdings(held, ["B"], pd.Timestamp("2026-01-05"), 50, ranks)
        assert result == []

    def test_unranked_ticker_not_returned(self):
        held = {"A": None}
        ranks = {pd.Timestamp("2026-01-01"): {}}
        result = ms.sticky_promoted_holdings(held, ["B"], pd.Timestamp("2026-01-05"), 50, ranks)
        assert result == []


class TestBuildCategoryPresets:
    def _presets(self):
        return ms.build_category_presets(
            "VOL", "MCAP", {"A": 1.1}, "REGIME", "QSCORES",
            min_adtv_cr=0.1, max_pct_of_adtv=0.05, circuit_band_pct=0.2,
            quality_gate={"min_f_score": 4}, disable_in_high_vol_regime="high_vol",
        )

    def test_all_risk_is_empty(self):
        assert self._presets()["all_risk"] == {}

    def test_balanced_has_liquidity_and_quality_filters(self):
        balanced = self._presets()["balanced"]
        assert balanced["min_adtv_cr"] == 0.1
        assert balanced["quality_gate"] == {"min_f_score": 4}
        assert "regime_series" not in balanced
        assert "orthogonalize_vs_size_beta" not in balanced

    def test_risk_managed_adds_regime_conditioning(self):
        risk_managed = self._presets()["risk_managed"]
        assert risk_managed["regime_series"] == "REGIME"
        assert risk_managed["disable_in_regimes"] == {"high_vol"}
        # still carries balanced's filters
        assert risk_managed["min_adtv_cr"] == 0.1

    def test_max_defensive_adds_orthogonalization(self):
        max_defensive = self._presets()["max_defensive"]
        assert max_defensive["orthogonalize_vs_size_beta"] is True
        assert max_defensive["market_cap_panel"] == "MCAP"
        assert max_defensive["beta_map"] == {"A": 1.1}
        # still carries risk_managed's regime conditioning
        assert max_defensive["disable_in_regimes"] == {"high_vol"}

    def test_returns_all_four_categories(self):
        assert set(self._presets().keys()) == {"all_risk", "balanced", "risk_managed", "max_defensive"}


class TestComputeFyNetTax:
    def _txn(self, buy_price, sell_price, qty, holding_days):
        return {"buy_price": buy_price, "sell_price": sell_price, "qty": qty, "holding_days": holding_days}

    def test_single_stcg_gain(self):
        # gain = (120-100)*10 = 200, STCG_RATE=0.20 -> 40.0
        tax = ms.compute_fy_net_tax([self._txn(100, 120, 10, 100)])
        assert tax == pytest.approx(40.0)

    def test_single_ltcg_gain(self):
        # gain = (120-100)*10 = 200, LTCG_RATE=0.125 -> 25.0
        tax = ms.compute_fy_net_tax([self._txn(100, 120, 10, 400)])
        assert tax == pytest.approx(25.0)

    def test_stcg_loss_nets_against_stcg_gain(self):
        # winner: (120-100)*10=200 gain; loser: (80-100)*10=-200 loss -> net 0 -> tax 0
        txns = [self._txn(100, 120, 10, 100), self._txn(100, 80, 10, 100)]
        assert ms.compute_fy_net_tax(txns) == pytest.approx(0.0)

    def test_net_loss_bucket_floored_at_zero_not_credited(self):
        # net STCG loss of -100 must not offset/credit the LTCG gain bucket
        stcg_loss = self._txn(100, 90, 10, 50)  # -100
        ltcg_gain = self._txn(100, 120, 10, 400)  # +200 gain -> tax 25.0
        tax = ms.compute_fy_net_tax([stcg_loss, ltcg_gain])
        assert tax == pytest.approx(25.0)

    def test_open_positions_without_sell_price_ignored(self):
        assert ms.compute_fy_net_tax([{"buy_price": 100, "sell_price": None, "qty": 10, "holding_days": None}]) == 0.0

    def test_empty_list_is_zero(self):
        assert ms.compute_fy_net_tax([]) == 0.0


class TestFyEndDatesThrough:
    def test_single_year_span(self):
        boundaries = ms.fy_end_dates_through(pd.Timestamp("2026-05-01"), pd.Timestamp("2027-06-01"))
        assert boundaries == [pd.Timestamp("2027-03-31")]

    def test_multi_year_span(self):
        boundaries = ms.fy_end_dates_through(pd.Timestamp("2024-06-01"), pd.Timestamp("2027-01-01"))
        assert boundaries == [pd.Timestamp("2025-03-31"), pd.Timestamp("2026-03-31")]

    def test_no_boundary_before_first_fy_end(self):
        boundaries = ms.fy_end_dates_through(pd.Timestamp("2026-05-01"), pd.Timestamp("2026-12-01"))
        assert boundaries == []

    def test_start_before_april_counts_prior_fy(self):
        # start_date in Jan 2026 -> FY containing it is FY25-26 (ends Mar 2026)
        boundaries = ms.fy_end_dates_through(pd.Timestamp("2026-01-15"), pd.Timestamp("2026-06-01"))
        assert boundaries == [pd.Timestamp("2026-03-31")]


class TestSelectForcedSellForShortfall:
    class _Pos:
        def __init__(self, grace_remaining=None, entry_rank=None):
            self.grace_remaining = grace_remaining
            self.entry_rank = entry_rank

    def test_prefers_grace_holding_closest_to_expiry(self):
        positions = {
            "A": self._Pos(grace_remaining=2, entry_rank=1),
            "B": self._Pos(grace_remaining=1, entry_rank=2),
            "C": self._Pos(grace_remaining=None, entry_rank=3),
        }
        prices = pd.Series({"A": 100.0, "B": 100.0, "C": 100.0})
        assert ms.select_forced_sell_for_shortfall(positions, prices) == "B"

    def test_falls_back_to_weakest_rank_when_no_grace_holdings(self):
        positions = {
            "A": self._Pos(grace_remaining=None, entry_rank=1),
            "B": self._Pos(grace_remaining=None, entry_rank=5),
        }
        prices = pd.Series({"A": 100.0, "B": 100.0})
        assert ms.select_forced_sell_for_shortfall(positions, prices) == "B"

    def test_excludes_already_sold_tickers(self):
        positions = {
            "A": self._Pos(grace_remaining=1, entry_rank=1),
            "B": self._Pos(grace_remaining=2, entry_rank=2),
        }
        prices = pd.Series({"A": 100.0, "B": 100.0})
        assert ms.select_forced_sell_for_shortfall(positions, prices, exclude={"A"}) == "B"

    def test_skips_tickers_with_no_real_price(self):
        positions = {
            "A": self._Pos(grace_remaining=1, entry_rank=1),
            "B": self._Pos(grace_remaining=2, entry_rank=2),
        }
        prices = pd.Series({"A": float("nan"), "B": 100.0})
        assert ms.select_forced_sell_for_shortfall(positions, prices) == "B"

    def test_returns_none_when_no_candidates(self):
        assert ms.select_forced_sell_for_shortfall({}, pd.Series(dtype=float)) is None
