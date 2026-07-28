"""tests/unit/test_fundamental_adapter.py — backtest/adapters/fundamental_adapter.py."""

from datetime import date

import pandas as pd
import pytest

from backtest.adapters.fundamental_adapter import FundamentalAdapter
from backtest.core.horizon import HorizonBucket


def _panel(rows):
    """rows: list of dicts, each with 'ticker' + ratio columns."""
    return pd.DataFrame(rows)


class TestInitialization:
    def test_rejects_unknown_preset(self):
        with pytest.raises(ValueError, match="Unknown screener preset"):
            FundamentalAdapter(preset="not_a_real_preset")

    def test_rejects_non_positive_top_n(self):
        with pytest.raises(ValueError):
            FundamentalAdapter(preset="quality_compounder", top_n=0)

    def test_bespoke_preset_construction_without_db_conn_does_not_raise(self):
        # db_conn is validated lazily (in generate_signals), not at
        # construction — orchestrator CLI callers (run_orchestrator_backtest.py)
        # need to build the adapter before a DB connection exists, then wire
        # db_conn post-construction (same pattern as the technical channel's
        # _screener_cache_conn).
        adapter = FundamentalAdapter(preset="piotroski_on_value")
        assert adapter._db_conn is None

    def test_bespoke_preset_generate_signals_without_db_conn_raises(self):
        adapter = FundamentalAdapter(preset="net_net")
        with pytest.raises(ValueError, match="requires db_conn"):
            adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.Y1)


class TestGenerateSignals:
    def test_no_feature_snapshot_for_date_returns_no_signals(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: None)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=2)
        signals = adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.Y1)
        assert signals == []

    def test_buys_tickers_that_clear_the_preset_thresholds(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        # quality_compounder: roe >= 1.0, roce >= 1.0, debt_to_equity <= -0.5 (sign-adjusted)
        panel = _panel([
            {"ticker": "GOOD", "roe": 1.5, "roce": 1.2, "debt_to_equity": -0.8},
            {"ticker": "BAD", "roe": 0.2, "roce": 0.1, "debt_to_equity": 0.5},
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        signals = adapter.generate_signals(["GOOD", "BAD"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"GOOD"}

    def test_missing_ratio_conservatively_excludes_the_ticker(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([{"ticker": "INCOMPLETE", "roe": 1.5, "roce": None, "debt_to_equity": -0.8}])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        signals = adapter.generate_signals(["INCOMPLETE"], date(2020, 6, 1), HorizonBucket.Y1)
        assert signals == []

    def test_second_call_sells_tickers_that_no_longer_qualify(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        good_panel = _panel([{"ticker": "A", "roe": 1.5, "roce": 1.2, "debt_to_equity": -0.8}])
        bad_panel = _panel([{"ticker": "A", "roe": 0.1, "roce": 0.1, "debt_to_equity": 0.5}])

        calls = {"n": 0}

        def fake_read(date_str):
            calls["n"] += 1
            return good_panel if calls["n"] == 1 else bad_panel

        monkeypatch.setattr(mod, "read_feature_day", fake_read)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        adapter.generate_signals(["A"], date(2020, 6, 1), HorizonBucket.Y1)
        signals = adapter.generate_signals(["A"], date(2021, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}

    def test_results_filtered_to_supplied_universe(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([
            {"ticker": "IN_UNIVERSE", "roe": 1.5, "roce": 1.2, "debt_to_equity": -0.8},
            {"ticker": "OUT_OF_UNIVERSE", "roe": 2.0, "roce": 2.0, "debt_to_equity": -1.0},
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        signals = adapter.generate_signals(["IN_UNIVERSE"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals} == {"IN_UNIVERSE"}

    def test_more_matches_than_top_n_ranked_by_composite_strength(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([
            {"ticker": "STRONG", "roe": 3.0, "roce": 3.0, "debt_to_equity": -2.0},
            {"ticker": "WEAK", "roe": 1.0, "roce": 1.0, "debt_to_equity": -0.5},
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=1)
        signals = adapter.generate_signals(["STRONG", "WEAK"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"STRONG"}


class TestFeatureVector:
    def test_matched_ticker_reports_ratio_values(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([{"ticker": "A", "roe": 1.5, "roce": 1.2, "debt_to_equity": -0.8}])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        adapter.generate_signals(["A"], date(2020, 6, 1), HorizonBucket.Y1)
        fv = adapter.feature_vector("A", date(2020, 6, 1))
        assert fv["matched"] is True
        assert fv["ratio__roe"] == 1.5

    def test_unmatched_ticker_reports_matched_false(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: None)
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        adapter.generate_signals(["A"], date(2020, 6, 1), HorizonBucket.Y1)
        fv = adapter.feature_vector("A", date(2020, 6, 1))
        assert fv["matched"] is False


class TestCompositeScoreStrategies:
    """2026-07-25: the 22 SCORE_FUNCTIONS composite-score strategies (QGLP,
    Moat, Owner Earnings, etc.) — previously had no backtest path at all."""

    def test_accepts_a_score_function_key_at_construction(self):
        adapter = FundamentalAdapter(preset="moat", top_n=5)
        assert adapter.preset == "moat"

    def test_ranks_universe_by_score_and_respects_top_n(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        # moat weights: {"avg_roce_5y": 0.45, "margin_stability_5y": 0.30, "debt_to_equity": -0.25}
        panel = _panel([
            {"ticker": "STRONG_MOAT", "avg_roce_5y": 2.0, "margin_stability_5y": 2.0, "debt_to_equity": -1.0},
            {"ticker": "WEAK_MOAT", "avg_roce_5y": 0.5, "margin_stability_5y": 0.5, "debt_to_equity": 0.0},
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="moat", top_n=1)
        signals = adapter.generate_signals(["STRONG_MOAT", "WEAK_MOAT"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"STRONG_MOAT"}

    def test_tickers_below_min_coverage_are_excluded_not_ranked_as_zero(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        # Only 1 of moat's 3 weighted inputs present -> below MIN_COVERAGE (50%)
        # -> score is None -> must NOT be silently ranked in (e.g. as a 0).
        panel = _panel([{"ticker": "SPARSE", "avg_roce_5y": 5.0}])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="moat", top_n=5)
        signals = adapter.generate_signals(["SPARSE"], date(2020, 6, 1), HorizonBucket.Y1)
        assert signals == []

    def test_conviction_equals_the_composite_score(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([{"ticker": "A", "avg_roce_5y": 1.0, "margin_stability_5y": 1.0, "debt_to_equity": -1.0}])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="moat", top_n=5)
        signals = adapter.generate_signals(["A"], date(2020, 6, 1), HorizonBucket.Y1)
        buy = next(s for s in signals if s.action == "buy")
        assert buy.conviction > 50  # all-positive inputs -> above the neutral midpoint

    def test_no_feature_snapshot_returns_no_signals(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: None)
        adapter = FundamentalAdapter(preset="qglp", top_n=5)
        signals = adapter.generate_signals(["A"], date(2020, 6, 1), HorizonBucket.Y1)
        assert signals == []

    def test_financial_services_excluded_even_for_a_top_scoring_ticker(self, monkeypatch):
        """[BUG FIX, 2026-07-28 model-review] Composite-score strategies
        (Moat here) never checked PRESET_EXCLUDED_SECTORS at all — a bank
        with a great-looking (but structurally meaningless) avg_roce_5y/
        debt_to_equity score would still get bought. Must be excluded now."""
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([
            {"ticker": "BANK", "avg_roce_5y": 5.0, "margin_stability_5y": 5.0, "debt_to_equity": -5.0},
            {"ticker": "NONBANK", "avg_roce_5y": 1.0, "margin_stability_5y": 1.0, "debt_to_equity": -1.0},
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(
            preset="moat", top_n=5, sector_lookup={"BANK": "Financial Services", "NONBANK": "IT"},
        )
        signals = adapter.generate_signals(["BANK", "NONBANK"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"NONBANK"}


class TestLiquidityFloorOnSmallnessRewardingStrategies:
    """[BUG FIX, 2026-07-28 second model-review, item 9] small_cap_
    compounders/smile/under_followed actively reward smallness/low
    ownership with no minimum market-cap gate of their own (unlike
    net_net.py's LIQUIDITY_FLOOR_MARKET_CAP_CR) — prone to selecting
    circuit-filter-prone, unfillable Indian small-caps."""

    def test_below_floor_market_cap_ticker_excluded_from_small_cap_compounders(self, monkeypatch):
        import backtest.adapters.fundamental_adapter as mod
        # small_cap_compounders weights: size(market_cap:-1.0), quality_growth
        # (roce/eps_growth_yoy/revenue_cagr_3yr), risk_control (debt_to_equity/cfo_to_pat).
        panel = _panel([
            {
                "ticker": "TINY", "market_cap": 3.0, "roce": 3.0, "eps_growth_yoy": 3.0,
                "revenue_cagr_3yr": 3.0, "debt_to_equity": -3.0, "cfo_to_pat": 3.0,
            },
            {
                "ticker": "HEALTHY_SIZE", "market_cap": 1.0, "roce": 1.0, "eps_growth_yoy": 1.0,
                "revenue_cagr_3yr": 1.0, "debt_to_equity": -1.0, "cfo_to_pat": 1.0,
            },
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(
            preset="small_cap_compounders", top_n=5,
            market_cap_lookup={"TINY": 10.0, "HEALTHY_SIZE": 500.0},  # TINY below the 50cr floor
        )
        signals = adapter.generate_signals(["TINY", "HEALTHY_SIZE"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"HEALTHY_SIZE"}

    def test_no_market_cap_lookup_supplied_is_a_safe_noop(self, monkeypatch):
        """A caller that hasn't wired market_cap_lookup yet must not have
        this new gate silently start excluding every candidate."""
        import backtest.adapters.fundamental_adapter as mod
        panel = _panel([
            {
                "ticker": "TINY", "market_cap": 3.0, "roce": 3.0, "eps_growth_yoy": 3.0,
                "revenue_cagr_3yr": 3.0, "debt_to_equity": -3.0, "cfo_to_pat": 3.0,
            },
        ])
        monkeypatch.setattr(mod, "read_feature_day", lambda date_str: panel)
        adapter = FundamentalAdapter(preset="small_cap_compounders", top_n=5)
        signals = adapter.generate_signals(["TINY"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"TINY"}


class TestBespokePresets:
    """generate_signals' BESPOKE_PRESETS branch (piotroski_on_value/
    margin_of_safety/net_net) — reads raw PIT financials via db_conn
    rather than the z-scored feature panel, so it's dispatched to a
    dedicated systems.fundamental_analysis.quality.* module per preset.
    Never exercised before this test (0% coverage on that branch)."""

    def test_piotroski_on_value_matches_passing_tickers(self, monkeypatch):
        import systems.fundamental_analysis.quality.piotroski_on_value as pov_mod

        def fake_compute(conn, ticker, as_of_dt, feature_date_str=None):
            return {"f_score": 8, "is_cheap": True, "passes": ticker == "GOOD"}

        monkeypatch.setattr(pov_mod, "compute_piotroski_on_value", fake_compute)
        adapter = FundamentalAdapter(preset="piotroski_on_value", top_n=5, db_conn=object())
        signals = adapter.generate_signals(["GOOD", "BAD"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"GOOD"}
        assert adapter._last_ratios["GOOD"] == {"f_score": 8}

    def test_margin_of_safety_matches_passing_tickers(self, monkeypatch):
        import systems.fundamental_analysis.quality.margin_of_safety as mos_mod

        def fake_compute(conn, ticker, as_of_dt):
            return {"margin_of_safety": 0.40, "passes": ticker == "CHEAP"}

        monkeypatch.setattr(mos_mod, "compute_margin_of_safety", fake_compute)
        adapter = FundamentalAdapter(preset="margin_of_safety", top_n=5, db_conn=object())
        signals = adapter.generate_signals(["CHEAP", "EXPENSIVE"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"CHEAP"}
        assert adapter._last_ratios["CHEAP"] == {"margin_of_safety": 0.40}

    def test_net_net_matches_passing_tickers(self, monkeypatch):
        import systems.fundamental_analysis.quality.net_net as nn_mod

        def fake_compute(conn, ticker, as_of_dt):
            return {"ncav_per_share": 55.0, "passes": ticker == "DEEPVALUE"}

        monkeypatch.setattr(nn_mod, "compute_net_net", fake_compute)
        adapter = FundamentalAdapter(preset="net_net", top_n=5, db_conn=object())
        signals = adapter.generate_signals(["DEEPVALUE", "FAIR"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "buy"} == {"DEEPVALUE"}
        assert adapter._last_ratios["DEEPVALUE"] == {"ncav_per_share": 55.0}

    def test_bespoke_preset_no_matches_sells_existing_holdings(self, monkeypatch):
        import systems.fundamental_analysis.quality.net_net as nn_mod

        monkeypatch.setattr(nn_mod, "compute_net_net", lambda conn, ticker, as_of_dt: {"passes": False})
        adapter = FundamentalAdapter(preset="net_net", top_n=5, db_conn=object())
        adapter._currently_held = {"OLD_HOLDING"}
        signals = adapter.generate_signals(["OLD_HOLDING"], date(2020, 6, 1), HorizonBucket.Y1)
        assert {s.ticker for s in signals if s.action == "sell"} == {"OLD_HOLDING"}


class TestRealFeatureStoreIntegration:
    """No-Mock-Data Policy: exercises the adapter against the real feature
    Parquet store (config.settings.FEATURES_DAILY_DIR) for a real recent
    date, rather than a fabricated panel."""

    def test_real_feature_day_produces_a_valid_signal_list(self):
        adapter = FundamentalAdapter(preset="quality_compounder", top_n=5)
        signals = adapter.generate_signals(
            ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"], date(2026, 7, 15), HorizonBucket.Y1,
        )
        assert all(s.ticker in {"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"} for s in signals)
