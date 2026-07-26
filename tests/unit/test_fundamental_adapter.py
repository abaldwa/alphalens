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
