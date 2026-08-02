"""tests/unit/test_technical_combo_adapter.py — backtest/adapters/technical_combo_adapter.py.

Same fake-ScreenerEngine fixture pattern as test_technical_adapter.py.
"""

from datetime import date

import pytest

from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.adapters.technical_combo_adapter import TechnicalComboAdapter
from backtest.core.horizon import HorizonBucket


class _FakeResult:
    def __init__(self, ticker, score, matched=3, total=4, key_values=None):
        self.ticker = ticker
        self.score = score
        self.matched_conditions = matched
        self.total_conditions = total
        self.key_values = key_values or {}


class _FakeScreenerEngine:
    def __init__(self, results_by_date):
        self._results_by_date = results_by_date

    def screen(self, template_name, date=None, limit=50):
        return self._results_by_date.get(date, [])


class TestInitialization:
    def test_rejects_fewer_than_two_adapters(self):
        engine = _FakeScreenerEngine({})
        with pytest.raises(ValueError):
            TechnicalComboAdapter([TechnicalAdapter(template_name="A1", screener_engine=engine)], top_n=5)

    def test_rejects_non_positive_top_n(self):
        engine = _FakeScreenerEngine({})
        a1 = TechnicalAdapter(template_name="A1", screener_engine=engine)
        a2 = TechnicalAdapter(template_name="D4", screener_engine=engine)
        with pytest.raises(ValueError):
            TechnicalComboAdapter([a1, a2], top_n=0)


class TestGenerateSignals:
    def test_pools_candidates_across_templates(self):
        engine_a1 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("X", 0.9), _FakeResult("Y", 0.1)]})
        engine_d4 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("Z", 50.0)]})
        a1 = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine_a1)
        d4 = TechnicalAdapter(template_name="D4", top_n=5, screener_engine=engine_d4)
        combo = TechnicalComboAdapter([a1, d4], top_n=5)

        signals = combo.generate_signals(["X", "Y", "Z"], date(2020, 1, 1), HorizonBucket.D21)
        buys = {s.ticker for s in signals if s.action == "buy"}
        assert buys == {"X", "Y", "Z"}  # all 3 candidates pooled, well under top_n=5

    def test_respects_combined_top_n(self):
        engine_a1 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("A", 0.9), _FakeResult("B", 0.8)]})
        engine_d4 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("C", 100.0), _FakeResult("D", 90.0)]})
        a1 = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine_a1)
        d4 = TechnicalAdapter(template_name="D4", top_n=5, screener_engine=engine_d4)
        combo = TechnicalComboAdapter([a1, d4], top_n=2)

        signals = combo.generate_signals(["A", "B", "C", "D"], date(2020, 1, 1), HorizonBucket.D21)
        buys = {s.ticker for s in signals if s.action == "buy"}
        assert len(buys) == 2  # top_n=2 respected even though 4 candidates pooled

    def test_normalizes_scores_within_each_template_before_pooling(self):
        # A1's own top score should compete fairly with D4's top score even
        # though D4's raw scale (0-100) dwarfs A1's (0-1) — without
        # normalization D4 would always dominate.
        engine_a1 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("A", 0.9), _FakeResult("B", 0.1)]})
        engine_d4 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("C", 100.0), _FakeResult("D", 10.0)]})
        a1 = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine_a1)
        d4 = TechnicalAdapter(template_name="D4", top_n=5, screener_engine=engine_d4)
        combo = TechnicalComboAdapter([a1, d4], top_n=2)

        signals = combo.generate_signals(["A", "B", "C", "D"], date(2020, 1, 1), HorizonBucket.D21)
        buys = {s.ticker for s in signals if s.action == "buy"}
        # A (top of A1, normalized to 1.0) and C (top of D4, normalized to
        # 1.0) should both make it in over the two normalized-to-0.0 laggards.
        assert buys == {"A", "C"}

    def test_no_candidates_returns_no_signals(self):
        engine = _FakeScreenerEngine({})
        a1 = TechnicalAdapter(template_name="A1", screener_engine=engine)
        d4 = TechnicalAdapter(template_name="D4", screener_engine=engine)
        combo = TechnicalComboAdapter([a1, d4], top_n=5)
        signals = combo.generate_signals(["X"], date(2020, 1, 1), HorizonBucket.D21)
        assert signals == []

    def test_rotation_sells_dropped_ticker(self):
        engine_a1 = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9)],
            "2020-01-02": [],
        })
        engine_d4 = _FakeScreenerEngine({"2020-01-01": [], "2020-01-02": []})
        a1 = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine_a1)
        d4 = TechnicalAdapter(template_name="D4", top_n=5, screener_engine=engine_d4)
        combo = TechnicalComboAdapter([a1, d4], top_n=5)

        combo.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        signals = combo.generate_signals(["A"], date(2020, 1, 2), HorizonBucket.D21)
        assert [s.action for s in signals] == ["sell"]
        assert signals[0].ticker == "A"


class TestFeatureVector:
    def test_returns_matched_details_for_held_ticker(self):
        engine_a1 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("A", 0.9, matched=7, total=8)]})
        engine_d4 = _FakeScreenerEngine({"2020-01-01": []})
        a1 = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine_a1)
        d4 = TechnicalAdapter(template_name="D4", top_n=5, screener_engine=engine_d4)
        combo = TechnicalComboAdapter([a1, d4], top_n=5)
        combo.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)

        fv = combo.feature_vector("A", date(2020, 1, 1))
        assert fv["matched"] is True
        assert fv["source_template"] == "A1"
        assert fv["matched_conditions"] == 7

    def test_returns_unmatched_for_unknown_ticker(self):
        engine = _FakeScreenerEngine({})
        a1 = TechnicalAdapter(template_name="A1", screener_engine=engine)
        d4 = TechnicalAdapter(template_name="D4", screener_engine=engine)
        combo = TechnicalComboAdapter([a1, d4], top_n=5)
        fv = combo.feature_vector("ZZZ", date(2020, 1, 1))
        assert fv["matched"] is False
