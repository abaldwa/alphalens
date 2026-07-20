"""tests/unit/test_technical_adapter.py — backtest/adapters/technical_adapter.py.

Deterministic-fixture tests use a fake ScreenerEngine (injected, matching
this adapter's testability design) so rotation mechanics are tested
without depending on which real screener templates happen to fire on a
given historical date. TestRealScreenerIntegration exercises the adapter
against the real ScreenerEngine + real daily feature Parquet store, per
the No-Mock-Data Policy.
"""

from datetime import date

import pytest

from backtest.adapters.technical_adapter import TechnicalAdapter
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
    def test_rejects_non_positive_top_n(self):
        with pytest.raises(ValueError):
            TechnicalAdapter(template_name="A1", top_n=0, screener_engine=_FakeScreenerEngine({}))


class TestGenerateSignals:
    def test_no_matches_on_date_returns_no_signals(self):
        engine = _FakeScreenerEngine({})
        adapter = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=engine)
        signals = adapter.generate_signals(["RELIANCE"], date(2020, 1, 1), HorizonBucket.D21)
        assert signals == []

    def test_first_call_buys_top_n_by_score(self):
        engine = _FakeScreenerEngine({
            "2020-01-01": [
                _FakeResult("A", 0.9), _FakeResult("B", 0.7), _FakeResult("C", 0.5),
            ],
        })
        adapter = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=engine)
        signals = adapter.generate_signals(["A", "B", "C"], date(2020, 1, 1), HorizonBucket.D21)
        buys = {s.ticker for s in signals if s.action == "buy"}
        assert buys == {"A", "B"}  # top-2 by score

    def test_results_filtered_to_supplied_universe(self):
        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9), _FakeResult("OUT_OF_UNIVERSE", 0.99)],
        })
        adapter = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=engine)
        signals = adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        assert {s.ticker for s in signals} == {"A"}

    def test_second_call_sells_tickers_that_dropped_out(self):
        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9), _FakeResult("B", 0.7)],
            "2020-01-30": [_FakeResult("A", 0.9)],  # B no longer matches
        })
        adapter = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=engine)
        adapter.generate_signals(["A", "B"], date(2020, 1, 1), HorizonBucket.D21)
        signals = adapter.generate_signals(["A", "B"], date(2020, 1, 30), HorizonBucket.D21)
        sells = {s.ticker for s in signals if s.action == "sell"}
        assert sells == {"B"}

    def test_holding_dropped_from_result_set_entirely_is_sold_not_ignored(self):
        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9)],
            "2020-01-30": [],  # nothing matches at all
        })
        adapter = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=engine)
        adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        signals = adapter.generate_signals(["A"], date(2020, 1, 30), HorizonBucket.D21)
        assert {s.ticker for s in signals if s.action == "sell"} == {"A"}


class TestFeatureVector:
    def test_matched_ticker_reports_score_and_key_values(self):
        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9, key_values={"rsi_14": 28.0})],
        })
        adapter = TechnicalAdapter(template_name="A1", top_n=1, screener_engine=engine)
        adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        fv = adapter.feature_vector("A", date(2020, 1, 1))
        assert fv["matched"] is True
        assert fv["score"] == 0.9
        assert fv["feature__rsi_14"] == 28.0

    def test_unmatched_ticker_reports_matched_false_not_fabricated_score(self):
        engine = _FakeScreenerEngine({"2020-01-01": []})
        adapter = TechnicalAdapter(template_name="A1", top_n=1, screener_engine=engine)
        adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        fv = adapter.feature_vector("A", date(2020, 1, 1))
        assert fv["matched"] is False
        assert "score" not in fv


class TestRealScreenerIntegration:
    """No-Mock-Data Policy: exercises the adapter against the real
    ScreenerEngine + real daily feature Parquet store."""

    def test_real_screener_produces_a_valid_signal_list(self):
        from systems.technical_analysis.screener.engine import ScreenerEngine

        adapter = TechnicalAdapter(template_name="A1", top_n=5, screener_engine=ScreenerEngine())
        # A recent real date, per the verified 2007-2026 feature parquet coverage.
        signals = adapter.generate_signals(
            ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"], date(2026, 7, 15), HorizonBucket.D21,
        )
        assert all(s.ticker in {"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"} for s in signals)
