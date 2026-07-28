"""tests/unit/test_technical_adapter.py — backtest/adapters/technical_adapter.py.

Deterministic-fixture tests use a fake ScreenerEngine (injected, matching
this adapter's testability design) so rotation mechanics are tested
without depending on which real screener templates happen to fire on a
given historical date. TestRealScreenerIntegration exercises the adapter
against the real ScreenerEngine + real daily feature Parquet store, per
the No-Mock-Data Policy.
"""

from datetime import date

import pandas as pd
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


class TestScreenerCacheIntegration:
    """Parity + cross-instance sharing coverage for the screener_cache-
    wired path (2026-07-25, reviewed by ml-rigor-reviewer + backtest-
    reviewer — see FeatureBacklog.md): screener_cache_conn=None must
    remain byte-for-byte identical to the pre-existing always-live
    behavior (every test above exercises exactly that, unchanged), and
    when a cache conn IS wired in, results must be identical to the
    live path while a second adapter for the same template/date makes
    zero additional screen() calls."""

    def test_cached_path_produces_identical_signals_to_live_path(self):
        from datastore.api.db import get_duckdb_connection
        from datastore.schema import create_backtest

        results_by_date = {
            "2020-01-01": [_FakeResult("A", 0.9), _FakeResult("B", 0.7), _FakeResult("C", 0.5)],
        }
        live_adapter = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=_FakeScreenerEngine(results_by_date))
        live_signals = live_adapter.generate_signals(["A", "B", "C"], date(2020, 1, 1), HorizonBucket.D21)

        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            cached_adapter = TechnicalAdapter(
                template_name="A1", top_n=2, screener_engine=_FakeScreenerEngine(results_by_date),
                screener_cache_conn=conn,
            )
            cached_signals = cached_adapter.generate_signals(["A", "B", "C"], date(2020, 1, 1), HorizonBucket.D21)

        live_shape = {(s.ticker, s.action, s.conviction) for s in live_signals}
        cached_shape = {(s.ticker, s.action, s.conviction) for s in cached_signals}
        assert live_shape == cached_shape

    def test_second_adapter_for_same_template_date_reuses_the_cache(self):
        from datastore.api.db import get_duckdb_connection
        from datastore.schema import create_backtest

        engine1 = _FakeScreenerEngine({"2020-01-01": [_FakeResult("A", 0.9), _FakeResult("B", 0.7)]})

        class _CountingWrapper:
            def __init__(self, inner):
                self._inner = inner
                self.calls = 0

            def screen(self, template_name, date=None, limit=50):
                self.calls += 1
                return self._inner.screen(template_name, date=date, limit=limit)

        counting1 = _CountingWrapper(engine1)
        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            adapter1 = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=counting1, screener_cache_conn=conn)
            adapter1.generate_signals(["A", "B"], date(2020, 1, 1), HorizonBucket.D21)

            # A second adapter instance — simulating a second exit-variant
            # job's own TechnicalAdapter — sharing the same connection
            # (same cached DuckDB file in a real multi-process run).
            counting2 = _CountingWrapper(_FakeScreenerEngine({}))  # would return [] if actually called
            adapter2 = TechnicalAdapter(template_name="A1", top_n=2, screener_engine=counting2, screener_cache_conn=conn)
            signals2 = adapter2.generate_signals(["A", "B"], date(2020, 1, 1), HorizonBucket.D21)

        assert counting1.calls == 1
        assert counting2.calls == 0  # never hit the (empty-returning) engine — served entirely from cache
        assert {s.ticker for s in signals2 if s.action == "buy"} == {"A", "B"}

    def test_feature_vector_works_through_the_cached_path(self):
        from datastore.api.db import get_duckdb_connection
        from datastore.schema import create_backtest

        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9, key_values={"rsi_14": 28.0})],
        })
        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            adapter = TechnicalAdapter(template_name="A1", top_n=1, screener_engine=engine, screener_cache_conn=conn)
            adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
            fv = adapter.feature_vector("A", date(2020, 1, 1))
        assert fv["matched"] is True
        assert fv["score"] == 0.9
        assert fv["feature__rsi_14"] == 28.0


class TestAdtvWiring:
    """[BUG FIX, 4th fundamental-strategies review, item 2] Signal.adtv_cr
    was never populated for the Technical channel (always None), forcing
    check_06_liquidity's applied_min_adt_inr=0.0 — the MIN_ADT_INR floor was
    silently never enforced. Confirms adtv_cr is real/non-None when a real
    OHLCV-derived price/volume panel is supplied."""

    def test_adtv_cr_populated_from_price_volume_panels(self):
        dates = pd.date_range("2019-12-01", periods=25, freq="B")
        price_panel = pd.DataFrame({"A": [100.0 + i for i in range(25)]}, index=dates)
        volume_panel = pd.DataFrame({"A": [10_000.0 + i * 10 for i in range(25)]}, index=dates)

        engine = _FakeScreenerEngine({
            str(dates[-1].date()): [_FakeResult("A", 0.9)],
        })
        adapter = TechnicalAdapter(
            template_name="A1", top_n=1, screener_engine=engine,
            price_panel=price_panel, volume_panel=volume_panel,
        )
        signals = adapter.generate_signals(["A"], dates[-1].date(), HorizonBucket.D21)
        buy = next(s for s in signals if s.action == "buy")
        assert buy.adtv_cr is not None
        assert buy.adtv_cr > 0

    def test_adtv_cr_none_when_panels_not_supplied(self):
        engine = _FakeScreenerEngine({"2020-01-01": [_FakeResult("A", 0.9)]})
        adapter = TechnicalAdapter(template_name="A1", top_n=1, screener_engine=engine)
        signals = adapter.generate_signals(["A"], date(2020, 1, 1), HorizonBucket.D21)
        buy = next(s for s in signals if s.action == "buy")
        assert buy.adtv_cr is None


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
