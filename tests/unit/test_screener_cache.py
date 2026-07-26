"""
tests/unit/test_screener_cache.py

Unit tests for backtest/core/screener_cache.py — the technical_screener_cache
table read/write helpers TechnicalAdapter uses to share entry-signal
candidates across exit-variant jobs for the same (template, date).
"""

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_backtest

from backtest.core.screener_cache import get_or_compute
from systems.technical_analysis.screener.engine import ScreenerResult


class _FakeResult:
    def __init__(self, ticker, score, matched=3, total=4, key_values=None):
        self.ticker = ticker
        self.score = score
        self.matched_conditions = matched
        self.total_conditions = total
        self.key_values = key_values or {}


class _CountingEngine:
    """Records every screen() call — used to assert a cache hit never
    re-invokes the underlying (expensive) screener."""

    def __init__(self, results_by_date):
        self._results_by_date = results_by_date
        self.calls = []

    def screen(self, template_name, date=None, limit=50):
        self.calls.append((template_name, date, limit))
        return self._results_by_date.get(date, [])


def _conn():
    create_backtest.create_backtest_schema(in_memory=True)
    return get_duckdb_connection(None)


class TestGetOrCompute:
    def test_miss_computes_and_caches(self):
        engine = _CountingEngine({"2023-01-03": [_FakeResult("A", 0.9, key_values={"rsi_14": 28.0})]})
        with _conn() as conn:
            results = get_or_compute(conn, engine, "A1", "2023-01-03")
        assert [r.ticker for r in results] == ["A"]
        assert engine.calls == [("A1", "2023-01-03", 10_000)]  # populate-limit, never a job's own top_n

    def test_hit_does_not_recompute(self):
        engine = _CountingEngine({"2023-01-03": [_FakeResult("A", 0.9)]})
        with _conn() as conn:
            get_or_compute(conn, engine, "A1", "2023-01-03")
            get_or_compute(conn, engine, "A1", "2023-01-03")
            get_or_compute(conn, engine, "A1", "2023-01-03")
        assert len(engine.calls) == 1

    def test_hit_from_a_second_engine_instance_still_does_not_recompute(self):
        # Simulates a second job's subprocess: same cache (same DuckDB
        # file/connection in this test), a DIFFERENT ScreenerEngine
        # instance/object — proves the cache genuinely lives outside any
        # one engine/adapter instance's memory.
        engine1 = _CountingEngine({"2023-01-03": [_FakeResult("A", 0.9)]})
        engine2 = _CountingEngine({"2023-01-03": [_FakeResult("A", 0.9)]})
        with _conn() as conn:
            get_or_compute(conn, engine1, "A1", "2023-01-03")
            results = get_or_compute(conn, engine2, "A1", "2023-01-03")
        assert engine1.calls == [("A1", "2023-01-03", 10_000)]
        assert engine2.calls == []
        assert [r.ticker for r in results] == ["A"]

    def test_zero_match_day_is_cached_as_genuinely_empty_not_a_miss(self):
        engine = _CountingEngine({"2023-01-03": []})
        with _conn() as conn:
            first = get_or_compute(conn, engine, "A1", "2023-01-03")
            second = get_or_compute(conn, engine, "A1", "2023-01-03")
        assert first == []
        assert second == []
        assert len(engine.calls) == 1  # second call was a real cache hit, not a re-miss

    def test_key_values_round_trip_through_the_cache(self):
        engine = _CountingEngine({
            "2023-01-03": [_FakeResult("A", 0.75, matched=3, total=4, key_values={"rsi_14": 28.0, "adx_14": 31.5})],
        })
        with _conn() as conn:
            get_or_compute(conn, engine, "A1", "2023-01-03")  # populate
            cached = get_or_compute(conn, engine, "A1", "2023-01-03")  # read back
        assert len(cached) == 1
        r = cached[0]
        assert isinstance(r, ScreenerResult)
        assert r.ticker == "A"
        assert r.score == 0.75
        assert r.matched_conditions == 3
        assert r.total_conditions == 4
        assert r.key_values == {"rsi_14": 28.0, "adx_14": 31.5}

    def test_different_dates_are_cached_independently(self):
        engine = _CountingEngine({
            "2023-01-03": [_FakeResult("A", 0.9)],
            "2023-01-04": [_FakeResult("B", 0.8)],
        })
        with _conn() as conn:
            day1 = get_or_compute(conn, engine, "A1", "2023-01-03")
            day2 = get_or_compute(conn, engine, "A1", "2023-01-04")
        assert [r.ticker for r in day1] == ["A"]
        assert [r.ticker for r in day2] == ["B"]
        assert len(engine.calls) == 2

    def test_different_templates_same_date_are_cached_independently(self):
        engine = _CountingEngine({"2023-01-03": [_FakeResult("A", 0.9)]})

        def screen_by_template(template_name, date=None, limit=50):
            engine.calls.append((template_name, date, limit))
            if template_name == "A1":
                return [_FakeResult("A", 0.9)]
            return [_FakeResult("Z", 0.6)]

        engine.screen = screen_by_template
        with _conn() as conn:
            a1 = get_or_compute(conn, engine, "A1", "2023-01-03")
            a2 = get_or_compute(conn, engine, "A2", "2023-01-03")
        assert [r.ticker for r in a1] == ["A"]
        assert [r.ticker for r in a2] == ["Z"]
