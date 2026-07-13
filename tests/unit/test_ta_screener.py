"""
tests/unit/test_ta_screener.py

Phase: 3.x (Technical Analysis Screener Tests)
Specs: SPEC-TA-005, SPEC-TA-006, SPEC-SYS-006
Owner: QA / Platform
Consumers: pytest CI

Unit tests for the TA screener engine, templates registry, and daily alert
checker.  Per SPEC-SYS-006's test-fixture exemption, synthetic DataFrames
are permitted in test files to exercise function logic in isolation — they
are clearly labeled as "test fixture" and never exist in production code
paths.  No real feature Parquets are required for these tests to pass.
"""


import pandas as pd
import pytest

from systems.technical_analysis.screener.engine import ScreenerEngine, ScreenerResult
from systems.technical_analysis.screener.templates import TEMPLATE_MAP, TEMPLATES, ScreenerTemplate


# ---------------------------------------------------------------------------
# Test 1: Templates registry contains exactly 42 templates
# ---------------------------------------------------------------------------

def test_templates_count_exactly_42():
    """SPEC-TA-005: the registry must define exactly 42 pre-built templates.

    Verifies that:
    1. TEMPLATES list has exactly 42 entries.
    2. All template names are unique.
    3. ScreenerEngine.list_templates() also returns exactly 42 TemplateInfo objects.
    """
    assert len(TEMPLATES) == 42, (
        f"Expected 42 templates, got {len(TEMPLATES)}. "
        "Check systems/technical_analysis/screener/templates.py."
    )

    names = [t.name for t in TEMPLATES]
    assert len(set(names)) == 42, "Duplicate template names detected"

    engine = ScreenerEngine()
    infos = engine.list_templates()
    assert len(infos) == 42, (
        f"ScreenerEngine.list_templates() returned {len(infos)}, expected 42"
    )


# ---------------------------------------------------------------------------
# Test 2: BB Squeeze (A1) template filters by volume_ratio_21d correctly
# ---------------------------------------------------------------------------

def _make_minimal_feature_df() -> pd.DataFrame:
    """Create a minimal synthetic DataFrame mimicking the daily feature Parquet.

    This is a test fixture (SPEC-SYS-006 exemption) — it is NOT used in any
    production code path.  Columns mirror the real Parquet schema, values are
    deliberately chosen to test boundary conditions for template A1.

    Returns
    -------
    pd.DataFrame
        4 rows; only TICKER_A passes all A1 conditions:
          - bb_width_pct in bottom 25% of universe (squeeze)
          - sma_200_ratio > 1.0 (above SMA200)
          - volume_ratio_21d > 1.8 (volume surge)
    """
    return pd.DataFrame(
        {
            "ticker": ["TICKER_A", "TICKER_B", "TICKER_C", "TICKER_D"],
            # bb_width_pct: values [5, 20, 6, 15]; quantile(0.25) ≈ 5.75
            # → only TICKER_A (5) and TICKER_C (6) qualify for squeeze
            "bb_width_pct": [5.0, 20.0, 6.0, 15.0],
            # sma_200_ratio: TICKER_A=1.05 (above), TICKER_B=1.05, TICKER_C=0.90 (below), TICKER_D=1.02
            "sma_200_ratio": [1.05, 1.05, 0.90, 1.02],
            # volume_ratio_21d: TICKER_A=2.0 (surge), TICKER_B=2.0, TICKER_C=2.0, TICKER_D=1.0 (no surge)
            "volume_ratio_21d": [2.0, 2.0, 2.0, 1.0],
            # Extra feature referenced in key_display_features
            "rsi_14": [45.0, 60.0, 30.0, 55.0],
        }
    )


def test_a1_bb_squeeze_volume_filter():
    """SPEC-TA-005: A1 template (BB Squeeze Breakout) correctly filters tickers.

    With the test fixture DataFrame:
    - TICKER_A: bb_width=5 (squeeze ✓), sma_200_ratio=1.05 (above ✓), volume=2.0 (surge ✓) → PASS
    - TICKER_B: bb_width=20 (not squeeze ✗) → FAIL
    - TICKER_C: bb_width=6 (squeeze ✓ — bottom 25%), sma_200_ratio=0.90 (below ✗) → FAIL
    - TICKER_D: bb_width=15 (not squeeze ✗) → FAIL

    Note: bottom_pct=0.25 on 4 values [5,20,6,15] gives threshold=5.75
    (pd.Series.quantile(0.25)), so only bb_width_pct <= 5.75 qualifies.
    Only TICKER_A (value=5) passes the squeeze condition.

    Expected: exactly 1 result with ticker="TICKER_A" and score=1.0.
    """
    engine = ScreenerEngine()
    fixture_df = _make_minimal_feature_df()
    date_str = "2026-07-02"

    template = TEMPLATE_MAP["A1"]
    results = engine._screen_df(fixture_df, template, date_str, limit=50)

    # Only TICKER_A should pass all three A1 conditions
    assert len(results) == 1, (
        f"Expected 1 result for A1, got {len(results)}: "
        f"{[r.ticker for r in results]}"
    )
    assert results[0].ticker == "TICKER_A"
    assert results[0].score == pytest.approx(1.0, abs=1e-6)
    assert results[0].matched_conditions == results[0].total_conditions


# ---------------------------------------------------------------------------
# Test 3: screen_custom() with rsi_14 < 30 works correctly
# ---------------------------------------------------------------------------

def _make_rsi_fixture_df() -> pd.DataFrame:
    """Synthetic DataFrame for testing custom RSI condition (test fixture).

    Returns
    -------
    pd.DataFrame
        3 rows: only tickers with rsi_14 < 30 should be returned.
    """
    return pd.DataFrame(
        {
            "ticker": ["OVERSOLD_A", "OVERSOLD_B", "NEUTRAL_C"],
            "rsi_14": [20.0, 28.5, 55.0],
            "sma_200_ratio": [1.05, 1.10, 1.02],
            "volume_ratio_21d": [1.5, 1.2, 1.8],
        }
    )


def test_screen_custom_rsi_filter():
    """SPEC-TA-005: screen_custom() with rsi_14 < 30 returns only oversold tickers.

    Expected: 2 tickers (OVERSOLD_A with rsi=20, OVERSOLD_B with rsi=28.5)
    are returned; NEUTRAL_C (rsi=55) is excluded.
    """
    engine = ScreenerEngine()
    fixture_df = _make_rsi_fixture_df()
    date_str = "2026-07-02"

    from systems.technical_analysis.screener.templates import ScreenerTemplate

    custom_template = ScreenerTemplate(
        name="custom",
        category="custom",
        description="Custom RSI test",
        conditions=[{"feature": "rsi_14", "op": "lt", "value": 30}],
        key_display_features=["rsi_14"],
    )

    results = engine._screen_df(fixture_df, custom_template, date_str, limit=50)

    tickers_returned = {r.ticker for r in results}
    assert "OVERSOLD_A" in tickers_returned
    assert "OVERSOLD_B" in tickers_returned
    assert "NEUTRAL_C" not in tickers_returned
    assert len(results) == 2

    # All returned rows should have score = 1.0 (single condition, all met)
    for r in results:
        assert r.score == pytest.approx(1.0, abs=1e-6)
        assert r.matched_conditions == 1
        assert r.total_conditions == 1


# ---------------------------------------------------------------------------
# Test 4: DailyAlertChecker writes to the ta_signals table correctly
# ---------------------------------------------------------------------------

def test_daily_alert_checker_writes_correct_table():
    """SPEC-TA-006: DailyAlertChecker writes results to ta_signals table.

    Uses an in-memory DuckDB connection (via get_duckdb_connection with db_path=None)
    to verify that:
    1. The ta_signals table is created with the correct schema.
    2. The DailyAlertChecker._write_results_batch() inserts rows with the
       correct column values.
    3. The upsert (ON CONFLICT DO UPDATE) is idempotent.

    The ScreenerEngine is mocked so no real feature Parquet is needed.
    """
    from systems.technical_analysis.alerts.daily_alert_checker import DailyAlertChecker

    # Build a minimal set of fake results — 2 full matches for template A1
    fake_results = [
        ScreenerResult(
            ticker="RELI",
            date="2026-07-02",
            template_name="A1",
            matched_conditions=3,
            total_conditions=3,
            score=1.0,
            key_values={"bb_width_pct": 5.1, "sma_200_ratio": 1.05},
        ),
        ScreenerResult(
            ticker="INFY",
            date="2026-07-02",
            template_name="A1",
            matched_conditions=3,
            total_conditions=3,
            score=1.0,
            key_values={"bb_width_pct": 4.8, "sma_200_ratio": 1.10},
        ),
    ]

    checker = DailyAlertChecker()

    # Use an in-memory DuckDB connection for the write test
    # (get_duckdb_connection with db_path=None opens :memory:)
    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(db_path=None) as conn:
        checker._ensure_db_and_table(conn)

        # Verify the table was created
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_name = 'ta_signals'"
            ).fetchall()
        ]
        assert "ta_signals" in tables, "ta_signals table was not created"

        # Write the fake results
        checker._write_results_batch(conn, "2026-07-02", fake_results)

        # Verify row count and values
        rows = conn.execute(
            "SELECT date, ticker, template_name, score FROM ta_signals ORDER BY ticker"
        ).fetchall()

        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

        tickers_stored = {r[1] for r in rows}
        assert tickers_stored == {"INFY", "RELI"}

        # Verify all rows reference the correct table / template
        for r in rows:
            assert str(r[2]) == "A1"
            assert abs(float(r[3]) - 1.0) < 1e-6

        # Test idempotency: write the same batch again (ON CONFLICT DO UPDATE)
        checker._write_results_batch(conn, "2026-07-02", fake_results)
        rows_after = conn.execute(
            "SELECT COUNT(*) FROM ta_signals"
        ).fetchone()[0]
        assert rows_after == 2, (
            f"Expected 2 rows after idempotent re-write, got {rows_after}"
        )


# ---------------------------------------------------------------------------
# Test 5: additional condition ops (gte, lte, eq, between, top_pct, gt_col,
# unknown op, exception path) and edge cases in ScreenerEngine._screen_df /
# _apply_single_condition / _load_df / screen() / screen_custom().
# ---------------------------------------------------------------------------


def _make_ops_fixture_df() -> pd.DataFrame:
    """Test fixture (SPEC-SYS-006 exemption) covering all supported ops."""
    return pd.DataFrame(
        {
            "ticker": ["T1", "T2", "T3", "T4"],
            "feat_a": [10.0, 20.0, 30.0, 40.0],
            "feat_b": [5.0, 25.0, 25.0, 45.0],
            "volume_ratio_21d": [1.0, 2.0, 3.0, 4.0],
        }
    )


class TestApplySingleConditionOps:
    def test_gte_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "gte", "value": 20.0}, frozenset(df.columns)
        )
        assert not missing
        assert list(mask) == [False, True, True, True]

    def test_lte_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "lte", "value": 20.0}, frozenset(df.columns)
        )
        assert not missing
        assert list(mask) == [True, True, False, False]

    def test_eq_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "eq", "value": 30.0}, frozenset(df.columns)
        )
        assert list(mask) == [False, False, True, False]

    def test_between_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "between", "value": [15.0, 35.0]}, frozenset(df.columns)
        )
        assert list(mask) == [False, True, True, False]

    def test_top_pct_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "top_pct", "value": 0.25}, frozenset(df.columns)
        )
        assert not missing
        assert mask.iloc[-1]  # T4 (highest value) always in top 25%

    def test_gt_col_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "gt_col", "feature2": "feat_b"}, frozenset(df.columns)
        )
        assert not missing
        assert list(mask) == [True, False, True, False]

    def test_lt_col_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "lt_col", "feature2": "feat_b"}, frozenset(df.columns)
        )
        assert list(mask) == [False, True, False, True]

    def test_gte_col_and_lte_col_ops(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask_gte, _ = engine._apply_single_condition(
            df, {"feature": "feat_b", "op": "gte_col", "feature2": "feat_b"}, frozenset(df.columns)
        )
        assert mask_gte.all()
        mask_lte, _ = engine._apply_single_condition(
            df, {"feature": "feat_b", "op": "lte_col", "feature2": "feat_b"}, frozenset(df.columns)
        )
        assert mask_lte.all()

    def test_col_vs_col_missing_feature2(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "gt_col", "feature2": "not_a_col"}, frozenset(df.columns)
        )
        assert missing
        assert not mask.any()

    def test_missing_feature_column(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "no_such_feature", "op": "lt", "value": 10}, frozenset(df.columns)
        )
        assert missing
        assert not mask.any()

    def test_unknown_op(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "frobnicate", "value": 1}, frozenset(df.columns)
        )
        assert not missing
        assert not mask.any()

    def test_condition_evaluation_exception_is_caught(self):
        # 'between' with a malformed value (not a 2-element sequence) raises internally;
        # the engine must catch it and treat the condition as unmet rather than propagate.
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        mask, missing = engine._apply_single_condition(
            df, {"feature": "feat_a", "op": "between", "value": 5.0}, frozenset(df.columns)
        )
        assert not mask.any()


class TestScreenDfTiebreakOrdering:
    """T9 regression: secondary sort must never silently degrade to
    ticker-alphabetical (source Parquet row) order when volume_ratio_21d
    (or any other volume proxy) is absent from the day's feature set.
    """

    def _tied_fixture_df(self) -> pd.DataFrame:
        """Test fixture (SPEC-SYS-006 exemption): 5 tickers, all tied on the
        single condition (score == 1.0), stored in alphabetical row order,
        with NO volume/volume-proxy columns at all — reproducing the T9 bug
        scenario where volume_ratio_21d is missing from the day's Parquet.
        """
        return pd.DataFrame(
            {
                "ticker": ["AAA", "BBB", "CCC", "DDD", "EEE"],
                "feat_a": [10.0, 10.0, 10.0, 10.0, 10.0],
            }
        )

    def test_missing_volume_columns_does_not_fall_back_to_alphabetical_order(self):
        engine = ScreenerEngine()
        df = self._tied_fixture_df()
        template = ScreenerTemplate(
            name="all_tied",
            category="custom",
            description="all rows tie on a single condition",
            conditions=[{"feature": "feat_a", "op": "gte", "value": 0.0}],
        )

        results = engine._screen_df(df, template, "2026-07-02", limit=50)

        assert len(results) == 5
        result_order = [r.ticker for r in results]
        # All 5 tickers must still be present (nothing silently dropped)...
        assert set(result_order) == {"AAA", "BBB", "CCC", "DDD", "EEE"}
        # ...but the order must NOT simply be the alphabetical source order,
        # since that was the T9 symptom (screener "only picks up tickers in
        # alphabetical order" when volume_ratio_21d is unavailable).
        assert result_order != ["AAA", "BBB", "CCC", "DDD", "EEE"], (
            "Screener fell back to alphabetical row order when no volume "
            "proxy column was available — T9 regression."
        )

    def test_tiebreak_order_is_deterministic_across_calls(self):
        engine = ScreenerEngine()
        template = ScreenerTemplate(
            name="all_tied",
            category="custom",
            description="all rows tie on a single condition",
            conditions=[{"feature": "feat_a", "op": "gte", "value": 0.0}],
        )

        order_1 = [
            r.ticker
            for r in engine._screen_df(self._tied_fixture_df(), template, "2026-07-02", limit=50)
        ]
        order_2 = [
            r.ticker
            for r in engine._screen_df(self._tied_fixture_df(), template, "2026-07-02", limit=50)
        ]
        assert order_1 == order_2

    def test_volume_ratio_21d_present_still_used_as_primary_tiebreak(self):
        """When volume_ratio_21d IS present, it must still take priority over
        the hash-based fallback tiebreak (i.e. no behavior change for the
        common case where the column is available).
        """
        engine = ScreenerEngine()
        df = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB", "CCC"],
                "feat_a": [10.0, 10.0, 10.0],
                "volume_ratio_21d": [1.0, 3.0, 2.0],
            }
        )
        template = ScreenerTemplate(
            name="all_tied",
            category="custom",
            description="all rows tie on a single condition",
            conditions=[{"feature": "feat_a", "op": "gte", "value": 0.0}],
        )

        results = engine._screen_df(df, template, "2026-07-02", limit=50)
        assert [r.ticker for r in results] == ["BBB", "CCC", "AAA"]


class TestScreenDfEdgeCases:
    def test_empty_dataframe_returns_no_results(self):
        engine = ScreenerEngine()
        empty_df = pd.DataFrame(columns=["ticker", "feat_a"])
        template = TEMPLATE_MAP["A1"]
        assert engine._screen_df(empty_df, template, "2026-07-02", limit=50) == []

    def test_missing_ticker_column_returns_no_results(self):
        engine = ScreenerEngine()
        df = pd.DataFrame({"feat_a": [1.0, 2.0]})
        template = TEMPLATE_MAP["A1"]
        assert engine._screen_df(df, template, "2026-07-02", limit=50) == []

    def test_zero_conditions_returns_no_results(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        template = ScreenerTemplate(
            name="empty", category="custom", description="No conditions", conditions=[],
        )
        assert engine._screen_df(df, template, "2026-07-02", limit=50) == []

    def test_limit_truncates_results(self):
        engine = ScreenerEngine()
        df = _make_ops_fixture_df()
        template = ScreenerTemplate(
            name="all_pass", category="custom", description="always true",
            conditions=[{"feature": "feat_a", "op": "gte", "value": 0.0}],
        )
        results = engine._screen_df(df, template, "2026-07-02", limit=2)
        assert len(results) == 2


class TestScreenerEnginePublicMethods:
    def test_screen_unknown_template_raises_keyerror(self):
        engine = ScreenerEngine()
        with pytest.raises(KeyError, match="Unknown template"):
            engine.screen("NOT_A_TEMPLATE")

    def test_load_df_missing_file_returns_none(self, tmp_path, monkeypatch):
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        engine = ScreenerEngine()
        assert engine._load_df("2099-01-01") is None

    def test_screen_reads_real_parquet_end_to_end(self, tmp_path, monkeypatch):
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        df = _make_minimal_feature_df()
        df.to_parquet(tmp_path / "2026-07-02.parquet")

        engine = ScreenerEngine()
        results = engine.screen("A1", date="2026-07-02", limit=50)
        assert len(results) == 1
        assert results[0].ticker == "TICKER_A"

    def test_screen_no_parquet_for_date_returns_empty(self, tmp_path, monkeypatch):
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        engine = ScreenerEngine()
        assert engine.screen("A1", date="2099-01-01", limit=50) == []

    def test_screen_custom_end_to_end(self, tmp_path, monkeypatch):
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        df = _make_rsi_fixture_df()
        df.to_parquet(tmp_path / "2026-07-02.parquet")

        engine = ScreenerEngine()
        results = engine.screen_custom(
            [{"feature": "rsi_14", "op": "lt", "value": 30}], date="2026-07-02", limit=50
        )
        tickers = {r.ticker for r in results}
        assert tickers == {"OVERSOLD_A", "OVERSOLD_B"}
        assert all(r.template_name == "custom" for r in results)

    def test_screen_custom_no_resolved_date_returns_empty(self, tmp_path, monkeypatch):
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        monkeypatch.setattr(engine_mod, "resolve_date", lambda date: None)
        engine = ScreenerEngine()
        assert engine.screen_custom([{"feature": "x", "op": "lt", "value": 1}]) == []


# ---------------------------------------------------------------------------
# Test 6: DailyAlertChecker.evaluate() and run() end-to-end (SPEC-TA-006).
# ---------------------------------------------------------------------------


class TestDailyAlertCheckerEvaluateAndRun:
    def test_evaluate_no_parquet_returns_none_and_empty(self, tmp_path, monkeypatch):
        import systems.technical_analysis.alerts.daily_alert_checker as checker_mod

        monkeypatch.setattr(checker_mod, "resolve_date", lambda run_date: None)
        checker = checker_mod.DailyAlertChecker()
        resolved, results = checker.evaluate("2099-01-01")
        assert resolved is None
        assert results == {}

    def test_evaluate_runs_all_42_templates(self, tmp_path, monkeypatch):
        import systems.technical_analysis.alerts.daily_alert_checker as checker_mod
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        df = _make_minimal_feature_df()
        df.to_parquet(tmp_path / "2026-07-02.parquet")

        checker = checker_mod.DailyAlertChecker()
        resolved, template_results = checker.evaluate("2026-07-02")
        assert resolved == "2026-07-02"
        assert len(template_results) == 42
        # A1 should have TICKER_A as a full match (verified in test 2 above)
        assert any(r.ticker == "TICKER_A" for r in template_results.get("A1", []))

    def test_evaluate_template_exception_is_caught_and_isolated(self, tmp_path, monkeypatch):
        import systems.technical_analysis.alerts.daily_alert_checker as checker_mod
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        df = _make_minimal_feature_df()
        df.to_parquet(tmp_path / "2026-07-02.parquet")

        checker = checker_mod.DailyAlertChecker()

        original_screen = checker._engine.screen

        def flaky_screen(template_name, date=None, limit=50):
            if template_name == "A1":
                raise RuntimeError("boom")
            return original_screen(template_name, date=date, limit=limit)

        monkeypatch.setattr(checker._engine, "screen", flaky_screen)
        resolved, template_results = checker.evaluate("2026-07-02")
        assert resolved == "2026-07-02"
        assert template_results["A1"] == []  # failed template degrades to empty, not a crash
        assert len(template_results) == 42

    def test_run_writes_to_signals_db_and_returns_counts(self, tmp_path, monkeypatch):
        import systems.technical_analysis.alerts.daily_alert_checker as checker_mod
        import systems.technical_analysis.screener.engine as engine_mod

        monkeypatch.setattr(engine_mod, "FEATURES_DAILY_DIR", tmp_path)
        df = _make_minimal_feature_df()
        df.to_parquet(tmp_path / "2026-07-02.parquet")

        signals_db_path = tmp_path / "signals" / "signals.duckdb"
        monkeypatch.setattr(checker_mod, "SIGNALS_DUCKDB_PATH", signals_db_path)

        checker = checker_mod.DailyAlertChecker()
        counts = checker.run("2026-07-02")

        assert len(counts) == 42
        assert counts["A1"] == 1

        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(signals_db_path, persist=False) as conn:
            rows = conn.execute(
                "SELECT ticker, template_name FROM ta_signals"
            ).fetchall()
        assert ("TICKER_A", "A1") in rows

    def test_run_no_parquet_returns_empty_dict(self, tmp_path, monkeypatch):
        import systems.technical_analysis.alerts.daily_alert_checker as checker_mod

        monkeypatch.setattr(checker_mod, "resolve_date", lambda run_date: None)
        checker = checker_mod.DailyAlertChecker()
        assert checker.run("2099-01-01") == {}
