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

from systems.technical_analysis.screener.engine import ScreenerEngine
from systems.technical_analysis.screener.templates import TEMPLATE_MAP, TEMPLATES


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
    from systems.technical_analysis.screener.engine import ScreenerResult

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
