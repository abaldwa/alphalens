"""
tests/unit/test_ta_templates_category_t.py

Phase: 3.x (Technical Analysis Screener — Category T)
Specs: SPEC-TA-005
Owner: QA / Platform
Consumers: pytest CI

Structural unit tests for the 20 Category-T templates (T01-T20) added
2026-08-08 from the "Technical Strategies from AlphaLens Indicators" brief.

These tests are deliberately STRUCTURAL, not statistical: they assert the
templates are well-formed, registered, styled, and reference only real
feature columns. They must not require a real feature Parquet, so the
"does this template actually match stocks" question is covered by the
separate live smoke-check documented in the module docstring of
systems/technical_analysis/screener/templates.py (Category T section),
not here — a unit test that silently passes when a template matches zero
rows every day would be worse than no test at all.
"""

import pytest

from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.pattern_scores import PATTERN_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from systems.technical_analysis.screener.templates import (
    STRATEGY_STYLES,
    TEMPLATE_MAP,
    TEMPLATE_STYLE,
    TEMPLATES,
)

T_NAMES = [f"T{i:02d}" for i in range(1, 21)]

VALID_OPS = {"lt", "gt", "lte", "gte", "eq", "between", "top_pct", "bottom_pct"}


@pytest.fixture(scope="module")
def known_features():
    """Every feature column the daily Parquet is expected to carry."""
    cols = set(CORE_TECHNICAL_FEATURES) | set(ADVANCED_TECHNICAL_FEATURES) | set(PATTERN_FEATURES)
    return cols


class TestCategoryTRegistration:
    def test_all_twenty_registered(self):
        missing = [n for n in T_NAMES if n not in TEMPLATE_MAP]
        assert not missing, f"Category-T templates missing from registry: {missing}"

    def test_category_letter_is_t(self):
        for n in T_NAMES:
            assert TEMPLATE_MAP[n].category == "T", f"{n} has category {TEMPLATE_MAP[n].category!r}, expected 'T'"

    def test_exactly_twenty_t_templates(self):
        found = [t.name for t in TEMPLATES if t.category == "T"]
        assert len(found) == 20, f"Expected 20 Category-T templates, found {len(found)}: {sorted(found)}"

    def test_descriptions_are_unique_and_nonempty(self):
        descs = [TEMPLATE_MAP[n].description for n in T_NAMES]
        assert all(d.strip() for d in descs), "Every Category-T template needs a description"
        assert len(set(descs)) == 20, "Category-T descriptions must be unique"


class TestCategoryTStyling:
    def test_every_t_template_has_a_known_style(self):
        for n in T_NAMES:
            assert n in TEMPLATE_STYLE, f"{n} missing from TEMPLATE_STYLE"
            assert TEMPLATE_STYLE[n] in STRATEGY_STYLES, (
                f"{n} has style {TEMPLATE_STYLE[n]!r}, not in STRATEGY_STYLES={STRATEGY_STYLES}"
            )

    def test_exit_params_populated_from_style(self):
        """The bulk style->exit-params loop must have reached Category T;
        an unstyled template would silently backtest with no stop/target."""
        for n in T_NAMES:
            t = TEMPLATE_MAP[n]
            assert t.exit_stop_pct is not None, f"{n} has no exit_stop_pct"
            assert t.exit_target_pct is not None, f"{n} has no exit_target_pct"
            assert t.exit_max_hold_days is not None, f"{n} has no exit_max_hold_days"
            assert 0 < t.exit_stop_pct < 1, f"{n} exit_stop_pct={t.exit_stop_pct} out of range"
            assert 0 < t.exit_target_pct < 1, f"{n} exit_target_pct={t.exit_target_pct} out of range"
            assert t.exit_max_hold_days > 0, f"{n} exit_max_hold_days={t.exit_max_hold_days}"


class TestCategoryTConditions:
    def test_every_template_has_conditions(self):
        for n in T_NAMES:
            assert TEMPLATE_MAP[n].conditions, f"{n} has no conditions — would match the entire universe"

    def test_condition_ops_are_supported(self):
        for n in T_NAMES:
            for c in TEMPLATE_MAP[n].conditions:
                assert c["op"] in VALID_OPS, f"{n} uses unsupported op {c['op']!r}"

    def test_condition_features_exist(self, known_features):
        """A typo'd feature name is silently treated as 'condition unmet' by
        the engine rather than raising, so it would quietly make a template
        match nothing forever. This is the test that catches that."""
        for n in T_NAMES:
            for c in TEMPLATE_MAP[n].conditions:
                assert c["feature"] in known_features, (
                    f"{n} references unknown feature {c['feature']!r}"
                )

    def test_display_features_exist(self, known_features):
        for n in T_NAMES:
            for f in TEMPLATE_MAP[n].key_display_features:
                assert f in known_features, f"{n} displays unknown feature {f!r}"

    def test_pct_ops_have_fractional_values(self):
        """top_pct/bottom_pct take a fraction in (0,1) — passing e.g. 40
        instead of 0.40 would make the quantile call meaningless."""
        for n in T_NAMES:
            for c in TEMPLATE_MAP[n].conditions:
                if c["op"] in ("top_pct", "bottom_pct"):
                    v = c["value"]
                    assert isinstance(v, float) and 0.0 < v < 1.0, (
                        f"{n}: {c['op']} on {c['feature']} has value {v!r}, expected a fraction in (0,1)"
                    )

    def test_between_values_are_ordered_pairs(self):
        for n in T_NAMES:
            for c in TEMPLATE_MAP[n].conditions:
                if c["op"] == "between":
                    v = c["value"]
                    assert isinstance(v, list) and len(v) == 2, (
                        f"{n}: between on {c['feature']} needs a [lo, hi] pair, got {v!r}"
                    )
                    assert v[0] <= v[1], f"{n}: between on {c['feature']} has lo>hi: {v!r}"


class TestDegenerateFeaturesNotUsedAsConditions:
    """fractal_dimension is constant 1.0 across the universe and
    sample_entropy_21d is saturated at a clipped 23.026 for >50% of rows
    (both measured on the 2026-08-07 snapshot). Using either as a CONDITION
    filters nothing or everything, so Category T deliberately references
    them only in key_display_features. If a future edit adds one as a
    condition, that is almost certainly a mistake — this test says so
    loudly rather than letting the template quietly stop discriminating."""

    DEGENERATE = {"fractal_dimension", "sample_entropy_21d"}

    def test_degenerate_features_not_used_in_conditions(self):
        for n in T_NAMES:
            used = {c["feature"] for c in TEMPLATE_MAP[n].conditions} & self.DEGENERATE
            assert not used, (
                f"{n} uses degenerate feature(s) {used} as a condition — "
                "these do not discriminate; keep them in key_display_features only"
            )
