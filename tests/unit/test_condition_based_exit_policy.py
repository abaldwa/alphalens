"""
tests/unit/test_condition_based_exit_policy.py

Phase: 3.x (Exit policy experimentation — Variant B)
Owner: Platform / QA
Consumers: CI, pytest

Synthetic exit_ctx rows with known indicator values — no live Parquet/DB
dependency, matching the pattern in test_rule_based_exit_policy.py.
"""

import pandas as pd

from systems.ml_signal_engine.models.exit.condition_based_exit_policy import (
    ConditionBasedExitPolicy,
    _derive_exit_rule,
    build_template_exit_rules,
)


def _row(template, **features):
    row = {"template": template, **features}
    return pd.DataFrame([row], index=["TICK"])


class TestDeriveExitRule:
    def test_boundary_feature_flips_to_fixed_boundary_ignoring_entry_threshold(self):
        # sma_200_ratio entry gt 1.0 -> exit lt 1.0 (boundary, not the entry value)
        assert _derive_exit_rule("sma_200_ratio", "gt", 1.0) == ("sma_200_ratio", "lt", 1.0)
        assert _derive_exit_rule("macd_hist", "gt", 0) == ("macd_hist", "lt", 0.0)
        assert _derive_exit_rule("ema_ribbon_alignment", "gte", 1.0) == ("ema_ribbon_alignment", "lt", 0.0)
        assert _derive_exit_rule("ema_8_ratio", "gt", 1.0) == ("ema_8_ratio", "lt", 1.0)
        assert _derive_exit_rule("supertrend_dir", "gt", 0) == ("supertrend_dir", "lt", 0.0)
        assert _derive_exit_rule("ichimoku_cloud_position", "gt", 0) == ("ichimoku_cloud_position", "lt", 0.0)

    def test_adx_relaxed_reversal(self):
        assert _derive_exit_rule("adx_14", "gt", 20) == ("adx_14", "lt", 18.0)

    def test_rsi_bullish_entry_relaxed_reversal(self):
        assert _derive_exit_rule("rsi_14", "gte", 40) == ("rsi_14", "lt", 35.0)

    def test_rsi_oversold_entry_relaxed_reversal(self):
        assert _derive_exit_rule("rsi_14", "lt", 30) == ("rsi_14", "gt", 35.0)

    def test_williams_r_oversold_relaxed_reversal(self):
        assert _derive_exit_rule("williams_r", "lt", -85) == ("williams_r", "gt", -80.0)

    def test_roc_10_momentum_entry_exits_on_zero_regardless_of_entry_threshold(self):
        assert _derive_exit_rule("roc_10", "gt", 5) == ("roc_10", "lt", 0.0)

    def test_excluded_features_derive_no_rule(self):
        assert _derive_exit_rule("volume_ratio_21d", "gt", 1.5) is None
        assert _derive_exit_rule("bb_width_pct", "bottom_pct", 0.25) is None
        assert _derive_exit_rule("base_breakout_ratio", "gt", 0.97) is None
        assert _derive_exit_rule("base_breakout_score", "gt", 0.5) is None
        assert _derive_exit_rule("double_bottom_score", "gt", 0.4) is None
        assert _derive_exit_rule("flag_pattern_score", "gt", 0.5) is None
        assert _derive_exit_rule("hurst_exp_21d", "gt", 0.5) is None

    def test_top_pct_bottom_pct_and_between_excluded_regardless_of_feature(self):
        assert _derive_exit_rule("roc_10", "top_pct", 0.2) is None
        assert _derive_exit_rule("rsi_14", "between", [40, 60]) is None

    def test_build_template_exit_rules_derives_only_from_real_templates(self):
        rules = build_template_exit_rules()
        assert "A4" in rules  # RSI Oversold + Trend: rsi_14 lt 30, sma_200_ratio gt 1.0 -> both derivable
        assert ("sma_200_ratio", "lt", 1.0) in rules["A4"]


class TestConditionBasedExitPolicy:
    def test_boundary_breach_triggers_high_urgency_thesis_broken(self):
        policy = ConditionBasedExitPolicy(template_rules={"T1": [("sma_200_ratio", "lt", 1.0)]})
        out = policy.predict_full(_row("T1", sma_200_ratio=0.95))
        assert out["exit_type"].iloc[0] == "thesis_broken"
        assert out["exit_urgency"].iloc[0] > 80.0

    def test_no_breach_stays_below_hold_threshold(self):
        policy = ConditionBasedExitPolicy(template_rules={"T1": [("sma_200_ratio", "lt", 1.0)]})
        out = policy.predict_full(_row("T1", sma_200_ratio=1.10))
        assert out["exit_type"].iloc[0] == "opportunity_cost"
        assert out["exit_urgency"].iloc[0] <= 40.0

    def test_or_logic_any_one_rule_breach_triggers(self):
        policy = ConditionBasedExitPolicy(
            template_rules={"T1": [("sma_200_ratio", "lt", 1.0), ("rsi_14", "lt", 35.0)]}
        )
        # sma_200_ratio still fine, but rsi_14 breached
        out = policy.predict_full(_row("T1", sma_200_ratio=1.2, rsi_14=20.0))
        assert out["exit_type"].iloc[0] == "thesis_broken"
        assert out["exit_urgency"].iloc[0] > 80.0

    def test_template_with_no_derived_rules_never_triggers(self):
        policy = ConditionBasedExitPolicy(template_rules={})
        out = policy.predict_full(_row("UNKNOWN_TEMPLATE", sma_200_ratio=0.5))
        assert out["exit_urgency"].iloc[0] == 0.0 or out["exit_urgency"].iloc[0] <= 40.0
        assert out["exit_type"].iloc[0] == "opportunity_cost"

    def test_missing_feature_column_treated_as_not_breached_not_an_error(self):
        policy = ConditionBasedExitPolicy(template_rules={"T1": [("rsi_14", "lt", 35.0)]})
        out = policy.predict_full(_row("T1"))  # no rsi_14 column at all
        assert out["exit_type"].iloc[0] == "opportunity_cost"

    def test_missing_template_column_never_triggers(self):
        policy = ConditionBasedExitPolicy(template_rules={"T1": [("rsi_14", "lt", 35.0)]})
        out = policy.predict_full(pd.DataFrame([{"rsi_14": 10.0}], index=["TICK"]))
        assert out["exit_type"].iloc[0] == "opportunity_cost"

    def test_exit_type_always_valid(self):
        from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES

        policy = ConditionBasedExitPolicy(template_rules={"T1": [("sma_200_ratio", "lt", 1.0)]})
        out = policy.predict_full(_row("T1", sma_200_ratio=0.5))
        assert out["exit_type"].isin(EXIT_TYPES).all()
