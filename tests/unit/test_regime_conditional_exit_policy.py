"""
tests/unit/test_regime_conditional_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 3)
Owner: Platform / QA
Consumers: CI, pytest
"""

import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES
from systems.ml_signal_engine.models.exit.regime_conditional_exit_policy import (
    REGIME_STOP_MULTIPLIERS,
    RegimeConditionalExitPolicy,
)

_TEMPLATE_PARAMS = {"T1": {"stop_pct": 0.05, "target_pct": 0.15, "max_hold_days": 21}}


def _row(pnl_pct, regime, template="T1", days_held=5.0, drawdown=0.0):
    row = {
        "template": template, "regime": regime, "days_held": days_held,
        "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown,
    }
    return pd.DataFrame([row], index=["TICK"])


class TestRegimeConditionalExitPolicy:
    def test_multiplier_convention(self):
        assert REGIME_STOP_MULTIPLIERS["bear"] == 0.7
        assert REGIME_STOP_MULTIPLIERS["bull"] == 1.3
        assert REGIME_STOP_MULTIPLIERS["sideways"] == 1.0

    def test_bear_regime_tightens_the_stop(self):
        # base stop 5%, bear -> 0.7 * 5% = 3.5%; a -4% move breaches in Bear
        # but would NOT breach the unscaled 5% stop.
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.04, regime="bear"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_bull_regime_loosens_the_stop(self):
        # base stop 5%, bull -> 1.3 * 5% = 6.5%; a -6% move should NOT breach in Bull.
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.06, regime="bull"))
        assert out["exit_type"].iloc[0] != "thesis_broken"

    def test_sideways_regime_leaves_stop_unchanged(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        # -5.5% breaches the plain 5% stop regardless of regime scaling.
        out = policy.predict_full(_row(pnl_pct=-0.055, regime="sideways"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_same_move_breaches_in_bear_but_not_in_bull(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        bear_out = policy.predict_full(_row(pnl_pct=-0.04, regime="bear"))
        bull_out = policy.predict_full(_row(pnl_pct=-0.04, regime="bull"))
        assert bear_out["exit_type"].iloc[0] == "thesis_broken"
        assert bull_out["exit_type"].iloc[0] != "thesis_broken"

    def test_missing_or_unrecognized_regime_defaults_to_unscaled_stop(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.055, regime=None))
        assert out["exit_type"].iloc[0] == "thesis_broken"  # same as sideways (1.0x)

    def test_target_pct_and_max_hold_are_not_regime_scaled(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        bear_out = policy.predict_full(_row(pnl_pct=0.15, regime="bear"))
        bull_out = policy.predict_full(_row(pnl_pct=0.15, regime="bull"))
        assert bear_out["exit_type"].iloc[0] == "target_achieved"
        assert bull_out["exit_type"].iloc[0] == "target_achieved"

    def test_unmatched_template_falls_back_to_defaults(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS, default_stop_pct=-0.075)
        out = policy.predict_full(_row(pnl_pct=-0.10, regime="sideways", template="UNKNOWN"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_exit_type_always_valid(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=0.0, regime="sideways"))
        assert out["exit_type"].isin(EXIT_TYPES).all()
