"""
tests/unit/test_trailing_stop_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 1)
Owner: Platform / QA
Consumers: CI, pytest
"""

import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES
from systems.ml_signal_engine.models.exit.trailing_stop_exit_policy import TrailingStopExitPolicy


def _row(pnl_pct=0.0, days_held=5.0, drawdown=0.0, atr_pct=None):
    row = {"entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown}
    if atr_pct is not None:
        row["atr_pct"] = atr_pct
    return pd.DataFrame([row], index=["TICK"])


class TestTrailingStopExitPolicy:
    def test_pullback_from_peak_beyond_stop_triggers_even_when_still_profitable(self):
        # Still up 5% from entry (would NOT trigger a flat entry-anchored
        # stop), but down 10% from its own peak -> trailing stop breached.
        policy = TrailingStopExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.05, drawdown=-0.10))
        assert out["exit_type"].iloc[0] == "thesis_broken"
        assert out["exit_urgency"].iloc[0] >= 80.0

    def test_small_pullback_within_stop_distance_does_not_trigger(self):
        policy = TrailingStopExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.05, drawdown=-0.03))
        assert out["exit_type"].iloc[0] == "opportunity_cost"
        assert out["exit_urgency"].iloc[0] < 80.0

    def test_target_hit_off_entry_price_unchanged_from_rule_based(self):
        policy = TrailingStopExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.20, drawdown=-0.02))
        assert out["exit_type"].iloc[0] == "target_achieved"
        assert out["exit_urgency"].iloc[0] >= 70.0

    def test_max_hold_reached_without_target_or_stop(self):
        policy = TrailingStopExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.02, days_held=25.0, drawdown=-0.01))
        assert out["exit_type"].iloc[0] == "opportunity_cost"
        assert out["exit_urgency"].iloc[0] >= 50.0

    def test_atr_scaled_trailing_stop_when_atr_pct_present(self):
        # atr_pct=0.02 -> ATR_STOP_MULTIPLIER(1.0) * 0.02 = 2% trailing stop distance.
        policy = TrailingStopExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.01, drawdown=-0.03, atr_pct=0.02))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_construction_validation(self):
        import pytest

        with pytest.raises(ValueError):
            TrailingStopExitPolicy(target_pct=-0.1)
        with pytest.raises(ValueError):
            TrailingStopExitPolicy(stop_pct=0.05)
        with pytest.raises(ValueError):
            TrailingStopExitPolicy(max_hold_days=0)

    def test_exit_type_always_valid_and_non_null(self):
        policy = TrailingStopExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.0, drawdown=0.0))
        assert out["exit_type"].isin(EXIT_TYPES).all()
        assert out["exit_type"].notna().all()
