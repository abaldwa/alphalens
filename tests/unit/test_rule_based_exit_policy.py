"""
tests/unit/test_rule_based_exit_policy.py

Phase: 3.x (Paper Trading Logic Fix — Exit Signal bootstrap)
Specs: SPEC-MODEL-002 (barrier convention), SPEC-SOLID-003 (predict_full contract)
Owner: Platform / QA
Consumers: CI, pytest

Mirrors tests/unit/test_exit_signal.py's "6 exit types / pnd override /
exit_type never null" coverage for RuleBasedExitPolicy — the mechanical
stand-in used to bootstrap closed-trade history before ExitSignalModel has
enough real data to train (see BuildLog.md "Paper Trading Logic Fix").
"""

import pandas as pd
import pytest

from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES, PND_EXIT_SCORE_THRESHOLD
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy


def _row(pnl_pct=0.0, days_held=5.0, drawdown=0.0, pnd_score=None):
    row = {"entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown}
    if pnd_score is not None:
        row["pnd_score"] = pnd_score
    return pd.DataFrame([row], index=["TICK"])


class TestRuleBasedExitPolicy:
    def test_target_hit_classified_as_target_achieved_with_high_urgency(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.20))
        assert out["exit_type"].iloc[0] == "target_achieved"
        assert out["exit_urgency"].iloc[0] >= 70.0

    def test_stop_hit_classified_as_thesis_broken_with_high_urgency(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=-0.10))
        assert out["exit_type"].iloc[0] == "thesis_broken"
        assert out["exit_urgency"].iloc[0] >= 80.0

    def test_max_hold_reached_classified_as_opportunity_cost(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.02, days_held=25.0))
        assert out["exit_type"].iloc[0] == "opportunity_cost"

    def test_drawdown_after_gain_classified_as_momentum_exhaustion(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.05, days_held=10.0, drawdown=-0.12))
        assert out["exit_type"].iloc[0] == "momentum_exhaustion"

    def test_untriggered_position_gets_moderate_urgency_and_no_action_band(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.02, days_held=3.0))
        assert 0 <= out["exit_urgency"].iloc[0] <= 60.0

    def test_pnd_score_above_threshold_forces_pnd_exit(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.20, pnd_score=PND_EXIT_SCORE_THRESHOLD + 25.0))
        assert out["exit_type"].iloc[0] == "pnd_exit"
        assert out["exit_urgency"].iloc[0] >= 85.0

    def test_pnd_score_below_threshold_does_not_force_override(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.20, pnd_score=PND_EXIT_SCORE_THRESHOLD - 10.0))
        assert out["exit_type"].iloc[0] != "pnd_exit"

    def test_exit_type_always_valid_and_non_null(self):
        policy = RuleBasedExitPolicy()
        rows = pd.concat([
            _row(pnl_pct=0.20), _row(pnl_pct=-0.10), _row(pnl_pct=0.02, days_held=25.0),
            _row(pnl_pct=0.05, days_held=10.0, drawdown=-0.12), _row(pnl_pct=0.0, days_held=1.0),
        ], ignore_index=False)
        rows.index = [f"T{i}" for i in range(len(rows))]
        out = policy.predict_full(rows)
        assert out["exit_type"].notna().all()
        assert out["exit_type"].isin(EXIT_TYPES).all()

    def test_output_columns_match_exit_signal_model_contract(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.05))
        assert list(out.columns) == [
            "exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
        ]
        assert out["exit_urgency"].between(0, 100).all()

    def test_missing_required_column_raises(self):
        policy = RuleBasedExitPolicy()
        with pytest.raises(ValueError):
            policy.predict_full(pd.DataFrame([{"entry_price": 100.0}], index=["TICK"]))

    def test_invalid_init_args_raise(self):
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(target_pct=-0.1)
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(stop_pct=0.05)
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(max_hold_days=0)
