"""
Gate: every exit trigger a policy declares must be able to close a position.

This is the test that would have caught the defect the 2009-2026 sweep shipped
with. Exits are a two-step — a policy emits `exit_urgency`, and
PortfolioSimulator.exit_action_for_urgency requires a score STRICTLY ABOVE
EXIT_URGENT_THRESHOLD (80) to sell — and nothing anywhere asserted that a
trigger's score could actually reach the action threshold.

Three of RuleBasedExitPolicy's four documented triggers could not:

    max hold days      clip(50 + days - max_hold,   50, 65)  -> never
    momentum exhausted clip(60 + (-drawdown)*100,   60, 79)  -> never
    target achieved    clip(70 + (pnl - target)*50, 70, 90)  -> needs ~+20pp

Measured over 65 baseline runs and 108,762 model-driven exits: 90.94% stops,
8.06% target-with-overshoot, and 0.00% time exits. Every barrier looked wired
and produced entirely plausible results.

The test asserts a capability, not a number, so re-tuning urgency bands is free
while removing a trigger's ability to fire is loud.
"""

import pandas as pd
import pytest

from backtest.portfolio import PortfolioSimulator
from systems.ml_signal_engine.models.exit.risk_managed_exit_policy import RiskManagedExitPolicy


def _ctx(**overrides):
    """One open position's exit context, healthy by default."""
    row = {
        "entry_price": 100.0, "price": 100.0, "peak_price": 100.0,
        "days_held": 1, "unrealised_pnl_pct": 0.0, "drawdown_from_peak": 0.0,
        "atr_pct": float("nan"), "momentum_3m": 0.0, "pnd_score": 0.0,
        "template": "A1", "pillar": "technical", "regime": "bull",
    }
    row.update(overrides)
    return pd.DataFrame([row], index=["ACME"])


def _sells(policy, ctx) -> bool:
    """True if the engine would actually sell on this context.

    Mirrors backtest/core/engine.py's decision exactly: score the context, then
    map through the portfolio's own threshold logic. Reimplementing the
    comparison here would let the test pass while the engine still holds.
    """
    urgency = float(policy.predict_full(ctx)["exit_urgency"].loc["ACME"])
    return PortfolioSimulator.exit_action_for_urgency(urgency) == "immediate_exit"


class TestRiskManagedPolicyTriggersAllFire:
    """The variant added for the re-run. Every declared barrier must work."""

    @pytest.fixture()
    def policy(self):
        return RiskManagedExitPolicy(target_pct=0.12, stop_pct=-0.05, max_hold_days=25)

    def test_stop_loss_closes_the_position(self, policy):
        assert _sells(policy, _ctx(unrealised_pnl_pct=-0.06))

    def test_profit_target_closes_the_position(self, policy):
        """No overshoot required — hitting the target is the trigger."""
        assert _sells(policy, _ctx(unrealised_pnl_pct=0.12))

    def test_max_hold_days_closes_the_position(self, policy):
        """The trigger that had never once fired in production."""
        assert _sells(policy, _ctx(days_held=25))

    def test_momentum_exhaustion_closes_the_position(self, policy):
        assert _sells(policy, _ctx(unrealised_pnl_pct=0.05, drawdown_from_peak=-0.12))

    def test_healthy_position_is_held(self, policy):
        assert not _sells(policy, _ctx(unrealised_pnl_pct=0.03, days_held=5))

    def test_stop_outranks_max_hold_when_both_trigger(self, policy):
        """Severity ordering must survive: the reason reported should be the
        most severe one, not whichever mask was applied last."""
        ctx = _ctx(unrealised_pnl_pct=-0.06, days_held=30)
        assert policy.predict_full(ctx)["exit_type"].loc["ACME"] == "thesis_broken"

    def test_target_outranks_max_hold_when_both_trigger(self, policy):
        ctx = _ctx(unrealised_pnl_pct=0.20, days_held=30)
        assert policy.predict_full(ctx)["exit_type"].loc["ACME"] == "target_achieved"


class TestRegressionAgainstTheShippedDefect:
    """Pins the specific failure, so it cannot silently return."""

    def test_the_old_policy_could_not_time_exit(self):
        """Documents what was wrong. RuleBasedExitPolicy caps the max-hold band
        at 65 against a threshold of 80, so a position 100 days past a 25-day
        barrier is still held. Kept as executable evidence rather than prose."""
        from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy

        old = RuleBasedExitPolicy(target_pct=0.12, stop_pct=-0.05, max_hold_days=25)
        assert not _sells(old, _ctx(days_held=125)), (
            "RuleBasedExitPolicy unexpectedly time-exits — if this now passes, the urgency "
            "bands changed and the risk_managed variant may no longer be needed"
        )

    def test_risk_managed_fixes_exactly_that_case(self):
        new = RiskManagedExitPolicy(target_pct=0.12, stop_pct=-0.05, max_hold_days=25)
        assert _sells(new, _ctx(days_held=125))


class TestConstructorRejectsIncoherentParameters:
    """A silently-wrong sign here disables the stop entirely, which is how a
    'risk managed' run would end up with no risk management."""

    def test_positive_stop_pct_is_rejected(self):
        with pytest.raises(ValueError, match="stop_pct must be negative"):
            RiskManagedExitPolicy(stop_pct=0.05)

    def test_non_positive_target_is_rejected(self):
        with pytest.raises(ValueError, match="target_pct must be positive"):
            RiskManagedExitPolicy(target_pct=0.0)

    def test_non_positive_max_hold_is_rejected(self):
        with pytest.raises(ValueError, match="max_hold_days must be positive"):
            RiskManagedExitPolicy(max_hold_days=0)


class TestVariantIsWiredIntoTheEngine:
    def test_build_exit_model_for_variant_returns_a_working_policy(self):
        from backtest.core.engine import EXIT_POLICY_VARIANTS, build_exit_model_for_variant

        assert "risk_managed" in EXIT_POLICY_VARIANTS
        model = build_exit_model_for_variant("risk_managed")
        assert _sells(model, _ctx(days_held=200)), "engine-built risk_managed must time-exit"
