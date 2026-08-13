"""
systems/ml_signal_engine/models/exit/risk_managed_exit_policy.py

An exit policy whose triggers can actually close a position.

WHY THIS EXISTS
---------------
The exit interface is a two-step: a policy emits an `exit_urgency` score, and
PortfolioSimulator.exit_action_for_urgency maps it to an action, requiring
urgency STRICTLY ABOVE EXIT_URGENT_THRESHOLD (80) to sell. RuleBasedExitPolicy
emits bands that mostly cannot reach it:

    stop hit             clip(80 + (stop - pnl)*100,  80, 100)   fires
    target hit           clip(70 + (pnl - target)*50, 70,  90)   needs ~+20pp overshoot
    max hold days hit    clip(50 + days - max_hold,   50,  65)   CANNOT fire
    momentum exhausted   clip(60 + (-drawdown)*100,   60,  79)   CANNOT fire

Measured over the 2009-2026 sweep (65 baseline runs, 108,762 model-driven
exits): 90.94% were stops, 8.06% were targets that had overshot by ~20 points,
and 0.00% were time exits. Two of the four documented triggers had never fired
once, and the 60-80 band maps to "reduce_position", which StrategyPortfolio
cannot perform and silently treats as hold.

So the per-template stop/target/max-hold exits the strategy specification asked
for were never actually tested. This policy is the corrected implementation:
every trigger emits a score above the action threshold, and the numbers are
ordered by severity so urgency still ranks sensibly for reporting.

DESIGN NOTE — why not just re-tune RuleBasedExitPolicy
-----------------------------------------------------
Because that class is a trained-model surrogate whose urgency bands are also
consumed as an ML target elsewhere; widening them to reach 80 would change the
meaning of a value other code learns from. A separate policy keeps the fix
local to backtesting, which is where the requirement lives. The longer-term fix
is for policies to return an intent rather than a score to be re-thresholded
downstream (see the redesign plan); this class is deliberately compatible with
today's interface so it can ship without that refactor.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_intent import (
    EXIT_ACTION_EXIT,
    EXIT_ACTION_HOLD,
    validate_actions,
)

# Ordered by severity, all strictly above EXIT_URGENT_THRESHOLD (80) so each
# genuinely closes a position. The gaps are what keep urgency meaningful for
# ranking and reporting rather than collapsing to a single "exit now" value.
URGENCY_STOP = 96.0
URGENCY_TARGET = 90.0
URGENCY_MAX_HOLD = 86.0
URGENCY_MOMENTUM_EXHAUSTED = 82.0
URGENCY_HOLD = 40.0

DEFAULT_STOP_PCT = -0.05
DEFAULT_TARGET_PCT = 0.12
DEFAULT_MAX_HOLD_DAYS = 25
MOMENTUM_EXHAUSTION_DRAWDOWN = -0.10


class RiskManagedExitPolicy:
    """Stop-loss, profit target and holding-period barrier that all fire.

    stop_pct is negative (-0.05 == a 5% stop), matching RuleBasedExitPolicy's
    convention; PerTemplateExitPolicy passes -abs(...) for exactly this reason.
    """

    def __init__(
        self,
        target_pct: float = DEFAULT_TARGET_PCT,
        stop_pct: float = DEFAULT_STOP_PCT,
        max_hold_days: int = DEFAULT_MAX_HOLD_DAYS,
        momentum_exhaustion_drawdown: Optional[float] = MOMENTUM_EXHAUSTION_DRAWDOWN,
    ):
        if max_hold_days <= 0:
            raise ValueError("max_hold_days must be positive")
        if stop_pct >= 0:
            raise ValueError(
                f"stop_pct must be negative (a 5% stop is -0.05); got {stop_pct}. "
                "PerTemplateExitPolicy passes -abs(stop_pct) for this reason."
            )
        if target_pct <= 0:
            raise ValueError(f"target_pct must be positive; got {target_pct}")
        self.target_pct = target_pct
        self.stop_pct = stop_pct
        self.max_hold_days = int(max_hold_days)
        self.momentum_exhaustion_drawdown = momentum_exhaustion_drawdown

    # The exit-model interface used by backtest/core/engine.py.
    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        pnl = pd.to_numeric(X.get("unrealised_pnl_pct", pd.Series(0.0, index=X.index)), errors="coerce").fillna(0.0)
        days_held = pd.to_numeric(X.get("days_held", pd.Series(0, index=X.index)), errors="coerce").fillna(0)
        drawdown = pd.to_numeric(
            X.get("drawdown_from_peak", pd.Series(0.0, index=X.index)), errors="coerce"
        ).fillna(0.0)

        stop_hit = pnl <= self.stop_pct
        target_hit = pnl >= self.target_pct
        max_hold_hit = days_held >= self.max_hold_days
        if self.momentum_exhaustion_drawdown is None:
            momentum_exhausted = pd.Series(False, index=X.index)
        else:
            momentum_exhausted = (
                (~target_hit) & (~stop_hit)
                & (drawdown <= self.momentum_exhaustion_drawdown) & (pnl > 0)
            )

        # Applied least- to most-severe so the strongest reason wins where
        # several are true at once (a position can breach its stop on the same
        # day its holding period expires).
        urgency = pd.Series(URGENCY_HOLD, index=X.index)
        exit_type = pd.Series("hold", index=X.index)

        urgency = urgency.mask(momentum_exhausted, URGENCY_MOMENTUM_EXHAUSTED)
        exit_type = exit_type.mask(momentum_exhausted, "momentum_exhaustion")

        urgency = urgency.mask(max_hold_hit, URGENCY_MAX_HOLD)
        exit_type = exit_type.mask(max_hold_hit, "max_hold_days")

        urgency = urgency.mask(target_hit, URGENCY_TARGET)
        exit_type = exit_type.mask(target_hit, "target_achieved")

        urgency = urgency.mask(stop_hit, URGENCY_STOP)
        exit_type = exit_type.mask(stop_hit, "thesis_broken")

        # [STEP 4, 2026-08-13] Intent stated directly. Each of these four
        # conditions IS a decision to exit; encoding them as urgency and
        # letting the consumer re-threshold is what made three of them
        # unreachable in the policy this class was written to replace.
        action = pd.Series(EXIT_ACTION_HOLD, index=X.index, dtype=object)
        action = action.mask(momentum_exhausted | max_hold_hit | target_hit | stop_hit,
                             EXIT_ACTION_EXIT)

        out = pd.DataFrame(index=X.index)
        out["exit_action"] = action
        out["exit_urgency"] = urgency
        out["exit_type"] = exit_type.astype(str)
        # No survival model exists in a rule-based policy — NaN rather than a
        # fabricated curve (CLAUDE.md Rule 6). Informational only; the engine
        # consumes exit_urgency alone.
        out["exit_survival_5d"] = np.nan
        out["exit_survival_21d"] = np.nan
        out["exit_survival_63d"] = np.nan
        validate_actions(out["exit_action"].unique())
        return out

    def predict(self, X: pd.DataFrame) -> pd.Series:
        return self.predict_full(X)["exit_urgency"]

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"RiskManagedExitPolicy(target_pct={self.target_pct}, stop_pct={self.stop_pct}, "
            f"max_hold_days={self.max_hold_days})"
        )
