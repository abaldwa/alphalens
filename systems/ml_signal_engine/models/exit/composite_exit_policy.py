"""
systems/ml_signal_engine/models/exit/composite_exit_policy.py

Phase: 3.x (Exit policy experimentation — Variant C)
Owner: ml_signal_engine / exit
Consumers: backtest/run_orchestrator_backtest.py (--exit-variant combined),
           tests/unit/test_composite_exit_policy.py

Wraps N policies that each implement predict_full(X) -> DataFrame[
exit_urgency, ...] (RuleBasedExitPolicy/PerTemplateExitPolicy/
ConditionBasedExitPolicy/any other exit policy in this package), and
combines them with pure OR-of-triggers semantics: exit_action_for_urgency
(backtest/portfolio.py) is a binary threshold check on exit_urgency alone
(> EXIT_URGENT_THRESHOLD -> immediate_exit), so "if ANY wrapped policy
says exit now, exit now" is exactly the row-wise MAX of every wrapped
policy's exit_urgency — no new fusion logic needed beyond that max.

exit_type/exit_survival_* are taken from whichever wrapped policy
produced the winning (max) urgency for that row, so the reported reason
matches the actual trigger rather than an arbitrary first/last policy.
"""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from backtest.portfolio import (
    EXIT_REDUCE_THRESHOLD,
    EXIT_URGENT_THRESHOLD,
    MONITOR_THRESHOLD,
)
from systems.ml_signal_engine.models.exit.exit_intent import (
    EXIT_ACTION_EXIT,
    EXIT_ACTION_HOLD,
    EXIT_ACTION_MONITOR,
    EXIT_ACTION_REDUCE,
    action_from_urgency,
    validate_actions,
)


class CompositeExitPolicy:
    """OR-combines multiple exit policies: exit_urgency is the row-wise
    MAX across all wrapped policies' predict_full() output; exit_type/
    exit_survival_* are taken from whichever policy produced that max for
    each row. Same predict_full(X) -> DataFrame[exit_urgency, exit_type,
    exit_survival_5d/21d/63d] contract as every other policy in this
    package — a CompositeExitPolicy can itself be wrapped in another
    CompositeExitPolicy."""

    def __init__(self, policies: List[object]) -> None:
        if not policies:
            raise ValueError("CompositeExitPolicy requires at least one wrapped policy")
        self.policies = list(policies)

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        results = [policy.predict_full(X) for policy in self.policies]

        urgencies = pd.concat(
            [r["exit_urgency"].rename(i) for i, r in enumerate(results)], axis=1,
        )
        # Index of the winning (max-urgency) policy per row — ties keep the
        # first policy in `self.policies` order (pandas idxmax's default),
        # a stable, deterministic tie-break.
        winner_idx = urgencies.idxmax(axis=1)
        max_urgency = urgencies.max(axis=1)

        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = max_urgency.clip(0, 100)

        for col in ("exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d"):
            values = pd.Series(np.nan, index=X.index, dtype=object) if col == "exit_type" else pd.Series(
                np.nan, index=X.index, dtype=float,
            )
            for i, r in enumerate(results):
                mask = winner_idx == i
                if mask.any():
                    values.loc[mask] = r.loc[mask, col].values
            out[col] = values

        out["exit_type"] = out["exit_type"].astype(str)

        # [STEP 4, 2026-08-13] Intent is the SEVERITY UNION across children,
        # deliberately not the winning-by-urgency policy's action.
        #
        # This variant's contract is "whichever fires first" — if the barrier
        # half says exit and the thesis half says hold, the position exits.
        # Taking the max-urgency row's action instead would make the decision
        # depend on the children's urgency SCALES agreeing, and they do not:
        # RuleBasedExitPolicy emits a max-hold exit at 59 while a
        # non-triggering row sits at 45, so a genuine exit can be only 14
        # points clear of nothing-happening. Comparing intents avoids relying
        # on numbers that were never calibrated against each other.
        severity = {EXIT_ACTION_HOLD: 0, EXIT_ACTION_MONITOR: 1, EXIT_ACTION_REDUCE: 2, EXIT_ACTION_EXIT: 3}
        # A child still on the legacy contract has its action derived from its
        # own urgency rather than crashing the composite. Composite must stay
        # usable with any policy meeting the OLD contract — including the
        # minimal test doubles that pin its winner-selection behaviour — while
        # every real policy in this package now states intent directly.
        child_actions = []
        for i, r in enumerate(results):
            if "exit_action" in r.columns:
                child_actions.append(r["exit_action"].map(severity).rename(i))
            else:
                child_actions.append(
                    action_from_urgency(
                        r["exit_urgency"],
                        urgent_threshold=EXIT_URGENT_THRESHOLD,
                        reduce_threshold=EXIT_REDUCE_THRESHOLD,
                        monitor_threshold=MONITOR_THRESHOLD,
                    ).map(severity).rename(i)
                )
        ranked = pd.concat(child_actions, axis=1)
        inverse = {v: k for k, v in severity.items()}
        out["exit_action"] = ranked.max(axis=1).map(inverse)

        validate_actions(out["exit_action"].unique())
        return out[[
            "exit_action", "exit_urgency", "exit_type",
            "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
        ]]
