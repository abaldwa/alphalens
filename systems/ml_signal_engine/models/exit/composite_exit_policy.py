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
        return out[["exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d"]]
