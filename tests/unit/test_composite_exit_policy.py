"""
tests/unit/test_composite_exit_policy.py

Phase: 3.x (Exit policy experimentation — Variant C)
Owner: Platform / QA
Consumers: CI, pytest
"""

import pandas as pd
import pytest

from systems.ml_signal_engine.models.exit.composite_exit_policy import CompositeExitPolicy


class _FixedPolicy:
    """Stub policy returning a fixed urgency/exit_type for every row —
    isolates CompositeExitPolicy's OR/max logic from any real policy's
    internals."""

    def __init__(self, urgency: float, exit_type: str = "opportunity_cost"):
        self.urgency = urgency
        self.exit_type = exit_type

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = self.urgency
        out["exit_type"] = self.exit_type
        out["exit_survival_5d"] = float("nan")
        out["exit_survival_21d"] = float("nan")
        out["exit_survival_63d"] = float("nan")
        return out


def _x(n=1):
    return pd.DataFrame([{"a": 1.0}] * n, index=[f"T{i}" for i in range(n)])


class TestCompositeExitPolicy:
    def test_requires_at_least_one_policy(self):
        with pytest.raises(ValueError):
            CompositeExitPolicy([])

    def test_takes_max_urgency_across_wrapped_policies(self):
        composite = CompositeExitPolicy([_FixedPolicy(30.0), _FixedPolicy(90.0), _FixedPolicy(50.0)])
        out = composite.predict_full(_x())
        assert out["exit_urgency"].iloc[0] == 90.0

    def test_no_policy_triggers_stays_below_urgent_threshold(self):
        composite = CompositeExitPolicy([_FixedPolicy(10.0), _FixedPolicy(20.0)])
        out = composite.predict_full(_x())
        assert out["exit_urgency"].iloc[0] <= 40.0

    def test_exit_type_matches_winning_policy(self):
        composite = CompositeExitPolicy([
            _FixedPolicy(30.0, exit_type="opportunity_cost"),
            _FixedPolicy(95.0, exit_type="thesis_broken"),
        ])
        out = composite.predict_full(_x())
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_row_wise_independent_winners(self):
        # Two rows: policy A wins row 0 (higher urgency there), policy B wins row 1.
        class _RowAware:
            def __init__(self, urgencies, exit_type):
                self.urgencies = urgencies
                self.exit_type = exit_type

            def predict_full(self, X):
                out = pd.DataFrame(index=X.index)
                out["exit_urgency"] = self.urgencies
                out["exit_type"] = self.exit_type
                out["exit_survival_5d"] = float("nan")
                out["exit_survival_21d"] = float("nan")
                out["exit_survival_63d"] = float("nan")
                return out

        x = _x(n=2)
        policy_a = _RowAware([90.0, 10.0], "thesis_broken")
        policy_b = _RowAware([20.0, 95.0], "target_achieved")
        composite = CompositeExitPolicy([policy_a, policy_b])
        out = composite.predict_full(x)
        assert out["exit_urgency"].tolist() == [90.0, 95.0]
        assert out["exit_type"].tolist() == ["thesis_broken", "target_achieved"]

    def test_can_wrap_another_composite(self):
        inner = CompositeExitPolicy([_FixedPolicy(50.0), _FixedPolicy(60.0)])
        outer = CompositeExitPolicy([inner, _FixedPolicy(95.0)])
        out = outer.predict_full(_x())
        assert out["exit_urgency"].iloc[0] == 95.0
