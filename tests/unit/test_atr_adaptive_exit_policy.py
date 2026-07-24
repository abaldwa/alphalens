"""
tests/unit/test_atr_adaptive_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 2)
Owner: Platform / QA
Consumers: CI, pytest
"""

import pandas as pd
import pytest

from systems.ml_signal_engine.models.exit.atr_adaptive_exit_policy import (
    ATR_ADAPTIVE_PROFIT_MULTIPLIER,
    ATR_ADAPTIVE_STOP_MULTIPLIER,
    ATRAdaptiveExitPolicy,
)
from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES


def _row(pnl_pct=0.0, days_held=5.0, drawdown=0.0, atr_pct=None):
    row = {"entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown}
    if atr_pct is not None:
        row["atr_pct"] = atr_pct
    return pd.DataFrame([row], index=["TICK"])


class TestATRAdaptiveExitPolicy:
    def test_multipliers_are_1_5x_stop_3x_target(self):
        assert ATR_ADAPTIVE_STOP_MULTIPLIER == 1.5
        assert ATR_ADAPTIVE_PROFIT_MULTIPLIER == 3.0

    def test_high_atr_ticker_gets_a_wider_stop_than_low_atr_ticker(self):
        policy = ATRAdaptiveExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        # 4% adverse move: NOT enough to breach a high-ATR (atr_pct=0.05 -> 7.5% stop)
        # ticker's stop, but plenty to breach a low-ATR (atr_pct=0.01 -> 1.5% stop) ticker's.
        high_atr_out = policy.predict_full(_row(pnl_pct=-0.04, atr_pct=0.05))
        low_atr_out = policy.predict_full(_row(pnl_pct=-0.04, atr_pct=0.01))
        assert high_atr_out["exit_type"].iloc[0] != "thesis_broken"
        assert low_atr_out["exit_type"].iloc[0] == "thesis_broken"

    def test_target_scaled_by_atr(self):
        # atr_pct=0.03 -> target = 3.0 * 0.03 = 9%
        policy = ATRAdaptiveExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.10, atr_pct=0.03))
        assert out["exit_type"].iloc[0] == "target_achieved"

    def test_no_atr_falls_back_to_flat_pct(self):
        policy = ATRAdaptiveExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=-0.04))  # no atr_pct column at all
        assert out["exit_type"].iloc[0] != "thesis_broken"  # -4% is above the flat -7.5% stop

    def test_max_hold_and_construction_validation(self):
        policy = ATRAdaptiveExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.01, days_held=25.0))
        assert out["exit_type"].iloc[0] == "opportunity_cost"

        with pytest.raises(ValueError):
            ATRAdaptiveExitPolicy(target_pct=0.0)

    def test_exit_type_always_valid(self):
        policy = ATRAdaptiveExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.0, atr_pct=0.02))
        assert out["exit_type"].isin(EXIT_TYPES).all()
