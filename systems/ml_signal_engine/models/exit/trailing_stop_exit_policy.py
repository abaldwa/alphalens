"""
systems/ml_signal_engine/models/exit/trailing_stop_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 1)
Owner: ml_signal_engine / exit
Consumers: backtest/run_orchestrator_backtest.py (--exit-variant trailing),
           tests/unit/test_trailing_stop_exit_policy.py

RuleBasedExitPolicy's stop is a fixed distance BELOW entry_price — a
position that ran up 30% and pulled back 8% never stops out even though
it gave back a large chunk of its gain, since 8% off the peak is still
comfortably above entry_price * (1 + stop_pct). TrailingStopExitPolicy
instead stops a position out when price falls `stop_pct` BELOW its own
peak_price (highest price seen since entry) — the drawdown-from-peak
already tracked on backtest/portfolio.Position.peak_price and passed
into exit_ctx by BacktestOrchestrator._apply_exit_policy() as
`drawdown_from_peak = (price - peak_price) / peak_price`, so no new
exit_ctx column is needed: the trailing-stop check is simply
`drawdown_from_peak <= -stop_pct` instead of RuleBasedExitPolicy's
`unrealised_pnl_pct <= stop_pct` (which is anchored to entry_price, not
peak_price).

Target/take-profit logic is UNCHANGED from RuleBasedExitPolicy — still a
flat (or ATR-scaled, when atr_pct is present) `unrealised_pnl_pct >=
target_pct` off entry_price, per the build prompt ("Target/take-profit
logic can remain the same as the template's exit_target_pct"). Max-hold
and PnD-override behavior are also unchanged.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import (
    EXIT_TYPES,
    PND_EXIT_SCORE_THRESHOLD,
    PND_EXIT_URGENCY_FLOOR,
)
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import (
    ATR_PROFIT_MULTIPLIER,
    ATR_STOP_MULTIPLIER,
    MAX_HOLD_DAYS,
    STOP_PCT,
    TARGET_PCT,
)


class TrailingStopExitPolicy:
    """Target off entry_price (unchanged from RuleBasedExitPolicy), stop
    off peak_price (trailing) instead of entry_price. Same
    `predict_full(X) -> DataFrame[exit_urgency, exit_type,
    exit_survival_5d/21d/63d]` contract."""

    def __init__(
        self,
        target_pct: float = TARGET_PCT,
        stop_pct: float = STOP_PCT,
        max_hold_days: int = MAX_HOLD_DAYS,
    ) -> None:
        if target_pct <= 0:
            raise ValueError("target_pct must be positive")
        if stop_pct >= 0:
            raise ValueError("stop_pct must be negative")
        if max_hold_days <= 0:
            raise ValueError("max_hold_days must be positive")
        self.target_pct = target_pct
        self.stop_pct = stop_pct
        self.max_hold_days = max_hold_days

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Requires unrealised_pnl_pct, days_held, drawdown_from_peak (the
        peak-relative drawdown BacktestOrchestrator._apply_exit_policy()
        already computes from Position.peak_price). atr_pct is optional
        (same ATR-scaling convention as RuleBasedExitPolicy, applied to
        both target and the trailing-stop distance).
        """
        required = {"unrealised_pnl_pct", "days_held", "drawdown_from_peak"}
        missing = required - set(X.columns)
        if missing:
            raise ValueError(f"TrailingStopExitPolicy.predict_full missing required columns: {missing}")

        pnl = X["unrealised_pnl_pct"]
        days_held = X["days_held"]
        drawdown = X["drawdown_from_peak"]

        if "atr_pct" in X.columns:
            atr_pct = X["atr_pct"]
            has_atr = atr_pct.notna() & (atr_pct > 0)
            target_pct = pd.Series(self.target_pct, index=X.index).mask(has_atr, ATR_PROFIT_MULTIPLIER * atr_pct)
            stop_pct = pd.Series(self.stop_pct, index=X.index).mask(has_atr, -ATR_STOP_MULTIPLIER * atr_pct)
        else:
            target_pct = pd.Series(self.target_pct, index=X.index)
            stop_pct = pd.Series(self.stop_pct, index=X.index)

        target_hit = pnl >= target_pct
        # Trailing stop: breached when the pullback FROM THE PEAK (not from
        # entry) exceeds stop_pct — drawdown_from_peak is always <= 0.
        trailing_stop_hit = drawdown <= stop_pct
        max_hold_hit = days_held >= self.max_hold_days

        exit_type = pd.Series("opportunity_cost", index=X.index)
        exit_type = exit_type.mask(max_hold_hit & ~target_hit & ~trailing_stop_hit, "opportunity_cost")
        exit_type = exit_type.mask(trailing_stop_hit, "thesis_broken")
        exit_type = exit_type.mask(target_hit & ~trailing_stop_hit, "target_achieved")

        urgency = pd.Series(45.0, index=X.index)
        urgency = urgency.mask(
            max_hold_hit & ~target_hit & ~trailing_stop_hit,
            np.clip(50.0 + days_held - self.max_hold_days, 50.0, 65.0),
        )
        urgency = urgency.mask(
            trailing_stop_hit,
            np.clip(80.0 + (stop_pct - drawdown).clip(lower=0) * 100.0, 80.0, 100.0),
        )
        urgency = urgency.mask(
            target_hit & ~trailing_stop_hit,
            np.clip(70.0 + (pnl - target_pct).clip(lower=0) * 50.0, 70.0, 90.0),
        )

        triggered = target_hit | trailing_stop_hit | max_hold_hit
        urgency = urgency.where(triggered, 45.0)

        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = urgency.clip(0, 100)
        out["exit_type"] = exit_type.astype(str)
        out["exit_survival_5d"] = np.nan
        out["exit_survival_21d"] = np.nan
        out["exit_survival_63d"] = np.nan

        if "pnd_score" in X.columns:
            pnd_triggered = X["pnd_score"] > PND_EXIT_SCORE_THRESHOLD
            out.loc[pnd_triggered, "exit_type"] = "pnd_exit"
            out.loc[pnd_triggered, "exit_urgency"] = np.maximum(
                out.loc[pnd_triggered, "exit_urgency"].to_numpy(), PND_EXIT_URGENCY_FLOOR
            )

        assert out["exit_type"].isin(EXIT_TYPES).all() and out["exit_type"].notna().all(), (
            "exit_type must always be a valid, non-null EXIT_TYPES category"
        )

        return out[["exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d"]]
