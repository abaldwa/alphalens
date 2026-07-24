"""
systems/ml_signal_engine/models/exit/atr_adaptive_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 2)
Owner: ml_signal_engine / exit
Consumers: backtest/run_orchestrator_backtest.py (--exit-variant atr_adaptive),
           tests/unit/test_atr_adaptive_exit_policy.py

PerTemplateExitPolicy applies a flat stop_pct/target_pct per template
regardless of how volatile that specific ticker currently is — a 5% stop
is tight for a high-ATR small-cap and loose for a low-ATR large-cap.
ATRAdaptiveExitPolicy instead scales BOTH stop and target off each
position's own `atr_pct` (ATR/entry_price at entry time — same column
RuleBasedExitPolicy already reads, see that module's docstring for the
sourcing chain: backtest/portfolio.Position.entry_atr_pct <-
features/technical.py's atr_14_pct), reusing/extending
RuleBasedExitPolicy's existing ATR-scaling code path rather than
reinventing it.

Multiplier convention (distinct constants from RuleBasedExitPolicy's own
ATR_PROFIT_MULTIPLIER=2.0/ATR_STOP_MULTIPLIER=1.0 bootstrap numbers, per
this policy's own explicit design — a wider profit:stop ratio, 2:1
scaled from a larger stop distance, tuned separately for this
experiment): stop = ATR_ADAPTIVE_STOP_MULTIPLIER (1.5) x atr_pct, target
= ATR_ADAPTIVE_PROFIT_MULTIPLIER (3.0) x atr_pct. Rows with no usable
atr_pct (NaN/<=0 — early in a ticker's history, or a caller that hasn't
populated entry_atr_pct) fall back to the constructor's flat target_pct/
stop_pct, exactly like RuleBasedExitPolicy's own fallback.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import (
    EXIT_TYPES,
    PND_EXIT_SCORE_THRESHOLD,
    PND_EXIT_URGENCY_FLOOR,
)
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import MAX_HOLD_DAYS, STOP_PCT, TARGET_PCT

# This policy's own ATR multipliers — deliberately different from
# RuleBasedExitPolicy's ATR_PROFIT_MULTIPLIER/ATR_STOP_MULTIPLIER (see
# module docstring): every position's stop/target is ATR-scaled, not just
# a fallback-when-available.
ATR_ADAPTIVE_STOP_MULTIPLIER = 1.5
ATR_ADAPTIVE_PROFIT_MULTIPLIER = 3.0


class ATRAdaptiveExitPolicy:
    """Stop/target scaled per-position by atr_pct
    (ATR_ADAPTIVE_STOP_MULTIPLIER / ATR_ADAPTIVE_PROFIT_MULTIPLIER x
    atr_pct) instead of a flat template stop_pct/target_pct. Same
    `predict_full(X) -> DataFrame[exit_urgency, exit_type,
    exit_survival_5d/21d/63d]` contract as RuleBasedExitPolicy."""

    def __init__(
        self,
        target_pct: float = TARGET_PCT,
        stop_pct: float = STOP_PCT,
        max_hold_days: int = MAX_HOLD_DAYS,
    ) -> None:
        """target_pct/stop_pct here are only the FALLBACK for rows with no
        usable atr_pct — see module docstring."""
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
        required = {"unrealised_pnl_pct", "days_held", "drawdown_from_peak"}
        missing = required - set(X.columns)
        if missing:
            raise ValueError(f"ATRAdaptiveExitPolicy.predict_full missing required columns: {missing}")

        pnl = X["unrealised_pnl_pct"]
        days_held = X["days_held"]
        drawdown = X["drawdown_from_peak"]

        if "atr_pct" in X.columns:
            atr_pct = X["atr_pct"]
            has_atr = atr_pct.notna() & (atr_pct > 0)
            target_pct = pd.Series(self.target_pct, index=X.index).mask(
                has_atr, ATR_ADAPTIVE_PROFIT_MULTIPLIER * atr_pct
            )
            stop_pct = pd.Series(self.stop_pct, index=X.index).mask(
                has_atr, -ATR_ADAPTIVE_STOP_MULTIPLIER * atr_pct
            )
        else:
            target_pct = pd.Series(self.target_pct, index=X.index)
            stop_pct = pd.Series(self.stop_pct, index=X.index)

        target_hit = pnl >= target_pct
        stop_hit = pnl <= stop_pct
        max_hold_hit = days_held >= self.max_hold_days
        momentum_exhausted = (~target_hit) & (~stop_hit) & (drawdown <= -0.10) & (pnl > 0)

        exit_type = pd.Series("opportunity_cost", index=X.index)
        exit_type = exit_type.mask(max_hold_hit & ~target_hit & ~stop_hit, "opportunity_cost")
        exit_type = exit_type.mask(momentum_exhausted, "momentum_exhaustion")
        exit_type = exit_type.mask(stop_hit, "thesis_broken")
        exit_type = exit_type.mask(target_hit, "target_achieved")

        urgency = pd.Series(45.0, index=X.index)
        urgency = urgency.mask(
            max_hold_hit & ~target_hit & ~stop_hit,
            np.clip(50.0 + days_held - self.max_hold_days, 50.0, 65.0),
        )
        urgency = urgency.mask(momentum_exhausted, np.clip(60.0 + (-drawdown) * 100.0, 60.0, 79.0))
        urgency = urgency.mask(stop_hit, np.clip(80.0 + (stop_pct - pnl).clip(lower=0) * 100.0, 80.0, 100.0))
        urgency = urgency.mask(target_hit, np.clip(70.0 + (pnl - target_pct).clip(lower=0) * 50.0, 70.0, 90.0))

        triggered = target_hit | stop_hit | max_hold_hit | momentum_exhausted
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
