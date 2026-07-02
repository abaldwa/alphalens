"""
systems/ml_signal_engine/models/exit/rule_based_exit_policy.py

Phase: 3.x (Paper Trading Logic Fix — Exit Signal bootstrap)
Specs: SPEC-MODEL-002 (barrier convention), SPEC-SOLID-003 (predict_full contract)
Owner: ml_signal_engine / exit
Consumers: scripts/run_paper_trading_sim.py (pass-1 bootstrap exit policy)

ExitSignalModel cannot train until MIN_CLOSED_POSITIONS (200) real closed
paper-trading positions exist, and real forward paper trading has
accumulated almost none (see BuildLog.md "Real data sourcing — Exit
Signal"). RuleBasedExitPolicy is a mechanical, non-ML stand-in that
implements the exact same `predict_full(X) -> DataFrame[exit_urgency,
exit_type, ...]` contract as ExitSignalModel — a drop-in for
PortfolioSimulator-driven simulation — so a historical paper-trading replay
can generate real, varied closed-trade outcomes (entry/exit prices and
dates are real OHLCV, never fabricated) to bootstrap the very first
ExitSignalModel training set. Once that model trains, swap this policy out
for the real one and rerun — see BuildLog.md "Paper Trading Logic Fix".

Mechanical rule: target/stop percentage barriers in the same 2:1
profit:stop ratio TripleBarrierLabeler uses (profit_multiplier=2.0,
stop_multiplier=1.0 ATR-scaled, systems/ml_signal_engine/training/labeling.py)
plus a max-holding fallback and the same PnD override ExitSignalModel
applies. Expressed as flat percentages (not per-row ATR) because the
EXIT_CONTEXT_COLUMNS panel built in backtest/engine.py/run_paper_trading_sim.py
carries unrealised_pnl_pct, not a per-position ATR — adequate for
manufacturing duration/exit_type variety, not a claim of being as precise
as a real ATR-scaled barrier.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import (
    EXIT_TYPES,
    PND_EXIT_SCORE_THRESHOLD,
    PND_EXIT_URGENCY_FLOOR,
)

# 2:1 profit:stop ratio, same convention as TripleBarrierLabeler's
# (profit_multiplier=2.0, stop_multiplier=1.0) — expressed here as flat
# return-pct barriers rather than ATR multiples (see module docstring).
TARGET_PCT = 0.15
STOP_PCT = -0.075
MAX_HOLD_DAYS = 21


class RuleBasedExitPolicy:
    """Mechanical target/stop/max-hold/PnD exit policy — see module docstring."""

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
        Same output contract as ExitSignalModel.predict_full: exit_urgency
        (0-100), exit_type (always one of EXIT_TYPES), exit_survival_5d/21d/63d.

        Parameters
        ----------
        X : pd.DataFrame
            Must contain unrealised_pnl_pct, days_held, drawdown_from_peak.
            pnd_score is optional (same PnD override as the real model when
            present).
        """
        required = {"unrealised_pnl_pct", "days_held", "drawdown_from_peak"}
        missing = required - set(X.columns)
        if missing:
            raise ValueError(f"RuleBasedExitPolicy.predict_full missing required columns: {missing}")

        pnl = X["unrealised_pnl_pct"]
        days_held = X["days_held"]
        drawdown = X["drawdown_from_peak"]

        target_hit = pnl >= self.target_pct
        stop_hit = pnl <= self.stop_pct
        max_hold_hit = days_held >= self.max_hold_days
        # Sharp pullback from peak after meaningful unrealised gain — momentum
        # giving back ground rather than an outright stop-loss breach.
        momentum_exhausted = (~target_hit) & (~stop_hit) & (drawdown <= -0.10) & (pnl > 0)

        exit_type = pd.Series("opportunity_cost", index=X.index)
        exit_type = exit_type.mask(max_hold_hit & ~target_hit & ~stop_hit, "opportunity_cost")
        exit_type = exit_type.mask(momentum_exhausted, "momentum_exhaustion")
        exit_type = exit_type.mask(stop_hit, "thesis_broken")
        exit_type = exit_type.mask(target_hit, "target_achieved")

        # Urgency: scaled within each exit_type's own band, never just a
        # constant per bucket, so duration/outcome variety survives into
        # the urgency target too.
        urgency = pd.Series(45.0, index=X.index)  # default: nothing has triggered yet
        urgency = urgency.mask(
            max_hold_hit & ~target_hit & ~stop_hit,
            np.clip(50.0 + days_held - self.max_hold_days, 50.0, 65.0),
        )
        urgency = urgency.mask(momentum_exhausted, np.clip(60.0 + (-drawdown) * 100.0, 60.0, 79.0))
        urgency = urgency.mask(stop_hit, np.clip(80.0 + (self.stop_pct - pnl).clip(lower=0) * 100.0, 80.0, 100.0))
        urgency = urgency.mask(target_hit, np.clip(70.0 + (pnl - self.target_pct).clip(lower=0) * 50.0, 70.0, 90.0))

        triggered = target_hit | stop_hit | max_hold_hit | momentum_exhausted
        urgency = urgency.where(triggered, 45.0)

        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = urgency.clip(0, 100)
        out["exit_type"] = exit_type.astype(str)
        # No survival-curve fit exists in a rule-based policy — honestly NaN
        # rather than fabricated, per CLAUDE.md Rule 6. Only used as
        # informational columns; PortfolioSimulator.apply_exit_signal only
        # consumes exit_urgency.
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
