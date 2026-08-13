"""
systems/ml_signal_engine/models/exit/regime_conditional_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 3)
Owner: ml_signal_engine / exit
Consumers: backtest/run_orchestrator_backtest.py (--exit-variant
           regime_conditional), tests/unit/test_regime_conditional_exit_policy.py

PerTemplateExitPolicy applies the same stop_pct in a raging Bull market
as in a confirmed Bear — a 5% stop that's reasonable chop in a Bull tape
is a much easier trigger to hit in Bear-market volatility, and a Bear
tape arguably deserves a TIGHTER stop precisely because drawdowns compound
faster. RegimeConditionalExitPolicy scales each template's own stop_pct
by the day's market regime, reusing the EXISTING Bull/Bear/Sideways
segment classification (systems/regime/market_regime.py::classify_regimes
+ systems/regime/regime_store.py, already backing
BacktestOrchestrator's regime_breakdown / regime_conn/regime_index_name
constructor params) rather than building a new classifier.

Multiplier convention (confirmed with the user): Bear -> 0.7x stop_pct
(tighter), Bull -> 1.3x stop_pct (looser), Sideways -> 1.0x (unchanged).
target_pct and max_hold_days are NOT regime-scaled — only the stop is,
per the build prompt ("tighter stops in Bear regime, looser in Bull").

exit_ctx must carry a `regime` column (values "bull"/"bear"/"sideways",
case-insensitive — systems.regime.market_regime.Regime's own lowercase
convention) alongside the usual template/pillar columns. Rows with no
regime value (None/NaN/unrecognized) get the Sideways (1.0x, unchanged)
multiplier — a neutral, non-surprising default rather than silently
picking a directional bias.
"""

from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_intent import (
    EXIT_ACTION_EXIT,
    EXIT_ACTION_HOLD,
    validate_actions,
)

from systems.ml_signal_engine.models.exit.exit_signal import (
    EXIT_TYPES,
    PND_EXIT_SCORE_THRESHOLD,
    PND_EXIT_URGENCY_FLOOR,
)
from systems.ml_signal_engine.models.exit.per_template_exit_policy import build_default_template_params
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import MAX_HOLD_DAYS, STOP_PCT, TARGET_PCT

# Confirmed regime -> stop_pct multiplier convention (see module docstring).
REGIME_STOP_MULTIPLIERS: Dict[str, float] = {
    "bear": 0.7,
    "bull": 1.3,
    "sideways": 1.0,
}
_DEFAULT_REGIME_MULTIPLIER = 1.0  # unrecognized/missing regime -> unchanged, same as Sideways


class RegimeConditionalExitPolicy:
    """Per-template stop_pct/target_pct/max_hold_days (same source as
    PerTemplateExitPolicy — build_default_template_params()), with
    stop_pct additionally scaled by the row's own `regime` column
    (REGIME_STOP_MULTIPLIERS). Same `predict_full(X) -> DataFrame[
    exit_urgency, exit_type, exit_survival_5d/21d/63d]` contract."""

    def __init__(
        self,
        template_params: Optional[Dict[str, Dict[str, float]]] = None,
        default_target_pct: float = TARGET_PCT,
        default_stop_pct: float = STOP_PCT,
        default_max_hold_days: int = MAX_HOLD_DAYS,
    ) -> None:
        """
        Parameters
        ----------
        template_params : dict, optional
            template/preset name -> {"stop_pct": float (positive fraction),
            "target_pct": float (positive fraction), "max_hold_days": int}.
            Defaults to build_default_template_params() (same source
            PerTemplateExitPolicy uses). Pass an explicit dict in tests to
            avoid depending on the live template catalog.
        default_target_pct/default_stop_pct/default_max_hold_days :
            Fallback for rows whose `template` doesn't match
            template_params (positive fraction / positive fraction /
            positive int, RuleBasedExitPolicy's own convention — negated
            internally where needed).
        """
        self.template_params = template_params if template_params is not None else build_default_template_params()
        self.default_target_pct = default_target_pct
        self.default_stop_pct = abs(default_stop_pct)
        self.default_max_hold_days = default_max_hold_days

    def _base_params_for(self, template: Optional[str]) -> Dict[str, float]:
        if template is not None and template in self.template_params:
            p = self.template_params[template]
            return {
                "target_pct": float(p["target_pct"]),
                "stop_pct": abs(float(p["stop_pct"])),
                "max_hold_days": int(p["max_hold_days"]),
            }
        return {
            "target_pct": self.default_target_pct,
            "stop_pct": self.default_stop_pct,
            "max_hold_days": self.default_max_hold_days,
        }

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        required = {"unrealised_pnl_pct", "days_held", "drawdown_from_peak"}
        missing = required - set(X.columns)
        if missing:
            raise ValueError(f"RegimeConditionalExitPolicy.predict_full missing required columns: {missing}")

        templates = X["template"] if "template" in X.columns else pd.Series(None, index=X.index)
        regimes = X["regime"] if "regime" in X.columns else pd.Series(None, index=X.index)

        # Vectorized replacement for a former `for idx in X.index: ...
        # .loc[idx] = ...` row-by-row loop (2026-07-25, reviewed by
        # ml-rigor-reviewer + backtest-reviewer before landing — see
        # BuildLog.md/FeatureBacklog.md): that loop was O(n) label-based
        # `.loc` scalar writes per row across 3 separate Series, the only
        # non-vectorized exit-policy class in this directory, and the
        # confirmed root cause of regime_conditional backtest jobs using
        # 6-10x more memory/CPU than every sibling variant on a full
        # multi-year/multi-hundred-ticker run. Both reviewers confirmed the
        # loop was correctness-safe (this refactor changes performance
        # only, not output) — see tests/unit/test_regime_conditional_exit_policy.py's
        # TestPredictFullVectorizedEquivalence for the byte-for-byte
        # equivalence regression test they required before merging.
        #
        # Template lookup: Series.map(<Series indexed by template name>)
        # returns NaN both for an unmatched string AND for a NaN/None
        # template value (map() looks the raw cell up as a dict/Series key
        # either way) — .fillna(default) then reproduces _base_params_for's
        # exact "template is not None and template in self.template_params
        # else default" fallback in one vectorized pass, no separate NaN
        # branch needed.
        if self.template_params:
            params_df = pd.DataFrame.from_dict(self.template_params, orient="index")
            target_map = params_df["target_pct"].astype(float)
            stop_map = params_df["stop_pct"].astype(float).abs()
            hold_map = params_df["max_hold_days"].astype(int)
            target_pct = templates.map(target_map).fillna(self.default_target_pct).astype(float)
            base_stop_pct = templates.map(stop_map).fillna(self.default_stop_pct).astype(float)
            max_hold_days = templates.map(hold_map).fillna(self.default_max_hold_days).astype(int)
        else:
            target_pct = pd.Series(self.default_target_pct, index=X.index, dtype=float)
            base_stop_pct = pd.Series(self.default_stop_pct, index=X.index, dtype=float)
            max_hold_days = pd.Series(self.default_max_hold_days, index=X.index, dtype=int)

        # Regime normalization: same "strip + lowercase, NaN -> None"
        # convention as the original loop, applied to the whole column via
        # pandas' vectorized .str accessor (not a per-row Python call) —
        # only the non-null subset is touched, matching pd.notna(regime_raw)
        # else None row-by-row.
        regime_key = pd.Series(None, index=X.index, dtype=object)
        regime_notna = regimes.notna()
        if regime_notna.any():
            regime_key.loc[regime_notna] = regimes.loc[regime_notna].astype(str).str.strip().str.lower()
        multiplier = regime_key.map(REGIME_STOP_MULTIPLIERS).fillna(_DEFAULT_REGIME_MULTIPLIER)

        stop_pct = -(base_stop_pct * multiplier)  # negative fraction, RuleBasedExitPolicy convention

        pnl = X["unrealised_pnl_pct"]
        days_held = X["days_held"]
        drawdown = X["drawdown_from_peak"]

        target_hit = pnl >= target_pct
        stop_hit = pnl <= stop_pct
        max_hold_hit = days_held >= max_hold_days
        momentum_exhausted = (~target_hit) & (~stop_hit) & (drawdown <= -0.10) & (pnl > 0)

        exit_type = pd.Series("opportunity_cost", index=X.index)
        exit_type = exit_type.mask(max_hold_hit & ~target_hit & ~stop_hit, "opportunity_cost")
        exit_type = exit_type.mask(momentum_exhausted, "momentum_exhaustion")
        exit_type = exit_type.mask(stop_hit, "thesis_broken")
        exit_type = exit_type.mask(target_hit, "target_achieved")

        urgency = pd.Series(45.0, index=X.index)
        urgency = urgency.mask(
            max_hold_hit & ~target_hit & ~stop_hit,
            np.clip(50.0 + days_held - max_hold_days, 50.0, 65.0),
        )
        urgency = urgency.mask(momentum_exhausted, np.clip(60.0 + (-drawdown) * 100.0, 60.0, 79.0))
        urgency = urgency.mask(stop_hit, np.clip(80.0 + (stop_pct - pnl).clip(lower=0) * 100.0, 80.0, 100.0))
        urgency = urgency.mask(target_hit, np.clip(70.0 + (pnl - target_pct).clip(lower=0) * 50.0, 70.0, 90.0))

        triggered = target_hit | stop_hit | max_hold_hit | momentum_exhausted
        urgency = urgency.where(triggered, 45.0)

        out = pd.DataFrame(index=X.index)
        # [STEP 4, 2026-08-13] Intent from the trigger boolean already computed
        # above, not re-derived downstream from an urgency band.
        out["exit_action"] = pd.Series(EXIT_ACTION_HOLD, index=X.index, dtype=object).mask(
            triggered, EXIT_ACTION_EXIT
        )
        out["exit_urgency"] = urgency.clip(0, 100)
        out["exit_type"] = exit_type.astype(str)
        out["exit_survival_5d"] = np.nan
        out["exit_survival_21d"] = np.nan
        out["exit_survival_63d"] = np.nan

        if "pnd_score" in X.columns:
            pnd_triggered = X["pnd_score"] > PND_EXIT_SCORE_THRESHOLD
            out.loc[pnd_triggered, "exit_type"] = "pnd_exit"
            out.loc[pnd_triggered, "exit_action"] = EXIT_ACTION_EXIT
            out.loc[pnd_triggered, "exit_urgency"] = np.maximum(
                out.loc[pnd_triggered, "exit_urgency"].to_numpy(), PND_EXIT_URGENCY_FLOOR
            )

        assert out["exit_type"].isin(EXIT_TYPES).all() and out["exit_type"].notna().all(), (
            "exit_type must always be a valid, non-null EXIT_TYPES category"
        )

        validate_actions(out["exit_action"].unique())
        return out[[
            "exit_action", "exit_urgency", "exit_type",
            "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
        ]]
