"""
systems/ml_signal_engine/models/exit/condition_based_exit_policy.py

Phase: 3.x (Exit policy experimentation — Variant B)
Owner: ml_signal_engine / exit
Consumers: backtest/run_orchestrator_backtest.py (--exit-variant condition),
           tests/unit/test_condition_based_exit_policy.py

RuleBasedExitPolicy/PerTemplateExitPolicy exit purely on P&L barriers
(target/stop/max-hold) with no regard for whether the ORIGINAL entry
thesis (the screener template's own conditions) still holds.
ConditionBasedExitPolicy derives an exit trigger directly from each
Technical template's `conditions` (systems/technical_analysis/screener/
templates.py::ScreenerTemplate.conditions) — "exit when the setup that
got us in has broken down," independent of unrealised P&L.

Mapping from an entry condition to its derived exit rule (confirmed with
the user, 2026-07-24 build session — see build prompt for the full
rationale table):

  Already-boundary features (simple flip, no relaxation) — the entry
  condition's own threshold is a natural 0/1 boundary, so the exit rule
  just flips the inequality back through that same boundary, ignoring
  whatever exact threshold the template used:
    sma_200_ratio / sma_50_ratio / sma_20_ratio  gt  -> exit < 1.0
    macd_hist                                    gt  -> exit < 0.0
    ema_ribbon_alignment                         gt  -> exit < 0.0
    ema_8_ratio                                  gt  -> exit < 1.0
    supertrend_dir                               gt  -> exit < 0.0
    ichimoku_cloud_position                      gt  -> exit < 0.0

  Directional oscillators (relaxed reversal — a small buffer past the
  entry threshold, not an instant flip, since these features are noisy
  day-to-day):
    adx_14                    entry gt X   -> exit < X - 2
    rsi_14 (bullish entry)    entry gt/gte X -> exit < X - 5
    rsi_14 (oversold entry)   entry lt/lte X -> exit > X + 5
    williams_r (oversold)     entry lt/lte X -> exit > X + 5
    roc_10 (momentum entry)   entry gt/gte X -> exit < 0.0 (momentum itself
                                                  turned negative, regardless
                                                  of the entry's X)

  Excluded entirely (no exit condition derived — volume/pattern-score/
  cross-sectional-rank conditions don't have a clean "thesis broken"
  flip): any volume_ratio_* feature, bb_width_pct, base_breakout_ratio,
  base_breakout_score, double_bottom_score, flag_pattern_score,
  hurst_exp_21d, and any condition using op="top_pct"/"bottom_pct"
  (cross-sectional rank — meaningless evaluated against a single row's
  fixed threshold) or op="between" (no natural single-sided flip).

  A template with NO derivable exit rule at all (every one of its
  conditions falls in the excluded set) gets exit_urgency=0 for every
  row tagged with that template — this policy simply has nothing to say
  about it; combine with another policy via CompositeExitPolicy if a
  P&L-based backstop is also wanted.

OR logic across a template's own derived rules: if ANY ONE of them is
breached on a given row, that row's condition-based thesis is considered
broken -> high urgency (>80, EXIT_URGENT_THRESHOLD territory, same
convention as RuleBasedExitPolicy's stop_hit branch). No breach -> low,
non-triggering urgency (well under MONITOR_THRESHOLD=40, i.e. 'hold').

exit_ctx must carry a `template` column (same as PerTemplateExitPolicy)
plus one column per indicator this policy might reference (sma_200_ratio,
rsi_14, adx_14, macd_hist, williams_r, roc_10, ema_ribbon_alignment,
ema_8_ratio, supertrend_dir, ichimoku_cloud_position, sma_50_ratio,
sma_20_ratio) — the SAME feature values ScreenerEngine/TechnicalAdapter
already compute for entry screening (features/technical.py::
compute_technical_features), reused here rather than recomputed. Rows/
templates missing a particular referenced column simply never trigger
that one rule (treated as not-breached, never fabricated).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES

# Feature -> fixed boundary threshold for the "already-boundary" (simple
# flip) category. All of these templates enter on `> boundary` (or
# `>= boundary`); the exit rule fires when the live value falls back
# below that same boundary.
_BOUNDARY_FEATURES: Dict[str, float] = {
    "sma_200_ratio": 1.0,
    "sma_50_ratio": 1.0,
    "sma_20_ratio": 1.0,
    "macd_hist": 0.0,
    "ema_ribbon_alignment": 0.0,
    "ema_8_ratio": 1.0,
    "supertrend_dir": 0.0,
    "ichimoku_cloud_position": 0.0,
}

# Features explicitly excluded from condition-based exit derivation —
# volume/pattern-score/hurst features have no clean "thesis broken" flip.
_EXCLUDED_FEATURES = {
    "volume_ratio_21d",
    "bb_width_pct",
    "base_breakout_ratio",
    "base_breakout_score",
    "double_bottom_score",
    "flag_pattern_score",
    "hurst_exp_21d",
}

_GT_OPS = ("gt", "gte")
_LT_OPS = ("lt", "lte")

# Urgency bands (0-100), matching RuleBasedExitPolicy's convention:
# EXIT_URGENT_THRESHOLD=80 -> immediate_exit; MONITOR_THRESHOLD=40 -> hold
# below it. See config.settings.
_NOT_TRIGGERED_URGENCY = 30.0
_TRIGGERED_URGENCY_FLOOR = 85.0
_TRIGGERED_URGENCY_CEIL = 98.0


def _derive_exit_rule(feature: str, op: str, value: Any) -> Optional[Tuple[str, str, float]]:
    """One entry condition dict's (feature, op, value) -> derived exit rule
    (feature, comparator, threshold), where comparator in {"lt", "gt"}
    means "breached (exit-worthy) when live value <comparator> threshold".
    Returns None when this condition has no derivable exit rule (excluded
    feature, or an op with no defined mapping — top_pct/bottom_pct/
    between)."""
    if feature.startswith("volume_ratio") or feature in _EXCLUDED_FEATURES:
        return None
    if op in ("top_pct", "bottom_pct", "between", "eq"):
        return None

    if feature in _BOUNDARY_FEATURES and op in _GT_OPS:
        return (feature, "lt", _BOUNDARY_FEATURES[feature])

    if feature == "adx_14" and op in _GT_OPS:
        return (feature, "lt", float(value) - 2.0)

    if feature == "rsi_14":
        if op in _GT_OPS:
            return (feature, "lt", float(value) - 5.0)
        if op in _LT_OPS:
            return (feature, "gt", float(value) + 5.0)
        return None

    if feature == "williams_r" and op in _LT_OPS:
        return (feature, "gt", float(value) + 5.0)

    if feature == "roc_10" and op in _GT_OPS:
        return (feature, "lt", 0.0)

    return None


def build_template_exit_rules() -> Dict[str, List[Tuple[str, str, float]]]:
    """{template_name -> [(feature, comparator, threshold), ...]} derived
    from every ScreenerTemplate's own entry `conditions`
    (systems/technical_analysis/screener/templates.py). Templates whose
    conditions derive zero exit rules are simply absent from the returned
    dict (predict_full treats an absent template the same as one with an
    empty rule list: never triggers via this policy).

    Local import: keeps this module importable without a hard dependency
    on systems.technical_analysis for callers building template_rules
    directly (mirrors per_template_exit_policy.build_default_template_
    params()'s own local-import rationale).
    """
    from systems.technical_analysis.screener.templates import TEMPLATES

    rules: Dict[str, List[Tuple[str, str, float]]] = {}
    for template in TEMPLATES:
        derived: List[Tuple[str, str, float]] = []
        seen_features: set = set()
        for cond in template.conditions:
            feature = cond.get("feature")
            op = cond.get("op")
            value = cond.get("value")
            if feature is None or op is None or feature in seen_features:
                continue
            rule = _derive_exit_rule(feature, op, value)
            if rule is not None:
                derived.append(rule)
                seen_features.add(feature)
        if derived:
            rules[template.name] = derived
    return rules


class ConditionBasedExitPolicy:
    """Exits a position when its OWN entry template's derived condition(s)
    (see module docstring) flip against it — OR logic across whichever of
    a template's conditions map to a derived exit rule. Same
    `predict_full(X) -> DataFrame[exit_urgency, exit_type,
    exit_survival_5d/21d/63d]` contract as RuleBasedExitPolicy."""

    def __init__(self, template_rules: Optional[Dict[str, List[Tuple[str, str, float]]]] = None) -> None:
        """
        Parameters
        ----------
        template_rules : dict, optional
            template_name -> [(feature, comparator, threshold), ...].
            Defaults to build_template_exit_rules() (derived from the real
            42 screener templates). Pass an explicit dict in tests to
            avoid depending on the live template catalog.
        """
        self.template_rules = template_rules if template_rules is not None else build_template_exit_rules()

    def _breach_mask(self, subset: pd.DataFrame, rules: List[Tuple[str, str, float]]) -> pd.Series:
        breached = pd.Series(False, index=subset.index)
        for feature, comparator, threshold in rules:
            if feature not in subset.columns:
                continue  # feature not supplied for this run — never fabricate, just skip this one rule
            col = pd.to_numeric(subset[feature], errors="coerce")
            if comparator == "lt":
                hit = col < threshold
            else:  # "gt"
                hit = col > threshold
            breached = breached | hit.fillna(False)
        return breached

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        if "template" not in X.columns:
            templates = pd.Series(None, index=X.index)
        else:
            templates = X["template"]

        breached = pd.Series(False, index=X.index)
        # Fraction of a template's own rules that are breached, used only to
        # add urgency variety within the triggered band — never affects the
        # trigger/no-trigger decision itself (that's pure OR logic).
        breach_fraction = pd.Series(0.0, index=X.index)

        for template_name in [t for t in templates.dropna().unique()]:
            rules = self.template_rules.get(template_name)
            if not rules:
                continue
            mask = templates == template_name
            subset = X.loc[mask]
            hit_any = pd.Series(False, index=subset.index)
            hit_count = pd.Series(0, index=subset.index)
            for feature, comparator, threshold in rules:
                if feature not in subset.columns:
                    continue
                col = pd.to_numeric(subset[feature], errors="coerce")
                hit = (col < threshold) if comparator == "lt" else (col > threshold)
                hit = hit.fillna(False)
                hit_any = hit_any | hit
                hit_count = hit_count + hit.astype(int)
            breached.loc[subset.index] = hit_any
            breach_fraction.loc[subset.index] = (hit_count / max(len(rules), 1)).astype(float)

        urgency = pd.Series(_NOT_TRIGGERED_URGENCY, index=X.index)
        urgency = urgency.mask(
            breached,
            np.clip(_TRIGGERED_URGENCY_FLOOR + breach_fraction * (_TRIGGERED_URGENCY_CEIL - _TRIGGERED_URGENCY_FLOOR),
                    _TRIGGERED_URGENCY_FLOOR, _TRIGGERED_URGENCY_CEIL),
        )

        exit_type = pd.Series("opportunity_cost", index=X.index)
        exit_type = exit_type.mask(breached, "thesis_broken")

        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = urgency.clip(0, 100)
        out["exit_type"] = exit_type.astype(str)
        out["exit_survival_5d"] = np.nan
        out["exit_survival_21d"] = np.nan
        out["exit_survival_63d"] = np.nan

        assert out["exit_type"].isin(EXIT_TYPES).all() and out["exit_type"].notna().all(), (
            "exit_type must always be a valid, non-null EXIT_TYPES category"
        )

        return out[["exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d"]]
