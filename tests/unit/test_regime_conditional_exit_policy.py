"""
tests/unit/test_regime_conditional_exit_policy.py

Phase: 3.x (Exit policy experimentation — Extra experiment 3)
Owner: Platform / QA
Consumers: CI, pytest
"""

import numpy as np
import pandas as pd
import pandas.testing as pdt

from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES, PND_EXIT_SCORE_THRESHOLD, PND_EXIT_URGENCY_FLOOR
from systems.ml_signal_engine.models.exit.regime_conditional_exit_policy import (
    REGIME_STOP_MULTIPLIERS,
    _DEFAULT_REGIME_MULTIPLIER,
    RegimeConditionalExitPolicy,
)

_TEMPLATE_PARAMS = {"T1": {"stop_pct": 0.05, "target_pct": 0.15, "max_hold_days": 21}}


def _row(pnl_pct, regime, template="T1", days_held=5.0, drawdown=0.0):
    row = {
        "template": template, "regime": regime, "days_held": days_held,
        "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown,
    }
    return pd.DataFrame([row], index=["TICK"])


class TestRegimeConditionalExitPolicy:
    def test_multiplier_convention(self):
        assert REGIME_STOP_MULTIPLIERS["bear"] == 0.7
        assert REGIME_STOP_MULTIPLIERS["bull"] == 1.3
        assert REGIME_STOP_MULTIPLIERS["sideways"] == 1.0

    def test_bear_regime_tightens_the_stop(self):
        # base stop 5%, bear -> 0.7 * 5% = 3.5%; a -4% move breaches in Bear
        # but would NOT breach the unscaled 5% stop.
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.04, regime="bear"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_bull_regime_loosens_the_stop(self):
        # base stop 5%, bull -> 1.3 * 5% = 6.5%; a -6% move should NOT breach in Bull.
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.06, regime="bull"))
        assert out["exit_type"].iloc[0] != "thesis_broken"

    def test_sideways_regime_leaves_stop_unchanged(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        # -5.5% breaches the plain 5% stop regardless of regime scaling.
        out = policy.predict_full(_row(pnl_pct=-0.055, regime="sideways"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_same_move_breaches_in_bear_but_not_in_bull(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        bear_out = policy.predict_full(_row(pnl_pct=-0.04, regime="bear"))
        bull_out = policy.predict_full(_row(pnl_pct=-0.04, regime="bull"))
        assert bear_out["exit_type"].iloc[0] == "thesis_broken"
        assert bull_out["exit_type"].iloc[0] != "thesis_broken"

    def test_missing_or_unrecognized_regime_defaults_to_unscaled_stop(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=-0.055, regime=None))
        assert out["exit_type"].iloc[0] == "thesis_broken"  # same as sideways (1.0x)

    def test_target_pct_and_max_hold_are_not_regime_scaled(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        bear_out = policy.predict_full(_row(pnl_pct=0.15, regime="bear"))
        bull_out = policy.predict_full(_row(pnl_pct=0.15, regime="bull"))
        assert bear_out["exit_type"].iloc[0] == "target_achieved"
        assert bull_out["exit_type"].iloc[0] == "target_achieved"

    def test_unmatched_template_falls_back_to_defaults(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS, default_stop_pct=-0.075)
        out = policy.predict_full(_row(pnl_pct=-0.10, regime="sideways", template="UNKNOWN"))
        assert out["exit_type"].iloc[0] == "thesis_broken"

    def test_exit_type_always_valid(self):
        policy = RegimeConditionalExitPolicy(_TEMPLATE_PARAMS)
        out = policy.predict_full(_row(pnl_pct=0.0, regime="sideways"))
        assert out["exit_type"].isin(EXIT_TYPES).all()


def _reference_predict_full_row_loop(policy: RegimeConditionalExitPolicy, X: pd.DataFrame) -> pd.DataFrame:
    """A deliberately non-vectorized, row-by-row reimplementation of the
    per-row parameter lookup `RegimeConditionalExitPolicy.predict_full`
    used before its 2026-07-25 vectorization (that version is no longer in
    production, since it was the confirmed root cause of regime_conditional
    backtest jobs using 6-10x more memory/CPU than every sibling exit-policy
    variant — see FeatureBacklog.md). Kept here, independent of the
    production code, purely as a slow-but-obviously-correct oracle so
    TestPredictFullVectorizedEquivalence can assert the vectorized rewrite
    produces byte-for-byte identical output — the regression test both
    ml-rigor-reviewer and backtest-reviewer required before landing the
    fix, since a grouping-key bug in the vectorized version would silently
    mis-price a subset of positions' risk rather than crash."""
    target_pct = pd.Series(0.0, index=X.index)
    stop_pct = pd.Series(0.0, index=X.index)
    max_hold_days = pd.Series(0, index=X.index)

    templates = X["template"] if "template" in X.columns else pd.Series(None, index=X.index)
    regimes = X["regime"] if "regime" in X.columns else pd.Series(None, index=X.index)

    for idx in X.index:
        base = policy._base_params_for(templates.loc[idx] if pd.notna(templates.loc[idx]) else None)
        regime_raw = regimes.loc[idx]
        regime_key = str(regime_raw).strip().lower() if pd.notna(regime_raw) else None
        multiplier = REGIME_STOP_MULTIPLIERS.get(regime_key, _DEFAULT_REGIME_MULTIPLIER)
        target_pct.loc[idx] = base["target_pct"]
        stop_pct.loc[idx] = -(base["stop_pct"] * multiplier)
        max_hold_days.loc[idx] = base["max_hold_days"]

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
    # STEP 4 (2026-08-13): the reference mirrors the policy's intent column too,
    # otherwise this equivalence test would silently stop covering the one
    # field the engine now actually acts on.
    out["exit_action"] = pd.Series("hold", index=X.index, dtype=object).mask(triggered, "exit")
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

    return out[[
        "exit_action", "exit_urgency", "exit_type",
        "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
    ]]


class TestPredictFullVectorizedEquivalence:
    """Regression coverage the pre-existing test suite structurally could
    not provide: every test above builds a single-row DataFrame, which
    cannot catch a grouping-key bug (rows silently swapped between
    templates/regimes, or dropped/misordered by the pd.concat + reindex
    step) — only a multi-row batch with several distinct (template, regime)
    combinations in the SAME predict_full() call can. Required by both
    ml-rigor-reviewer and backtest-reviewer before landing the 2026-07-25
    vectorization of predict_full (FeatureBacklog.md)."""

    _TEMPLATE_PARAMS = {
        "T1": {"stop_pct": 0.05, "target_pct": 0.15, "max_hold_days": 21},
        "T2": {"stop_pct": 0.08, "target_pct": 0.20, "max_hold_days": 10},
    }

    def _mixed_batch(self) -> pd.DataFrame:
        # Deliberately mixes: two real templates, an unmatched template
        # string, and a NaN template; four regime spellings that must all
        # normalize the same as their canonical lowercase form, plus a
        # NaN regime and an unrecognized regime string — every fallback
        # path the row-loop and the vectorized version must agree on,
        # in one single predict_full() call (unlike every single-row test
        # above, which can't exercise cross-row grouping at all).
        rows = [
            {"template": "T1", "regime": "bear", "days_held": 5.0, "unrealised_pnl_pct": -0.04, "drawdown_from_peak": 0.0},
            {"template": "T1", "regime": "Bull ", "days_held": 5.0, "unrealised_pnl_pct": -0.06, "drawdown_from_peak": 0.0},
            {"template": "T2", "regime": "SIDEWAYS", "days_held": 5.0, "unrealised_pnl_pct": -0.08, "drawdown_from_peak": 0.0},
            {"template": "T2", "regime": "bear", "days_held": 5.0, "unrealised_pnl_pct": -0.05, "drawdown_from_peak": 0.0},
            {"template": "UNKNOWN", "regime": "bull", "days_held": 5.0, "unrealised_pnl_pct": -0.09, "drawdown_from_peak": 0.0},
            {"template": None, "regime": None, "days_held": 5.0, "unrealised_pnl_pct": -0.06, "drawdown_from_peak": 0.0},
            {"template": "T1", "regime": "typhoon", "days_held": 25.0, "unrealised_pnl_pct": 0.02, "drawdown_from_peak": -0.15},
            # Duplicate (template, regime) key repeated across non-adjacent
            # rows, with a distinct outcome per row (via pnl/days_held) —
            # would surface a bug where the vectorized group-by collapses
            # distinct rows sharing a key into one, or misassigns a group's
            # result back to the wrong original row/index label.
            {"template": "T1", "regime": "bear", "days_held": 30.0, "unrealised_pnl_pct": 0.16, "drawdown_from_peak": 0.0},
        ]
        return pd.DataFrame(rows, index=[f"TICK{i}" for i in range(len(rows))])

    def test_vectorized_output_matches_row_loop_reference(self):
        policy = RegimeConditionalExitPolicy(self._TEMPLATE_PARAMS)
        X = self._mixed_batch()
        actual = policy.predict_full(X)
        expected = _reference_predict_full_row_loop(policy, X)
        pdt.assert_frame_equal(actual, expected)

    def test_row_order_and_index_labels_preserved(self):
        policy = RegimeConditionalExitPolicy(self._TEMPLATE_PARAMS)
        X = self._mixed_batch()
        out = policy.predict_full(X)
        assert list(out.index) == list(X.index)

    def test_duplicate_template_regime_key_rows_resolve_independently(self):
        # Two rows share (template="T1", regime="bear") but differ enough
        # in pnl/days_held to land in different exit_type buckets — proves
        # the vectorized group-by is keyed correctly per-row, not
        # collapsing same-key rows into a single shared result.
        policy = RegimeConditionalExitPolicy(self._TEMPLATE_PARAMS)
        X = self._mixed_batch()
        out = policy.predict_full(X)
        assert out.loc["TICK0", "exit_type"] == "thesis_broken"  # -4% breaches bear-scaled 3.5% stop
        assert out.loc["TICK7", "exit_type"] == "target_achieved"  # +16% clears the unscaled 15% target
