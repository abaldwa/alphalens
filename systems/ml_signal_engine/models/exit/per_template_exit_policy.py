"""
systems/ml_signal_engine/models/exit/per_template_exit_policy.py

Phase: 3.x (Per-strategy exit discipline)
Owner: ml_signal_engine / exit
Consumers: backtest/engine.py (exit_model), scripts/run_paper_trading_sim.py,
           scripts/run_daily_paper_trading.py

RuleBasedExitPolicy applies ONE flat target/stop/max-hold to every open
position, regardless of which screener template or fundamental preset
generated the entry — a Mean Reversion snap-back trade and a Trend
Following breakout get exited on the same clock even though their
theses play out on entirely different timescales.

PerTemplateExitPolicy is a thin router, not a new exit model: it groups
`exit_ctx` rows by their `template` column (see backtest/portfolio.Position.
template/pillar and backtest/engine.py's exit_ctx build), and for each
group instantiates (and caches) a RuleBasedExitPolicy configured with
that template's own stop_pct/target_pct/max_hold_days, falling back to
a pillar-level default and then a global default for rows with no
template match (untagged positions, legacy callers, templates added
after this policy's params dict was built). Same
`predict_full(X) -> DataFrame[exit_urgency, exit_type, exit_survival_5d/
21d/63d]` contract as RuleBasedExitPolicy/ExitSignalModel — a drop-in
replacement, not a special case callers need to branch on.
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy


def build_default_template_params() -> Dict[str, Dict[str, float]]:
    """Assemble {template/preset name -> {stop_pct, target_pct, max_hold_days}}
    from every ScreenerTemplate's exit_stop_pct/exit_target_pct/
    exit_max_hold_days (systems/technical_analysis/screener/templates.py)
    plus backtest/strategy_id.py's FUNDAMENTAL_PRESET_EXIT_PARAMS, so
    callers can do `PerTemplateExitPolicy(build_default_template_params())`
    without manually wiring either source.

    stop_pct here is the plain positive fraction (e.g. 0.05 for a 5% stop)
    as stored on ScreenerTemplate/FUNDAMENTAL_PRESET_EXIT_PARAMS —
    PerTemplateExitPolicy negates it when constructing each
    RuleBasedExitPolicy (whose own convention is a negative stop_pct).
    """
    # Local imports: avoids a hard import-time dependency from this
    # models/exit module onto the screener/backtest packages for callers
    # that build their own template_params dict directly instead.
    from backtest.strategy_id import FUNDAMENTAL_PRESET_EXIT_PARAMS
    from systems.technical_analysis.screener.templates import TEMPLATES

    params: Dict[str, Dict[str, float]] = {}
    for template in TEMPLATES:
        if template.exit_stop_pct is None or template.exit_target_pct is None or template.exit_max_hold_days is None:
            continue
        params[template.name] = {
            "stop_pct": template.exit_stop_pct,
            "target_pct": template.exit_target_pct,
            "max_hold_days": template.exit_max_hold_days,
        }
    params.update(FUNDAMENTAL_PRESET_EXIT_PARAMS)
    return params


class PerTemplateExitPolicy:
    """Routes each exit_ctx row to a RuleBasedExitPolicy tuned for its
    template (falling back to a pillar-level default, then a global
    default), and returns results in the same shape/row order as
    RuleBasedExitPolicy.predict_full().
    """

    def __init__(
        self,
        template_params: Dict[str, Dict[str, float]],
        default_policy: Optional[RuleBasedExitPolicy] = None,
        pillar_params: Optional[Dict[str, Dict[str, float]]] = None,
        policy_cls: type = RuleBasedExitPolicy,
    ) -> None:
        """
        Parameters
        ----------
        template_params : dict
            template/preset name -> {"stop_pct": float (positive fraction),
            "target_pct": float (positive fraction), "max_hold_days": int}.
            Typically built via build_default_template_params().
        default_policy : RuleBasedExitPolicy, optional
            Fallback for rows whose template is None/unmatched and whose
            pillar (if any) has no entry in pillar_params. Defaults to a
            plain RuleBasedExitPolicy() (its own flat bootstrap numbers).
        pillar_params : dict, optional
            Optional pillar-level ("technical"/"fundamental"/"momentum")
            fallback params, used when a row's template doesn't match
            template_params but its pillar does — one level more specific
            than the global default_policy, one level less specific than
            a template match.
        policy_cls : type, optional
            Per-template policy class to construct. Defaults to
            RuleBasedExitPolicy, so every existing caller is unaffected.
            Added 2026-08-12 for the "risk_managed" exit variant, which needs
            the same per-template routing but a policy whose triggers can
            actually reach the portfolio's exit threshold (see
            risk_managed_exit_policy.py). Any class accepting
            (target_pct, stop_pct, max_hold_days) works; the router itself
            cares only about predict_full().
        """
        self._template_policies: Dict[str, RuleBasedExitPolicy] = {
            name: policy_cls(
                target_pct=p["target_pct"], stop_pct=-abs(p["stop_pct"]), max_hold_days=int(p["max_hold_days"]),
            )
            for name, p in template_params.items()
        }
        self._pillar_policies: Dict[str, RuleBasedExitPolicy] = {
            name: policy_cls(
                target_pct=p["target_pct"], stop_pct=-abs(p["stop_pct"]), max_hold_days=int(p["max_hold_days"]),
            )
            for name, p in (pillar_params or {}).items()
        }
        self.default_policy = default_policy if default_policy is not None else RuleBasedExitPolicy()

    def _policy_for_row(self, template: Optional[str], pillar: Optional[str]) -> RuleBasedExitPolicy:
        if template is not None and template in self._template_policies:
            return self._template_policies[template]
        if pillar is not None and pillar in self._pillar_policies:
            return self._pillar_policies[pillar]
        return self.default_policy

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """Same contract as RuleBasedExitPolicy.predict_full: exit_urgency,
        exit_type, exit_survival_5d/21d/63d, indexed identically to X and
        in X's original row order — regardless of `template`/`pillar`
        columns present on X (both are optional; their absence just means
        every row uses self.default_policy)."""
        has_template = "template" in X.columns
        has_pillar = "pillar" in X.columns
        if not has_template and not has_pillar:
            return self.default_policy.predict_full(X)

        templates = X["template"] if has_template else pd.Series(None, index=X.index)
        pillars = X["pillar"] if has_pillar else pd.Series(None, index=X.index)

        # Group rows by (template, pillar) pair so each distinct policy runs
        # once over its full subset rather than row-by-row.
        group_key = pd.Series(
            [(t if pd.notna(t) else None, p if pd.notna(p) else None) for t, p in zip(templates, pillars)],
            index=X.index,
        )

        feature_cols = [c for c in X.columns if c not in ("template", "pillar")]
        results = []
        for key in group_key.unique():
            template, pillar = key
            mask = group_key == key
            subset = X.loc[mask, feature_cols]
            policy = self._policy_for_row(template, pillar)
            results.append(policy.predict_full(subset))

        out = pd.concat(results)
        return out.loc[X.index]
