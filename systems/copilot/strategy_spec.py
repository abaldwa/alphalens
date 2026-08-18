"""
systems/copilot/strategy_spec.py

Phase: Co-Pilot v1
Specs: (new) SPEC-COPILOT-001
Owner: Co-Pilot
Consumers: systems/copilot/spec_builder.py, dedup.py, backtest_bridge.py,
           registry.py, datastore/api/routers/copilot.py

Structured strategy spec produced by the Co-Pilot's NL-query step. This is
the ONLY thing an LLM is ever allowed to produce for Co-Pilot — a plain
data structure of filter conditions, never executable code. It is
interpreted (never eval'd) by backtest_bridge.py against the existing,
already-reviewed backtest engines.

Condition dicts use the exact same shape as
systems.technical_analysis.screener.templates.ScreenerTemplate.conditions:
{"feature": str, "op": str, "value": scalar/list} or
{"feature": str, "op": str, "feature2": str} for column-vs-column ops.

`unresolved` carries any field the LLM asked for that doesn't map to a
real, already-computed column (technical/fundamental/valuation) — these
are surfaced to the user verbatim, never silently dropped or guessed at
(Absolute Rule 6: no fabricated stand-ins for missing data).

No pandas/heavy imports — pure data structures, loads instantly, matching
systems/technical_analysis/screener/templates.py's convention.
"""

from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional

# Ops supported when interpreting conditions — mirrors templates.py.
SUPPORTED_OPS = {"lt", "gt", "lte", "gte", "eq", "between", "top_pct", "bottom_pct"}


@dataclass
class UniverseFilter:
    """Universe selection — mirrors features.momentum_universe.RANK_BANDS shape.

    rank_start/rank_end : optional market-cap rank band (1-indexed, inclusive),
        e.g. (1, 100) for top-100 by market cap.
    mcap_min/mcap_max : optional absolute market-cap bounds (INR), used only
        when the query doesn't map cleanly to a rank band.
    """

    rank_start: Optional[int] = None
    rank_end: Optional[int] = None
    mcap_min: Optional[float] = None
    mcap_max: Optional[float] = None


@dataclass
class RebalanceRules:
    """Rebalance/holding rules — mirrors backtest.momentum_backtest.MomentumBacktester's
    constructor parameters directly so a spec can be passed straight through.
    """

    lookback_days: Optional[int] = None
    rebalance_every_n_trading_days: Optional[int] = None
    top_n: Optional[int] = None
    # [2026-08-18] grace_cycles and min_momentum are gone. Momentum is a plain
    # list swap -- held while in the top N on raw momentum, sold the moment it
    # is not -- so there is no grace period, and a momentum floor is not a
    # knob the copilot may offer.


# Specs persisted before 2026-08-18 still carry the deprecated grace_cycles /
# min_momentum keys. from_dict filters against this set so an old spec loads
# (without those knobs) instead of raising TypeError.
_REBALANCE_RULE_FIELDS = frozenset(f.name for f in fields(RebalanceRules))

ConditionDict = Dict[str, Any]


@dataclass
class StrategySpec:
    name: str
    description: str
    source_query: str
    universe: UniverseFilter = field(default_factory=UniverseFilter)
    technical: List[ConditionDict] = field(default_factory=list)
    fundamental: List[ConditionDict] = field(default_factory=list)
    valuation: List[ConditionDict] = field(default_factory=list)
    rules: RebalanceRules = field(default_factory=RebalanceRules)
    unresolved: List[str] = field(default_factory=list)
    created_at: str = ""
    created_by: str = "copilot"

    def all_conditions(self) -> List[ConditionDict]:
        return [*self.technical, *self.fundamental, *self.valuation]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "source_query": self.source_query,
            "universe": {
                "rank_start": self.universe.rank_start,
                "rank_end": self.universe.rank_end,
                "mcap_min": self.universe.mcap_min,
                "mcap_max": self.universe.mcap_max,
            },
            "technical": self.technical,
            "fundamental": self.fundamental,
            "valuation": self.valuation,
            "rules": {
                "lookback_days": self.rules.lookback_days,
                "rebalance_every_n_trading_days": self.rules.rebalance_every_n_trading_days,
                "top_n": self.rules.top_n,
            },
            "unresolved": self.unresolved,
            "created_at": self.created_at,
            "created_by": self.created_by,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StrategySpec":
        universe_d = d.get("universe") or {}
        rules_d = d.get("rules") or {}
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            source_query=d.get("source_query", ""),
            universe=UniverseFilter(**universe_d),
            technical=list(d.get("technical") or []),
            fundamental=list(d.get("fundamental") or []),
            valuation=list(d.get("valuation") or []),
            rules=RebalanceRules(
                **{
                    k: v
                    for k, v in rules_d.items()
                    if k in _REBALANCE_RULE_FIELDS
                }
            ),
            unresolved=list(d.get("unresolved") or []),
            created_at=d.get("created_at", ""),
            created_by=d.get("created_by", "copilot"),
        )
