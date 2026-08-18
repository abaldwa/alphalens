"""
systems/copilot/spec_builder.py

NL query -> StrategySpec. Calls the LLM once to get a structured JSON
draft, then resolves every condition's `feature` against known_fields'
real column catalogs. Anything unresolvable is moved to
StrategySpec.unresolved instead of being silently kept or dropped.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from systems.copilot.known_fields import resolve_section
from systems.copilot.llm_client import call_openrouter_json
from systems.copilot.strategy_spec import (
    RebalanceRules,
    StrategySpec,
    SUPPORTED_OPS,
    UniverseFilter,
)

_SYSTEM_PROMPT = """You are a strategy-spec generator for an Indian equities \
research platform. Given a plain-English trading/investing strategy \
description, output ONLY a JSON object (no prose) with this shape:

{
  "name": "short strategy name",
  "description": "one sentence restating the strategy",
  "universe": {"rank_start": int|null, "rank_end": int|null,
               "mcap_min": number|null, "mcap_max": number|null},
  "technical": [{"feature": str, "op": "lt|gt|lte|gte|eq|between|top_pct|bottom_pct",
                 "value": number|[number,number]}],
  "fundamental": [ same condition shape ],
  "valuation": [ same condition shape ],
  "rules": {"lookback_days": int|null, "rebalance_every_n_trading_days": int|null,
            "top_n": int|null}
}

Only use feature names you are confident correspond to real, standard \
technical indicators (e.g. rsi_14, sma_200_ratio, macd_hist, volume_ratio_21d), \
fundamental ratios (e.g. roe, roce, debt_to_equity, eps), or valuation outputs \
(e.g. margin_of_safety, valuation_gap_pct). Do not invent condition values \
you are not asked for. If a part of the request cannot be expressed with a \
feature/op/value condition, omit it rather than guessing."""


def _validate_conditions(section: str, conditions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    known = resolve_section(section)
    valid: List[Dict[str, Any]] = []
    unresolved: List[str] = []
    for cond in conditions:
        feature = cond.get("feature")
        op = cond.get("op")
        if feature not in known:
            unresolved.append(f"{section}.{feature}: not a known {section} feature")
            continue
        if op not in SUPPORTED_OPS:
            unresolved.append(f"{section}.{feature}: unsupported op '{op}'")
            continue
        valid.append(cond)
    return valid, unresolved


def build_spec(nl_query: str) -> StrategySpec:
    """Turn a natural-language query into a validated StrategySpec.

    Raises systems.copilot.llm_client.LLMConfigError/LLMCallError if the
    LLM call itself fails — callers must surface that as a real error,
    never substitute a default spec.
    """
    raw = call_openrouter_json(_SYSTEM_PROMPT, nl_query)

    unresolved: List[str] = []
    technical, u1 = _validate_conditions("technical", raw.get("technical") or [])
    fundamental, u2 = _validate_conditions("fundamental", raw.get("fundamental") or [])
    valuation, u3 = _validate_conditions("valuation", raw.get("valuation") or [])
    unresolved.extend(u1 + u2 + u3)

    universe_raw = raw.get("universe") or {}
    rules_raw = raw.get("rules") or {}

    return StrategySpec(
        name=str(raw.get("name") or "Untitled Strategy"),
        description=str(raw.get("description") or ""),
        source_query=nl_query,
        universe=UniverseFilter(
            rank_start=universe_raw.get("rank_start"),
            rank_end=universe_raw.get("rank_end"),
            mcap_min=universe_raw.get("mcap_min"),
            mcap_max=universe_raw.get("mcap_max"),
        ),
        technical=technical,
        fundamental=fundamental,
        valuation=valuation,
        rules=RebalanceRules(
            lookback_days=rules_raw.get("lookback_days"),
            rebalance_every_n_trading_days=rules_raw.get("rebalance_every_n_trading_days"),
            top_n=rules_raw.get("top_n"),
        ),
        unresolved=unresolved,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
