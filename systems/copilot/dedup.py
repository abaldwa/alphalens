"""
systems/copilot/dedup.py

Deterministic structural similarity check — no LLM call, so results are
cheap and reproducible. Compares a new StrategySpec's conditions against
every existing screener template (systems/technical_analysis/screener/
templates.py::TEMPLATE_MAP) and every saved Co-Pilot strategy
(systems.copilot.registry.load_all) to catch "same strategy, different
name" before treating something as new.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from config.settings import COPILOT_DEDUP_SIMILARITY_THRESHOLD
from systems.copilot.registry import load_all
from systems.copilot.strategy_spec import StrategySpec
from systems.technical_analysis.screener.templates import TEMPLATE_MAP

_VALUE_TOLERANCE_PCT = 0.15  # numeric values within 15% count as "matching"


@dataclass
class MatchResult:
    matched_name: str
    matched_source: str  # "screener_template" | "saved_strategy"
    similarity: float


def _condition_key(cond: Dict[str, Any]) -> Optional[str]:
    feature = cond.get("feature")
    op = cond.get("op")
    if feature is None or op is None:
        return None
    return f"{feature}:{op}"


def _values_close(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == 0 and b == 0:
            return True
        denom = max(abs(a), abs(b), 1e-9)
        return abs(a - b) / denom <= _VALUE_TOLERANCE_PCT
    return a == b


def _condition_similarity(a: List[Dict[str, Any]], b: List[Dict[str, Any]]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0

    b_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for cond in b:
        key = _condition_key(cond)
        if key:
            b_by_key.setdefault(key, []).append(cond)

    matches = 0
    for cond in a:
        key = _condition_key(cond)
        if not key:
            continue
        candidates = b_by_key.get(key, [])
        if any(_values_close(cond.get("value"), c.get("value")) for c in candidates):
            matches += 1

    return matches / max(len(a), len(b))


def _spec_similarity(spec: StrategySpec, other_conditions: List[Dict[str, Any]]) -> float:
    return _condition_similarity(spec.all_conditions(), other_conditions)


def find_similar(spec: StrategySpec) -> Optional[MatchResult]:
    """Return the closest existing strategy above the similarity threshold, if any."""
    best: Optional[MatchResult] = None

    for template_name, template in TEMPLATE_MAP.items():
        similarity = _spec_similarity(spec, template.conditions)
        if similarity >= COPILOT_DEDUP_SIMILARITY_THRESHOLD and (best is None or similarity > best.similarity):
            best = MatchResult(matched_name=template_name, matched_source="screener_template", similarity=similarity)

    for saved in load_all():
        if saved.name == spec.name:
            continue
        similarity = _condition_similarity(spec.all_conditions(), saved.all_conditions())
        if similarity >= COPILOT_DEDUP_SIMILARITY_THRESHOLD and (best is None or similarity > best.similarity):
            best = MatchResult(matched_name=saved.name, matched_source="saved_strategy", similarity=similarity)

    return best
