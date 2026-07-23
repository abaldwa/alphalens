"""tests/unit/test_copilot_dedup.py — deterministic structural similarity, no LLM."""

import systems.copilot.dedup as dedup_mod
from systems.copilot.strategy_spec import StrategySpec
from systems.technical_analysis.screener.templates import TEMPLATE_MAP


def test_finds_match_against_existing_screener_template(monkeypatch):
    monkeypatch.setattr(dedup_mod, "load_all", lambda: [])
    a1 = TEMPLATE_MAP["A1"]
    spec = StrategySpec(
        name="My BB Squeeze Clone",
        description="",
        source_query="",
        technical=list(a1.conditions),
    )

    match = dedup_mod.find_similar(spec)

    assert match is not None
    assert match.matched_name == "A1"
    assert match.matched_source == "screener_template"
    assert match.similarity == 1.0


def test_no_match_for_dissimilar_conditions(monkeypatch):
    monkeypatch.setattr(dedup_mod, "load_all", lambda: [])
    spec = StrategySpec(
        name="Something Unrelated",
        description="",
        source_query="",
        technical=[{"feature": "hurst_exp_21d", "op": "gt", "value": 0.9}],
    )

    assert dedup_mod.find_similar(spec) is None


def test_matches_against_saved_strategy(monkeypatch):
    saved = StrategySpec(
        name="Saved Momentum Strategy",
        description="",
        source_query="",
        technical=[{"feature": "rsi_14", "op": "lt", "value": 30}],
    )
    monkeypatch.setattr(dedup_mod, "load_all", lambda: [saved])

    spec = StrategySpec(
        name="New Query Result",
        description="",
        source_query="",
        technical=[{"feature": "rsi_14", "op": "lt", "value": 32}],  # within tolerance
    )

    match = dedup_mod.find_similar(spec)

    assert match is not None
    assert match.matched_name == "Saved Momentum Strategy"
    assert match.matched_source == "saved_strategy"
