"""
tests/unit/test_copilot_spec_builder.py

Tests spec_builder's validation logic (feature resolution against real
known_fields catalogs, unresolved handling). The LLM call itself is
monkeypatched here — this is a unit test of JSON-parsing/validation logic,
not a stand-in for a real Co-Pilot response shown to a user (the no-stub
policy's AST scan excludes tests/, and llm_client.py itself never falls
back to a mocked response in production).
"""

import systems.copilot.spec_builder as spec_builder_mod


def test_build_spec_keeps_known_features_and_flags_unknown(monkeypatch):
    monkeypatch.setattr(
        spec_builder_mod,
        "call_openrouter_json",
        lambda system_prompt, user_prompt: {
            "name": "RSI Dip",
            "description": "Buy oversold large caps",
            "universe": {"rank_start": 1, "rank_end": 100},
            "technical": [
                {"feature": "rsi_14", "op": "lt", "value": 30},
                {"feature": "not_a_real_feature", "op": "lt", "value": 5},
            ],
            "fundamental": [{"feature": "roe", "op": "gt", "value": 0.15}],
            "valuation": [],
            "rules": {"lookback_days": 126, "rebalance_every_n_trading_days": 21, "top_n": 15},
        },
    )

    spec = spec_builder_mod.build_spec("stocks with RSI under 30 and ROE over 15%")

    assert spec.name == "RSI Dip"
    assert spec.technical == [{"feature": "rsi_14", "op": "lt", "value": 30}]
    assert spec.fundamental == [{"feature": "roe", "op": "gt", "value": 0.15}]
    assert any("not_a_real_feature" in u for u in spec.unresolved)
    assert spec.rules.lookback_days == 126
    assert spec.universe.rank_start == 1
    assert spec.source_query == "stocks with RSI under 30 and ROE over 15%"


def test_build_spec_rejects_unsupported_op(monkeypatch):
    monkeypatch.setattr(
        spec_builder_mod,
        "call_openrouter_json",
        lambda system_prompt, user_prompt: {
            "name": "Bad Op",
            "technical": [{"feature": "rsi_14", "op": "fuzzy_match", "value": 30}],
        },
    )

    spec = spec_builder_mod.build_spec("something vague")

    assert spec.technical == []
    assert any("unsupported op" in u for u in spec.unresolved)
