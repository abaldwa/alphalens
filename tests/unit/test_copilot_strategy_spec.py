"""tests/unit/test_copilot_strategy_spec.py — StrategySpec round-trip."""

from systems.copilot.strategy_spec import RebalanceRules, StrategySpec, UniverseFilter


def test_to_dict_from_dict_round_trip():
    spec = StrategySpec(
        name="RSI Dip",
        description="Buy oversold large caps",
        source_query="stocks with RSI under 30",
        universe=UniverseFilter(rank_start=1, rank_end=100),
        technical=[{"feature": "rsi_14", "op": "lt", "value": 30}],
        fundamental=[],
        valuation=[],
        rules=RebalanceRules(lookback_days=126, rebalance_every_n_trading_days=21, top_n=15),
        unresolved=["technical.made_up_feature: not a known technical feature"],
        created_at="2026-07-19T00:00:00+00:00",
    )

    round_tripped = StrategySpec.from_dict(spec.to_dict())

    assert round_tripped.name == spec.name
    assert round_tripped.universe.rank_start == 1
    assert round_tripped.universe.rank_end == 100
    assert round_tripped.technical == spec.technical
    assert round_tripped.rules.lookback_days == 126
    assert round_tripped.unresolved == spec.unresolved


def test_all_conditions_concatenates_sections():
    spec = StrategySpec(
        name="X",
        description="",
        source_query="",
        technical=[{"feature": "rsi_14", "op": "lt", "value": 30}],
        fundamental=[{"feature": "roe", "op": "gt", "value": 0.15}],
        valuation=[{"feature": "margin_of_safety", "op": "gt", "value": 0.2}],
    )
    assert len(spec.all_conditions()) == 3
