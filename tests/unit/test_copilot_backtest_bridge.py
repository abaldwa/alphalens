"""tests/unit/test_copilot_backtest_bridge.py"""

from contextlib import contextmanager

import systems.copilot.backtest_bridge as bridge_mod
from systems.copilot.strategy_spec import RebalanceRules, StrategySpec


@contextmanager
def _fake_conn():
    yield object()


def test_run_backtest_unsupported_without_rebalance_rules():
    spec = StrategySpec(name="No Rules", description="", source_query="")

    result = bridge_mod.run_backtest(spec)

    assert result["mode"] == "unsupported"
    assert "rebalance" in result["reason"]


def test_run_backtest_reports_missing_universe_data_honestly(monkeypatch):
    monkeypatch.setattr(bridge_mod, "get_duckdb_connection", lambda *a, **kw: _fake_conn())
    monkeypatch.setattr(bridge_mod, "yearly_band_universes", lambda *a, **kw: {})

    spec = StrategySpec(
        name="Momentum Test",
        description="",
        source_query="",
        rules=RebalanceRules(lookback_days=126, rebalance_every_n_trading_days=21, top_n=15),
    )

    result = bridge_mod.run_backtest(spec)

    assert result["mode"] == "backtest"
    assert "No real market-cap ranking data" in result["reason"]
    assert result["caveats"] == []


def test_fundamental_valuation_conditions_add_explicit_caveat(monkeypatch):
    monkeypatch.setattr(bridge_mod, "get_duckdb_connection", lambda *a, **kw: _fake_conn())
    monkeypatch.setattr(bridge_mod, "yearly_band_universes", lambda *a, **kw: {})

    spec = StrategySpec(
        name="Momentum Test",
        description="",
        source_query="",
        fundamental=[{"feature": "roe", "op": "gt", "value": 0.15}],
        rules=RebalanceRules(lookback_days=126, rebalance_every_n_trading_days=21, top_n=15),
    )

    result = bridge_mod.run_backtest(spec)

    assert any("Fundamental/valuation" in c for c in result["caveats"])
