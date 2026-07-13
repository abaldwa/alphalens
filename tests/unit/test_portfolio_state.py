"""
tests/unit/test_portfolio_state.py

Tests backtest/portfolio_state.py — save/load round-trip for the
automated daily paper-trading bot's portfolio persistence.
All tests are offline (no DB, no HTTP).
"""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from backtest.portfolio import PortfolioSimulator
from backtest.portfolio_state import load_portfolio_state, save_portfolio_state


@pytest.fixture
def sim():
    pf = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=10)
    pf.buy("TICK", "IT", 100.0, pd.Timestamp("2024-01-01"), {})
    pf.record_equity(pd.Timestamp("2024-01-01"), {"TICK": 100.0})
    return pf


class TestSaveLoadRoundtrip:
    def test_load_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            assert load_portfolio_state(Path(d) / "missing.json") is None

    def test_round_trip_preserves_cash(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(sim, path, as_of_date="2024-01-01")
            reloaded = load_portfolio_state(path)
            assert reloaded.cash == sim.cash
            assert reloaded.initial_capital == sim.initial_capital

    def test_round_trip_preserves_positions(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(sim, path, as_of_date="2024-01-01")
            reloaded = load_portfolio_state(path)
            assert set(reloaded.positions.keys()) == set(sim.positions.keys())
            orig = sim.positions["TICK"]
            got = reloaded.positions["TICK"]
            assert got.ticker == orig.ticker
            assert got.sector == orig.sector
            assert got.entry_price == orig.entry_price
            assert got.quantity == orig.quantity
            assert got.peak_price == orig.peak_price

    def test_round_trip_preserves_equity_curve(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(sim, path, as_of_date="2024-01-01")
            reloaded = load_portfolio_state(path)
            assert len(reloaded.equity_curve) == len(sim.equity_curve)

    def test_round_trip_empty_portfolio(self):
        pf = PortfolioSimulator(initial_capital=500_000.0)
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(pf, path, as_of_date="2024-01-01")
            reloaded = load_portfolio_state(path)
            assert reloaded.positions == {}
            assert reloaded.cash == 500_000.0

    def test_reloaded_portfolio_can_continue_trading(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(sim, path, as_of_date="2024-01-01")
            reloaded = load_portfolio_state(path)
            trade = reloaded.sell("TICK", 110.0, pd.Timestamp("2024-01-05"), reason="signal")
            assert trade is not None
            assert "TICK" not in reloaded.positions

    def test_position_meta_persisted_and_reloadable_from_disk(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            save_portfolio_state(
                sim, path, as_of_date="2024-01-01",
                position_meta={"TICK": {"buy_prob_entry": 0.8, "target_price": 120.0}},
            )
            saved = json.loads(path.read_text())
            assert saved["position_meta"]["TICK"]["buy_prob_entry"] == 0.8

    def test_position_meta_merges_with_existing_and_prunes_closed_positions(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            # First save: TICK open with meta, plus a stale meta entry for a ticker never held.
            save_portfolio_state(sim, path, as_of_date="2024-01-01", position_meta={"TICK": {"target_price": 100.0}})
            existing = json.loads(path.read_text())
            existing["position_meta"]["GHOST"] = {"target_price": 999.0}
            path.write_text(json.dumps(existing))

            # Second save: no new meta passed -> existing TICK meta preserved, GHOST pruned
            # (GHOST isn't in sim.positions).
            save_portfolio_state(sim, path, as_of_date="2024-01-02")
            saved = json.loads(path.read_text())
            assert "TICK" in saved["position_meta"]
            assert "GHOST" not in saved["position_meta"]

    def test_corrupt_existing_state_file_falls_back_to_empty_meta(self, sim):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            path.write_text("{not valid json")
            # Must not raise despite the pre-existing file being corrupt JSON.
            save_portfolio_state(sim, path, as_of_date="2024-01-01", position_meta={"TICK": {"target_price": 1.0}})
            saved = json.loads(path.read_text())
            assert saved["position_meta"]["TICK"]["target_price"] == 1.0
