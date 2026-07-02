"""
tests/unit/test_portfolio_state.py

Tests backtest/portfolio_state.py — save/load round-trip for the
automated daily paper-trading bot's portfolio persistence.
All tests are offline (no DB, no HTTP).
"""

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
