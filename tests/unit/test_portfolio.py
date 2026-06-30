"""
tests/unit/test_portfolio.py

Tests backtest/portfolio.py — PortfolioSimulator, Position, Trade dataclasses.
All tests are offline (no DB, no HTTP, no model inference).
"""

import pytest

from backtest.portfolio import MONITOR_THRESHOLD, REDUCE_FRACTION, SIZING_MODES, PortfolioSimulator, Position, Trade


# ===== Fixtures =====

@pytest.fixture
def sim():
    return PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=10)


@pytest.fixture
def sim_atr():
    return PortfolioSimulator(initial_capital=1_000_000.0, sizing_mode="atr", n_target_positions=10)


# ===== Construction =====

class TestPortfolioSimulatorInit:
    def test_default_capital_is_set(self, sim):
        assert sim.initial_capital == 1_000_000.0
        assert sim.cash == 1_000_000.0

    def test_positions_start_empty(self, sim):
        assert sim.positions == {}

    def test_trades_start_empty(self, sim):
        assert sim.trades == []

    def test_invalid_sizing_mode_raises(self):
        with pytest.raises(ValueError):
            PortfolioSimulator(sizing_mode="bad_mode")

    def test_invalid_n_target_positions_raises(self):
        with pytest.raises(ValueError):
            PortfolioSimulator(n_target_positions=0)

    def test_valid_sizing_modes(self):
        for mode in SIZING_MODES:
            p = PortfolioSimulator(sizing_mode=mode)
            assert p.sizing_mode == mode


# ===== Position sizing =====

class TestPositionSize:
    def test_equal_weight_returns_reasonable_qty(self, sim):
        qty = sim.position_size(price=1000.0, portfolio_value=1_000_000.0)
        assert qty > 0
        assert isinstance(qty, int)

    def test_zero_price_raises(self, sim):
        with pytest.raises(ValueError):
            sim.position_size(price=0.0, portfolio_value=1_000_000.0)

    def test_negative_price_raises(self, sim):
        with pytest.raises(ValueError):
            sim.position_size(price=-100.0, portfolio_value=1_000_000.0)

    def test_negative_portfolio_value_raises(self, sim):
        with pytest.raises(ValueError):
            sim.position_size(price=100.0, portfolio_value=-1.0)

    def test_atr_mode_with_valid_atr(self, sim_atr):
        qty = sim_atr.position_size(price=500.0, portfolio_value=1_000_000.0, atr=10.0)
        assert qty >= 0

    def test_atr_mode_falls_back_without_atr(self, sim_atr):
        qty_no_atr = sim_atr.position_size(price=500.0, portfolio_value=1_000_000.0, atr=None)
        qty_eq = sim_atr.position_size(price=500.0, portfolio_value=1_000_000.0)
        assert qty_no_atr == qty_eq

    def test_zero_portfolio_value_returns_zero(self, sim):
        qty = sim.position_size(price=100.0, portfolio_value=0.0)
        assert qty == 0


# ===== can_buy =====

class TestCanBuy:
    def test_can_buy_when_no_positions_and_cash(self, sim):
        assert sim.can_buy("RELIANCE", "Energy", 1000.0, {"RELIANCE": 1000.0})

    def test_cannot_buy_existing_position(self, sim):
        sim.positions["RELIANCE"] = Position("RELIANCE", "Energy", "2024-01-01", 1000.0, 10)
        assert not sim.can_buy("RELIANCE", "Energy", 1000.0, {"RELIANCE": 1000.0})

    def test_cannot_buy_with_no_cash(self):
        p = PortfolioSimulator(initial_capital=1.0)  # almost no cash
        result = p.can_buy("TCS", "IT", 5000.0, {"TCS": 5000.0})
        assert not result


# ===== total_equity =====

class TestTotalEquity:
    def test_total_equity_equals_cash_when_no_positions(self, sim):
        assert sim.total_equity({}) == sim.cash

    def test_total_equity_includes_positions(self, sim):
        sim.positions["TCS"] = Position("TCS", "IT", "2024-01-01", 2000.0, 5)
        equity = sim.total_equity({"TCS": 2100.0})
        assert equity == sim.cash + 5 * 2100.0


# ===== sector_exposure_pct =====

class TestSectorExposure:
    def test_no_exposure_when_no_positions(self, sim):
        assert sim.sector_exposure_pct("IT", {}) == 0.0

    def test_exposure_computed_correctly(self, sim):
        sim.positions["TCS"] = Position("TCS", "IT", "2024-01-01", 2000.0, 10)
        prices = {"TCS": 2000.0}
        exposure = sim.sector_exposure_pct("IT", prices)
        total_equity = sim.total_equity(prices)
        expected = (10 * 2000.0) / total_equity
        assert abs(exposure - expected) < 1e-6


# ===== buy / sell =====

class TestBuySell:
    def test_buy_adds_position(self, sim):
        sim.buy("RELIANCE", "Energy", 1000.0, "2024-01-02", {})
        assert "RELIANCE" in sim.positions

    def test_buy_reduces_cash(self, sim):
        cash_before = sim.cash
        sim.buy("RELIANCE", "Energy", 1000.0, "2024-01-02", {})
        assert sim.cash < cash_before

    def test_sell_removes_position(self, sim):
        sim.buy("RELIANCE", "Energy", 1000.0, "2024-01-02", {})
        sim.sell("RELIANCE", 1100.0, "2024-01-10", "signal")
        assert "RELIANCE" not in sim.positions

    def test_sell_records_trade(self, sim):
        sim.buy("TCS", "IT", 2000.0, "2024-01-02", {})
        sim.sell("TCS", 2200.0, "2024-01-10", "signal")
        assert len(sim.trades) == 1
        trade = sim.trades[0]
        assert trade.ticker == "TCS"

    def test_sell_nonexistent_ticker_is_noop(self, sim):
        result = sim.sell("NONEXISTENT", 500.0, "2024-01-10", "signal")
        assert result is None
        assert sim.trades == []

    def test_buy_already_held_is_noop(self, sim):
        sim.buy("TCS", "IT", 2000.0, "2024-01-02", {})
        result = sim.buy("TCS", "IT", 2100.0, "2024-01-03", {})
        assert result is None
        assert len(sim.positions) == 1

    def test_reduce_position_partial_close(self, sim):
        sim.buy("INFY", "IT", 1500.0, "2024-01-02", {})
        qty_before = sim.positions["INFY"].quantity
        sim.reduce_position("INFY", 1600.0, "2024-01-10")
        assert len(sim.trades) == 1


# ===== apply_exit_signal =====

class TestApplyExitSignal:
    def test_urgent_exit_above_threshold_closes_position(self, sim):
        sim.buy("RELIANCE", "Energy", 1000.0, "2024-01-02", {})
        from config.settings import EXIT_URGENT_THRESHOLD
        urgency = EXIT_URGENT_THRESHOLD + 1
        sim.apply_exit_signal("RELIANCE", urgency, 1050.0, "2024-01-10")
        assert "RELIANCE" not in sim.positions

    def test_low_urgency_holds_position(self, sim):
        sim.buy("RELIANCE", "Energy", 1000.0, "2024-01-02", {})
        sim.apply_exit_signal("RELIANCE", 10.0, 1050.0, "2024-01-10")
        assert "RELIANCE" in sim.positions

    def test_exit_action_for_urgency_immediate_exit(self):
        from config.settings import EXIT_URGENT_THRESHOLD
        action = PortfolioSimulator.exit_action_for_urgency(EXIT_URGENT_THRESHOLD + 1)
        assert action == "immediate_exit"

    def test_exit_action_for_urgency_reduce(self):
        from config.settings import EXIT_REDUCE_THRESHOLD, EXIT_URGENT_THRESHOLD
        action = PortfolioSimulator.exit_action_for_urgency((EXIT_REDUCE_THRESHOLD + EXIT_URGENT_THRESHOLD) / 2)
        assert action == "reduce_position"

    def test_exit_action_for_urgency_monitor(self):
        action = PortfolioSimulator.exit_action_for_urgency(50.0)
        assert action == "monitor"

    def test_exit_action_for_urgency_hold(self):
        action = PortfolioSimulator.exit_action_for_urgency(10.0)
        assert action == "hold"


# ===== record_equity =====

class TestRecordEquity:
    def test_record_equity_appends_to_curve(self, sim):
        sim.record_equity("2024-01-01", {})
        assert len(sim._equity_curve) == 1

    def test_equity_curve_has_date_and_value(self, sim):
        sim.record_equity("2024-01-01", {})
        entry = sim._equity_curve[0]
        assert "date" in entry
        assert "equity" in entry


# ===== trades_df / equity_curve =====

class TestOutputDataframes:
    def test_trades_df_empty(self, sim):
        df = sim.trades_df
        assert df.empty

    def test_trades_df_has_rows_after_trades(self, sim):
        sim.buy("TCS", "IT", 2000.0, "2024-01-02", {})
        sim.sell("TCS", 2200.0, "2024-01-10")
        df = sim.trades_df
        assert len(df) == 1

    def test_equity_curve_empty(self, sim):
        df = sim.equity_curve
        assert df.empty

    def test_equity_curve_has_rows_after_record(self, sim):
        sim.record_equity("2024-01-01", {})
        df = sim.equity_curve
        assert len(df) == 1
        assert "date" in df.columns
        assert "equity" in df.columns


# ===== Position dataclass =====

class TestPosition:
    def test_peak_price_defaults_to_entry_price(self):
        pos = Position("TCS", "IT", "2024-01-01", 2000.0, 10)
        assert pos.peak_price == 2000.0

    def test_peak_price_can_be_set(self):
        pos = Position("TCS", "IT", "2024-01-01", 2000.0, 10, peak_price=2100.0)
        assert pos.peak_price == 2100.0


# ===== Module-level constants =====

class TestConstants:
    def test_sizing_modes_non_empty(self):
        assert len(SIZING_MODES) > 0
        assert "equal_weight" in SIZING_MODES

    def test_reduce_fraction_is_valid(self):
        assert 0 < REDUCE_FRACTION < 1

    def test_monitor_threshold_is_positive(self):
        assert MONITOR_THRESHOLD > 0
