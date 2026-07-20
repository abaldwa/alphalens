"""tests/unit/test_live_runner.py — backtest/paper_trading/live_runner.py."""

from datetime import date

import pytest

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from backtest.paper_trading import approval_queue as aq
from backtest.paper_trading import live_runner as lr
from backtest.paper_trading.live_runner import PaperTradingRunner


@pytest.fixture(autouse=True)
def isolated_paper_trading_dirs(tmp_path, monkeypatch):
    root = tmp_path / "paper_trading"
    monkeypatch.setattr(aq, "PAPER_TRADING_ROOT", root)
    monkeypatch.setattr(aq, "PENDING_DIR", root / "pending")
    monkeypatch.setattr(aq, "EXECUTIONS_DIR", root / "executions")
    monkeypatch.setattr(aq, "STATE_DIR", root / "state")
    monkeypatch.setattr(lr, "STATE_DIR", root / "state")


class _FixedAdapter:
    def __init__(self, channel, signals):
        self.channel = channel
        self._signals = signals

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return self._signals

    def feature_vector(self, ticker, as_of_date):
        return {}


class TestProposeToday:
    def test_writes_pending_actions_from_adapter_signals(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        assert len(actions) == 1
        assert actions[0].status == "pending"

    def test_channel_mismatch_rejected(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("momentum", [])
        with pytest.raises(ValueError, match="does not match"):
            runner.propose_today(adapter, [], date(2026, 7, 20))

    def test_no_signals_produces_no_pending_actions(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [])
        actions = runner.propose_today(adapter, [], date(2026, 7, 20))
        assert actions == []


class TestAccept:
    def test_accept_buy_executes_against_portfolio_and_saves_state(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))

        decided = runner.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})
        assert decided.status == "accepted"
        assert decided.executed_quantity is not None and decided.executed_quantity > 0

        summary = runner.state_summary()
        assert summary["n_open_positions"] == 1
        assert summary["cash"] < 1_000_000.0

    def test_accept_advances_gate_counter(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})
        assert aq.count_paper_trading_days("technical", "ta_5d") == 1

    def test_unknown_action_id_raises(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        with pytest.raises(ValueError, match="No pending action"):
            runner.accept("not-a-real-id", date(2026, 7, 20), 100.0, {})

    def test_state_persists_across_runner_instances(self):
        runner1 = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner1.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner1.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})

        runner2 = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        summary = runner2.state_summary()
        assert summary["n_open_positions"] == 1


class TestReject:
    def test_reject_does_not_change_portfolio_state(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner.reject(actions[0].action_id, date(2026, 7, 20))

        summary = runner.state_summary()
        assert summary["n_open_positions"] == 0
        assert summary["cash"] == 1_000_000.0

    def test_reject_still_advances_gate_counter(self):
        """A rejected day is still a real day the strategy was reviewed —
        counts toward Gate 7, matching the existing ML-only semantics
        (a human actively deciding 'no' is still forward validation)."""
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner.reject(actions[0].action_id, date(2026, 7, 20))
        assert aq.count_paper_trading_days("technical", "ta_5d") == 1


class TestPortfolioStateRoundTrip:
    def test_save_and_load_preserves_positions_and_trades(self):
        runner = PaperTradingRunner("momentum", "mom_top10", HorizonBucket.D21, 1_000_000.0)
        adapter_buy = _FixedAdapter("momentum", [Signal(ticker="TCS", action="buy", sector="IT", conviction=0.5)])
        actions = runner.propose_today(adapter_buy, ["TCS"], date(2026, 7, 1))
        runner.accept(actions[0].action_id, date(2026, 7, 1), 3000.0, {"TCS": 3000.0})

        reloaded = lr.load_portfolio_state("momentum", "mom_top10")
        assert reloaded is not None
        assert "TCS" in reloaded.positions
        assert reloaded.positions["TCS"].entry_price == 3000.0

    def test_load_missing_state_returns_none(self):
        assert lr.load_portfolio_state("ml", "never_run") is None
