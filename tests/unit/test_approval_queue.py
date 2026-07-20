"""tests/unit/test_approval_queue.py — backtest/paper_trading/approval_queue.py."""

from datetime import date

import pytest

from backtest.core.engine import Signal
from backtest.paper_trading import approval_queue as aq


@pytest.fixture(autouse=True)
def isolated_paper_trading_dirs(tmp_path, monkeypatch):
    """Never write to the real project paper_trading/ directory from tests."""
    monkeypatch.setattr(aq, "PAPER_TRADING_ROOT", tmp_path / "paper_trading")
    monkeypatch.setattr(aq, "PENDING_DIR", tmp_path / "paper_trading" / "pending")
    monkeypatch.setattr(aq, "EXECUTIONS_DIR", tmp_path / "paper_trading" / "executions")
    monkeypatch.setattr(aq, "STATE_DIR", tmp_path / "paper_trading" / "state")


class TestWriteAndReadPendingActions:
    def test_round_trips_signals_as_pending_actions(self):
        signals = [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.8)]
        written = aq.write_pending_actions("technical", "ta_5d", date(2026, 7, 20), signals)
        assert len(written) == 1
        assert written[0].status == "pending"

        read_back = aq.read_pending_actions("technical", "ta_5d", date(2026, 7, 20))
        assert len(read_back) == 1
        assert read_back[0].ticker == "RELIANCE"
        assert read_back[0].action_id == written[0].action_id

    def test_no_file_returns_empty_list_not_error(self):
        assert aq.read_pending_actions("momentum", "unknown_strategy", date(2026, 7, 20)) == []

    def test_different_strategies_are_isolated(self):
        aq.write_pending_actions("momentum", "mom_a", date(2026, 7, 20), [Signal(ticker="A", action="buy")])
        aq.write_pending_actions("momentum", "mom_b", date(2026, 7, 20), [Signal(ticker="B", action="buy")])
        a = aq.read_pending_actions("momentum", "mom_a", date(2026, 7, 20))
        b = aq.read_pending_actions("momentum", "mom_b", date(2026, 7, 20))
        assert [x.ticker for x in a] == ["A"]
        assert [x.ticker for x in b] == ["B"]


class TestUpdateActionStatus:
    def test_accept_transitions_status_and_records_execution_fields(self):
        written = aq.write_pending_actions("ml", "ml_signal_5d", date(2026, 7, 20), [Signal(ticker="TCS", action="buy")])
        updated = aq.update_action_status(
            "ml", "ml_signal_5d", date(2026, 7, 20), written[0].action_id, "accepted",
            executed_price=3500.0, executed_quantity=10,
        )
        assert updated.status == "accepted"
        assert updated.executed_price == 3500.0
        assert updated.decided_at is not None

        persisted = aq.read_pending_actions("ml", "ml_signal_5d", date(2026, 7, 20))
        assert persisted[0].status == "accepted"

    def test_reject_transitions_status(self):
        written = aq.write_pending_actions("fundamental", "fa_quality", date(2026, 7, 20), [Signal(ticker="INFY", action="buy")])
        updated = aq.update_action_status("fundamental", "fa_quality", date(2026, 7, 20), written[0].action_id, "rejected")
        assert updated.status == "rejected"

    def test_unknown_action_id_raises(self):
        aq.write_pending_actions("ml", "ml_signal_5d", date(2026, 7, 20), [Signal(ticker="TCS", action="buy")])
        with pytest.raises(ValueError, match="No pending action"):
            aq.update_action_status("ml", "ml_signal_5d", date(2026, 7, 20), "not-a-real-id", "accepted")

    def test_already_decided_action_cannot_be_decided_again(self):
        written = aq.write_pending_actions("ml", "ml_signal_5d", date(2026, 7, 20), [Signal(ticker="TCS", action="buy")])
        aq.update_action_status("ml", "ml_signal_5d", date(2026, 7, 20), written[0].action_id, "accepted")
        with pytest.raises(ValueError, match="already"):
            aq.update_action_status("ml", "ml_signal_5d", date(2026, 7, 20), written[0].action_id, "rejected")

    def test_invalid_status_value_rejected(self):
        written = aq.write_pending_actions("ml", "ml_signal_5d", date(2026, 7, 20), [Signal(ticker="TCS", action="buy")])
        with pytest.raises(ValueError, match="status must be"):
            aq.update_action_status("ml", "ml_signal_5d", date(2026, 7, 20), written[0].action_id, "maybe")


class TestGateStatus:
    def test_no_executions_reports_zero_days_gate_not_passed(self):
        status = aq.gate_status("momentum", "brand_new_strategy")
        assert status == {
            "channel": "momentum", "strategy_id": "brand_new_strategy",
            "days_completed": 0, "gate_threshold": 90, "gate_passed": False,
        }

    def test_counts_distinct_dated_execution_files(self):
        for day in range(1, 6):
            action = aq.write_pending_actions("technical", "ta_5d", date(2026, 7, day), [Signal(ticker="X", action="buy")])[0]
            decided = aq.update_action_status("technical", "ta_5d", date(2026, 7, day), action.action_id, "accepted")
            aq.record_execution("technical", "ta_5d", date(2026, 7, day), decided)
        status = aq.gate_status("technical", "ta_5d")
        assert status["days_completed"] == 5
        assert status["gate_passed"] is False

    def test_gate_passes_at_threshold(self):
        for day in range(1, 91):
            action = aq.write_pending_actions("ml", "gate_test", date(2020, 1, 1), [Signal(ticker="X", action="buy")])[0]
            decided = aq.update_action_status("ml", "gate_test", date(2020, 1, 1), action.action_id, "accepted")
            aq.record_execution("ml", "gate_test", f"synthetic-day-{day}", decided)
        status = aq.gate_status("ml", "gate_test")
        assert status["days_completed"] == 90
        assert status["gate_passed"] is True

    def test_strategies_within_same_channel_counted_independently(self):
        action = aq.write_pending_actions("momentum", "strat_a", date(2026, 7, 20), [Signal(ticker="X", action="buy")])[0]
        decided = aq.update_action_status("momentum", "strat_a", date(2026, 7, 20), action.action_id, "accepted")
        aq.record_execution("momentum", "strat_a", date(2026, 7, 20), decided)
        assert aq.gate_status("momentum", "strat_a")["days_completed"] == 1
        assert aq.gate_status("momentum", "strat_b")["days_completed"] == 0
