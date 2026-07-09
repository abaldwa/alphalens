"""
tests/unit/test_paper_trading_tracker.py

Phase: 3.x (Paper Trading Logic Fix — Exit Signal bootstrap)
Owner: Platform / QA

Covers classify_target_outcome() and PaperTradingTracker.log_trade()'s
target_outcome column (FutureDevelopment.md #28): whether a closed trade's
target price was actually reached (hit) vs. timed out (max-hold) vs. missed
(stop/thesis-broken/PnD-forced), so a future ExitSignalModel retrain can be
scored against this instead of just raw P&L.
"""

import csv

from scripts.paper_trading_tracker import PaperTradingTracker, classify_target_outcome


class TestClassifyTargetOutcome:
    def test_target_achieved_is_hit(self):
        assert classify_target_outcome("target_achieved") == "hit"

    def test_opportunity_cost_is_timeout(self):
        assert classify_target_outcome("opportunity_cost") == "timeout"

    def test_thesis_broken_is_miss(self):
        assert classify_target_outcome("thesis_broken") == "miss"

    def test_pnd_exit_is_miss(self):
        assert classify_target_outcome("pnd_exit") == "miss"

    def test_momentum_exhaustion_is_miss(self):
        assert classify_target_outcome("momentum_exhaustion") == "miss"

    def test_blank_exit_type_is_unknown(self):
        assert classify_target_outcome(None) == "unknown"
        assert classify_target_outcome("") == "unknown"

    def test_unrecognised_exit_type_is_unknown(self):
        assert classify_target_outcome("some_future_exit_type") == "unknown"


class TestLogTradeTargetOutcome:
    def test_log_trade_derives_target_outcome_from_exit_type(self, tmp_path):
        tracker = PaperTradingTracker(logs_dir=str(tmp_path))
        tracker.log_trade(
            date="2026-01-01", ticker="TICK", signal_type="BUY", entry_price=100.0, quantity=10,
            entry_time="09:15:00", exit_price=110.0, exit_time="15:30:00", exit_date="2026-01-05",
            exit_type="target_achieved", pnl=100.0, pnl_pct=0.10,
        )
        rows = list(csv.DictReader(open(tmp_path / "2026-01-01.csv")))
        assert rows[0]["target_outcome"] == "hit"

    def test_log_trade_explicit_target_outcome_overrides_derivation(self, tmp_path):
        tracker = PaperTradingTracker(logs_dir=str(tmp_path))
        tracker.log_trade(
            date="2026-01-01", ticker="TICK", signal_type="BUY", entry_price=100.0, quantity=10,
            entry_time="09:15:00", exit_type="target_achieved", target_outcome="hit",
        )
        rows = list(csv.DictReader(open(tmp_path / "2026-01-01.csv")))
        assert rows[0]["target_outcome"] == "hit"

    def test_log_trade_without_exit_type_leaves_target_outcome_blank(self, tmp_path):
        tracker = PaperTradingTracker(logs_dir=str(tmp_path))
        tracker.log_trade(
            date="2026-01-01", ticker="TICK", signal_type="BUY", entry_price=100.0, quantity=10,
            entry_time="09:15:00",
        )
        rows = list(csv.DictReader(open(tmp_path / "2026-01-01.csv")))
        assert rows[0]["target_outcome"] == ""
