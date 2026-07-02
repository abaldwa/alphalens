"""
tests/unit/test_paper_trading_step.py

Tests systems/ml_signal_engine/inference/paper_trading_step.py — the shared
portfolio-mechanics functions extracted from scripts/run_paper_trading_sim.py.
Mirrors tests/unit/test_exit_signal.py's "partial close stays open / full
close logs once" regression coverage (the once-buggy behavior this module
exists to define exactly once — see BuildLog.md "Paper Trading Logic Fix").
All tests are offline (no DB, no HTTP, no model inference — RuleBasedExitPolicy
and a hand-built scored DataFrame stand in for trained models).
"""

import tempfile

import pandas as pd

from backtest.portfolio import PortfolioSimulator
from scripts.paper_trading_tracker import PaperTradingTracker
from systems.ml_signal_engine.inference.paper_trading_step import apply_daily_entries, apply_daily_exits
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy


def _exit_ctx(pnl_pct=0.0, days_held=5.0, drawdown=0.0, ticker="TICK"):
    return pd.DataFrame(
        [{"entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown}],
        index=[ticker],
    )


class TestApplyDailyExits:
    def test_empty_context_is_noop(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0)
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            apply_daily_exits(pf, RuleBasedExitPolicy(), pd.DataFrame(), {}, pd.Timestamp("2024-01-05"), tracker, {})
        assert pf.trades == []

    def test_full_close_logs_exactly_once_with_entry_context(self):
        # entry_date must be a plain date (not pd.Timestamp) — same shape
        # production callers pass, since str(trade.entry_date) becomes the
        # log filename and a Timestamp's str() includes "00:00:00".
        from datetime import date

        pf = PortfolioSimulator(initial_capital=1_000_000.0)
        pf.buy("TICK", "IT", 100.0, date(2024, 1, 1), {})
        entry_context = {"TICK": {"entry_time": "09:20:00"}}
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            ctx = _exit_ctx(pnl_pct=-0.20)  # stop_hit, urgency >= 80 -> immediate_exit
            apply_daily_exits(
                pf, RuleBasedExitPolicy(), ctx, {"TICK": 80.0}, date(2024, 1, 5), tracker, entry_context,
            )
            assert "TICK" not in pf.positions
            assert "TICK" not in entry_context  # popped on full close
            logged = tracker.get_trades_for_date("2024-01-01")
            assert len(logged) == 1
            assert logged[0]["entry_time"] == "09:20:00"
            assert logged[0]["exit_type"] == "thesis_broken"

    def test_reduce_position_does_not_log_and_keeps_entry_context(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0)
        pf.buy("TICK", "IT", 100.0, pd.Timestamp("2024-01-01"), {})
        entry_context = {"TICK": {"entry_time": "09:15:00"}}
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            # moderate drawdown after small gain -> momentum_exhaustion, urgency in reduce band (60-80)
            ctx = _exit_ctx(pnl_pct=0.05, days_held=10.0, drawdown=-0.12)
            apply_daily_exits(
                pf, RuleBasedExitPolicy(), ctx, {"TICK": 105.0}, pd.Timestamp("2024-01-05"), tracker, entry_context,
            )
            assert "TICK" in pf.positions  # partial close, still held
            assert "TICK" in entry_context  # not popped — position still open
            assert tracker.get_trades_for_date("2024-01-01") == []  # no log on partial close

    def test_skips_ticker_not_in_portfolio_or_prices(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0)
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            ctx = _exit_ctx(ticker="GHOST")
            apply_daily_exits(pf, RuleBasedExitPolicy(), ctx, {}, pd.Timestamp("2024-01-05"), tracker, {})
        assert pf.trades == []


class TestApplyDailyEntries:
    def test_empty_candidates_is_noop(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=2)
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            apply_daily_entries(pf, pd.DataFrame(), {}, {}, pd.Timestamp("2024-01-01"), tracker, {}, n_positions=2)
        assert pf.positions == {}

    def test_buys_top_n_by_buy_prob(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=3)
        candidates = pd.DataFrame(
            {"buy_prob": [0.9, 0.5, 0.7]}, index=["A", "B", "C"],
        )
        prices = {"A": 100.0, "B": 100.0, "C": 100.0}
        sector_map = {"A": "IT", "B": "IT", "C": "FIN"}
        entry_context = {}
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            apply_daily_entries(
                pf, candidates, sector_map, prices, pd.Timestamp("2024-01-01"), tracker, entry_context, n_positions=2,
            )
        assert set(pf.positions.keys()) == {"A", "C"}  # top 2 by buy_prob
        assert set(entry_context.keys()) == {"A", "C"}

    def test_skips_already_held_tickers(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=3)
        pf.buy("A", "IT", 100.0, pd.Timestamp("2024-01-01"), {})
        candidates = pd.DataFrame({"buy_prob": [0.9, 0.5]}, index=["A", "B"])
        entry_context = {}
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            apply_daily_entries(
                pf, candidates, {"A": "IT", "B": "IT"}, {"A": 100.0, "B": 100.0},
                pd.Timestamp("2024-01-02"), tracker, entry_context, n_positions=2,
            )
        assert "B" in pf.positions
        assert "A" not in entry_context  # never re-bought, no new entry_context entry

    def test_missing_or_invalid_price_skipped(self):
        pf = PortfolioSimulator(initial_capital=1_000_000.0, n_target_positions=2)
        candidates = pd.DataFrame({"buy_prob": [0.9]}, index=["NOPRICE"])
        with tempfile.TemporaryDirectory() as d:
            tracker = PaperTradingTracker(logs_dir=d)
            apply_daily_entries(pf, candidates, {}, {}, pd.Timestamp("2024-01-01"), tracker, {}, n_positions=2)
        assert pf.positions == {}
