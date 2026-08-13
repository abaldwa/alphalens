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
    # A94: propose_today now persists signals with source="paper", and
    # persistence is on by default. Never the real DuckDB, not even briefly.
    import config.settings as settings

    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", tmp_path / "signals_ledger.duckdb")


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
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        assert len(actions) == 1
        assert actions[0].status == "pending"

    def test_channel_mismatch_rejected(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("momentum", [])
        with pytest.raises(ValueError, match="does not match"):
            runner.propose_today(adapter, [], date(2026, 7, 20))

    def test_no_signals_produces_no_pending_actions(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [])
        actions = runner.propose_today(adapter, [], date(2026, 7, 20))
        assert actions == []


class TestAccept:
    def test_accept_buy_executes_against_portfolio_and_saves_state(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))

        decided = runner.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})
        assert decided.status == "accepted"
        assert decided.executed_quantity is not None and decided.executed_quantity > 0

        summary = runner.state_summary()
        assert summary["n_open_positions"] == 1
        assert summary["cash"] < 1_000_000.0

    def test_accept_advances_gate_counter(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})
        assert aq.count_paper_trading_days("technical", "ta_5d") == 1

    def test_unknown_action_id_raises(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        with pytest.raises(ValueError, match="No pending action"):
            runner.accept("not-a-real-id", date(2026, 7, 20), 100.0, {})

    def test_state_persists_across_runner_instances(self):
        runner1 = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner1.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner1.accept(actions[0].action_id, date(2026, 7, 20), 100.0, {"RELIANCE": 100.0})

        runner2 = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        summary = runner2.state_summary()
        assert summary["n_open_positions"] == 1


class TestReject:
    def test_reject_does_not_change_portfolio_state(self):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
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
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        runner.reject(actions[0].action_id, date(2026, 7, 20))
        assert aq.count_paper_trading_days("technical", "ta_5d") == 1


class TestPortfolioStateRoundTrip:
    def test_save_and_load_preserves_positions_and_trades(self):
        runner = PaperTradingRunner("momentum", "mom_top10", HorizonBucket.D21, 1_000_000.0, enforce_readiness=False)
        adapter_buy = _FixedAdapter("momentum", [Signal(ticker="TCS", action="buy", sector="IT", conviction=0.5)])
        actions = runner.propose_today(adapter_buy, ["TCS"], date(2026, 7, 1))
        runner.accept(actions[0].action_id, date(2026, 7, 1), 3000.0, {"TCS": 3000.0})

        reloaded = lr.load_portfolio_state("momentum", "mom_top10")
        assert reloaded is not None
        assert "TCS" in reloaded.positions
        assert reloaded.positions["TCS"].entry_price == 3000.0

    def test_load_missing_state_returns_none(self):
        assert lr.load_portfolio_state("ml", "never_run") is None


class TestReadinessGate:
    """A103 -- the gate must actually refuse, not merely exist.

    Paper trading is the path that most needs it: a proposal here is put in
    front of a human to accept, so a signal computed on incomplete data does
    not look broken, it looks like a recommendation.
    """

    class _NotReady:
        ready = False

        class _M:
            detail = "feature panel missing for 2026-07-20"
            kind = "feature_panel"

        missing = (_M(),)

    class _Ready:
        ready = True
        missing = ()

    class _Checker:
        def __init__(self, verdict):
            self._verdict = verdict
            self.calls = 0

        def check(self, channel, as_of_date, **kwargs):
            self.calls += 1
            return self._verdict

    def _adapter(self):
        return _FixedAdapter(
            "technical",
            [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)],
        )

    def test_an_unready_day_proposes_nothing(self):
        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            readiness_checker=self._Checker(self._NotReady()),
        )
        assert runner.propose_today(self._adapter(), ["RELIANCE"], date(2026, 7, 20)) == []

    def test_the_adapter_is_never_asked_when_unready(self):
        """Refusing after generating would still have read the partial data,
        and would leave the cost of the computation with none of the safety."""
        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            readiness_checker=self._Checker(self._NotReady()),
        )

        class _Exploding:
            channel = "technical"

            def generate_signals(self, *a, **k):
                raise AssertionError("generate_signals must not be called when not ready")

        assert runner.propose_today(_Exploding(), ["RELIANCE"], date(2026, 7, 20)) == []

    def test_a_ready_day_proposes_normally(self):
        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            readiness_checker=self._Checker(self._Ready()),
        )
        actions = runner.propose_today(self._adapter(), ["RELIANCE"], date(2026, 7, 21))
        assert [a.ticker for a in actions] == ["RELIANCE"]

    def test_a_crashing_check_blocks_rather_than_passes(self):
        """The failure mode that would silently disable the gate: treat an
        unrunnable check as a pass and every day looks ready forever."""

        class _Broken:
            def check(self, *a, **k):
                raise RuntimeError("registry unreachable")

        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            readiness_checker=_Broken(),
        )
        assert runner.propose_today(self._adapter(), ["RELIANCE"], date(2026, 7, 22)) == []

    def test_the_gate_can_be_turned_off_deliberately(self):
        checker = self._Checker(self._NotReady())
        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            enforce_readiness=False, readiness_checker=checker,
        )
        actions = runner.propose_today(self._adapter(), ["RELIANCE"], date(2026, 7, 23))
        assert [a.ticker for a in actions] == ["RELIANCE"]
        assert checker.calls == 0
