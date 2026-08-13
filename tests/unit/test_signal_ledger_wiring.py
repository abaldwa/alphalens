"""
tests/unit/test_signal_ledger_wiring.py

Owner: Platform / Backtest (A94)
Consumers: backtest/core/signal_ledger.py, backtest/core/engine.py
(BacktestOrchestrator), backtest/paper_trading/live_runner.py.

Proves the END of the A94 chain: that signals an adapter emits actually
land in strategy_signals with the right source and run_id. tests/unit/
test_strategy_signals.py already covers the write API in isolation; the
gap this closes is the wiring, which is where "the ledger exists and is
empty" came from in the first place.

tmp_path DuckDB only. Per project policy nothing here writes a row to the
real database, not even one it deletes afterwards — every test either
passes db_path explicitly or redirects BACKTEST_DUCKDB_PATH.

The orchestration fixtures are small hand-built price/signal panels, the
same convention tests/unit/test_core_engine.py uses: this tests a for-loop
and a write path, not market realism, so there is no real data to fabricate.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, Signal
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.core.signal_ledger import SignalLedgerRecorder
from backtest.paper_trading.live_runner import PaperTradingRunner
from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.signals import NO_RUN, read_signals

TRADING_DAYS = pd.date_range("2020-01-01", periods=20, freq="B")


@pytest.fixture
def ledger_db(tmp_path):
    path = tmp_path / "ledger.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


class _FixedSignalAdapter:
    """Emits the same signals every rebalance date, like test_core_engine's."""

    channel = "technical"

    def __init__(self, signals):
        self._signals = signals

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return list(self._signals)

    def feature_vector(self, ticker, as_of_date):
        return {"dummy_feature": 1.0}


def _run(**overrides):
    defaults = dict(
        channel="technical", strategy_id="A1_pullback", horizon_bucket=HorizonBucket.D5,
        mode="backtest", universe_spec="test_universe", start_date=date(2020, 1, 1),
        end_date=date(2020, 3, 1), capital_mode="lump", initial_capital=1_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


def _config(ledger_db, tickers, **overrides):
    prices = {(t, d.date()): 100.0 for t in tickers for d in TRADING_DAYS}
    kwargs = dict(
        trading_days=TRADING_DAYS,
        universe_provider=lambda d: list(tickers),
        price_lookup=lambda t, d: prices.get((t, d)),
        signal_ledger_db_path=ledger_db,
    )
    kwargs.update(overrides)
    return OrchestratorConfig(**kwargs)


class TestOrchestratorPersistsSignals:
    def test_emitted_signals_reach_the_ledger_with_source_and_run_id(self, ledger_db):
        run = _run()
        adapter = _FixedSignalAdapter([
            Signal(ticker="INFY", action="buy", sector="IT", conviction=0.9, template="A1_pullback"),
            Signal(ticker="TCS", action="buy", sector="IT", conviction=0.4, template="A1_pullback"),
        ])
        BacktestOrchestrator().run(run, adapter, _config(ledger_db, ["INFY", "TCS"]))

        rows = read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)
        assert rows, "the orchestrator emitted signals but wrote none to the ledger"
        assert {r["source"] for r in rows} == {"backtest"}
        assert {r["run_id"] for r in rows} == {run.run_id}
        assert {r["ticker"] for r in rows} == {"INFY", "TCS"}

    def test_context_carries_what_the_signal_object_actually_holds(self, ledger_db):
        adapter = _FixedSignalAdapter([
            Signal(ticker="INFY", action="buy", sector="IT", conviction=0.9, template="A1_pullback"),
        ])
        BacktestOrchestrator().run(_run(), adapter, _config(ledger_db, ["INFY"]))

        row = read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)[0]
        assert row["context"] == {"sector": "IT", "template": "A1_pullback", "channel": "technical"}
        assert row["conviction"] == pytest.approx(0.9)

    def test_rank_follows_the_order_capital_is_actually_allocated_in(self, ledger_db):
        """Buys execute sorted by descending conviction, so rank 1 is the
        signal with first claim on cash — a real ordering, not an invented
        field."""
        adapter = _FixedSignalAdapter([
            Signal(ticker="LOW", action="buy", sector="IT", conviction=0.1),
            Signal(ticker="HIGH", action="buy", sector="IT", conviction=0.9),
        ])
        BacktestOrchestrator().run(_run(), adapter, _config(ledger_db, ["LOW", "HIGH"]))

        ranks = {
            r["ticker"]: r["rank"]
            for r in read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)
        }
        assert ranks["HIGH"] == 1
        assert ranks["LOW"] == 2

    def test_persist_signals_false_writes_nothing(self, ledger_db):
        adapter = _FixedSignalAdapter([Signal(ticker="INFY", action="buy", sector="IT")])
        BacktestOrchestrator().run(
            _run(), adapter, _config(ledger_db, ["INFY"], persist_signals=False),
        )
        assert read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db) == []

    def test_persistence_is_on_by_default(self):
        """The flag is opt-OUT. A ledger populated only when someone
        remembers to ask for it is not an audit trail."""
        assert OrchestratorConfig.persist_signals is True

    def test_rerunning_the_same_dates_is_idempotent(self, ledger_db):
        """write_signals deletes-then-inserts, which is what lets a resumed
        job re-emit an interrupted day instead of dying on a PK collision."""
        run = _run()
        adapter = _FixedSignalAdapter([Signal(ticker="INFY", action="buy", sector="IT")])
        config = _config(ledger_db, ["INFY"])

        BacktestOrchestrator().run(run, adapter, config)
        first = read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)
        # Same run_id: the resume case, not a second independent run.
        BacktestOrchestrator().run(run, adapter, config)
        second = read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)

        assert len(second) == len(first)
        assert [r["ticker"] for r in second] == [r["ticker"] for r in first]


class TestHoldsDoNotExplode:
    def test_hold_only_batch_writes_nothing_and_does_not_raise(self, ledger_db):
        """Universe-wide holds are the shape that turns this table from
        millions of rows into hundreds of millions, and write_signals raises
        on them. The recorder drops them before that so one hold cannot kill
        a whole run's batch."""
        recorder = SignalLedgerRecorder(
            strategy_key="technical:A1_pullback", source="backtest", run_id="r1", db_path=ledger_db,
        )
        assert recorder.record(date(2020, 1, 6), [
            Signal(ticker=t, action="hold") for t in ("INFY", "TCS", "WIPRO")
        ]) == 0
        assert recorder.flush() == 0
        assert read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db) == []

    def test_holds_are_dropped_from_a_mixed_batch(self, ledger_db):
        recorder = SignalLedgerRecorder(
            strategy_key="technical:A1_pullback", source="backtest", run_id="r1", db_path=ledger_db,
        )
        recorder.record(date(2020, 1, 6), [
            Signal(ticker="INFY", action="buy"), Signal(ticker="TCS", action="hold"),
        ])
        recorder.flush()
        rows = read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)
        assert [r["ticker"] for r in rows] == ["INFY"]


class TestBatching:
    def test_nothing_is_written_before_flush(self, ledger_db):
        """The whole point of the buffer: a long run must not acquire the
        DuckDB write lock once per rebalance date."""
        recorder = SignalLedgerRecorder(
            strategy_key="technical:A1_pullback", source="backtest", run_id="r1", db_path=ledger_db,
        )
        for day in (6, 7, 8):
            recorder.record(date(2020, 1, day), [Signal(ticker="INFY", action="buy")])
        assert read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db) == []
        assert recorder.flush() == 3
        assert len(read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)) == 3

    def test_buffer_cap_flushes_early(self, ledger_db):
        """The memory guard, not the normal path — a runaway emitter must
        not grow the buffer until the machine OOMs."""
        recorder = SignalLedgerRecorder(
            strategy_key="technical:A1_pullback", source="backtest", run_id="r1",
            db_path=ledger_db, max_buffer_rows=2,
        )
        recorder.record(date(2020, 1, 6), [
            Signal(ticker="INFY", action="buy"), Signal(ticker="TCS", action="buy"),
        ])
        assert len(read_signals(strategy_key="technical:A1_pullback", db_path=ledger_db)) == 2


class TestPaperTrading:
    def test_propose_today_records_signals_with_source_paper(self, tmp_path, monkeypatch, ledger_db):
        import backtest.paper_trading.approval_queue as aq
        import backtest.paper_trading.live_runner as lr

        root = tmp_path / "paper_trading"
        for module, attr in ((aq, "PENDING_DIR"), (aq, "EXECUTIONS_DIR")):
            monkeypatch.setattr(module, attr, root / attr.lower())
        monkeypatch.setattr(aq, "STATE_DIR", root / "state")
        monkeypatch.setattr(lr, "STATE_DIR", root / "state")

        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, signal_ledger_db_path=ledger_db,
        )
        adapter = _FixedSignalAdapter([
            Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9),
        ])
        runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))

        rows = read_signals(strategy_key="technical:ta_5d", db_path=ledger_db)
        assert len(rows) == 1
        assert rows[0]["source"] == "paper"
        # run_id is the NO_RUN sentinel, never NULL: it is in the primary key
        # and DuckDB PK columns cannot be NULL.
        assert rows[0]["run_id"] == NO_RUN
        assert rows[0]["signal_date"] == date(2026, 7, 20)

    def test_persist_signals_false_disables_paper_ledger(self, tmp_path, monkeypatch, ledger_db):
        import backtest.paper_trading.approval_queue as aq
        import backtest.paper_trading.live_runner as lr

        root = tmp_path / "paper_trading"
        monkeypatch.setattr(aq, "PENDING_DIR", root / "pending")
        monkeypatch.setattr(aq, "STATE_DIR", root / "state")
        monkeypatch.setattr(lr, "STATE_DIR", root / "state")

        runner = PaperTradingRunner(
            "technical", "ta_5d", HorizonBucket.D5, 1_000_000.0,
            persist_signals=False, signal_ledger_db_path=ledger_db,
        )
        adapter = _FixedSignalAdapter([Signal(ticker="RELIANCE", action="buy", sector="Energy")])
        runner.propose_today(adapter, ["RELIANCE"], date(2026, 7, 20))
        assert read_signals(strategy_key="technical:ta_5d", db_path=ledger_db) == []
