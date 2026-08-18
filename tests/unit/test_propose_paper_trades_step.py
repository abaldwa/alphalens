"""tests/unit/test_propose_paper_trades_step.py — daily_pipeline.step_propose_paper_trades (F2).

Isolated DuckDB only: never the real normalised database, not even to read
(config/CLAUDE.md's no-synthetic-writes rule cuts both ways — a test that
seeded a deployment row into the real DB would deploy a strategy).
"""

from datetime import date

import duckdb
import pytest

from backtest.core.engine import Signal
from ingestion.scheduler import daily_pipeline as dp

RUN_DATE = date(2026, 8, 14)

_DDL = """
CREATE TABLE strategy_deployments (
    strategy_key VARCHAR, channel VARCHAR, is_active BOOLEAN
)
"""


@pytest.fixture
def deployments_db(tmp_path):
    path = tmp_path / "deployments.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute(_DDL)
    conn.close()
    return path


@pytest.fixture(autouse=True)
def isolated_paper_trading(tmp_path, monkeypatch):
    import config.settings as settings
    from backtest.paper_trading import approval_queue as aq
    from backtest.paper_trading import live_runner as lr

    root = tmp_path / "paper_trading"
    for attr, value in [
        ("PAPER_TRADING_ROOT", root), ("PENDING_DIR", root / "pending"),
        ("EXECUTIONS_DIR", root / "executions"), ("STATE_DIR", root / "state"),
    ]:
        monkeypatch.setattr(aq, attr, value)
    monkeypatch.setattr(lr, "STATE_DIR", root / "state")
    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", tmp_path / "ledger.duckdb")
    # The A103 gate is real in production; these tests are about the loop.
    monkeypatch.setattr(lr, "check_readiness", lambda *a, **k: None)


def _add_deployment(path, strategy_key, channel="technical", active=True):
    conn = duckdb.connect(str(path))
    conn.execute("INSERT INTO strategy_deployments VALUES (?, ?, ?)", [strategy_key, channel, active])
    conn.close()


class _FixedAdapter:
    def __init__(self, channel, signals):
        self.channel = channel
        self._signals = signals

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return self._signals

    def feature_vector(self, ticker, as_of_date):
        return {}


def _patch_factory(monkeypatch, builder):
    monkeypatch.setattr("backtest.core.live_adapter_factory.build_live_adapter", builder)


class TestNothingDeployed:
    def test_no_table_is_not_an_error(self, tmp_path):
        empty = tmp_path / "empty.duckdb"
        duckdb.connect(str(empty)).close()
        dp.step_propose_paper_trades(RUN_DATE, db_path=empty)  # must not raise

    def test_no_active_deployments_proposes_nothing(self, deployments_db, monkeypatch):
        called = []
        _patch_factory(monkeypatch, lambda *a, **k: called.append(a))
        _add_deployment(deployments_db, "technical:A1", active=False)
        dp.step_propose_paper_trades(RUN_DATE, db_path=deployments_db)
        assert called == []


class TestProposing:
    def test_an_active_deployment_gets_its_actions_queued(self, deployments_db, monkeypatch):
        from backtest.paper_trading.approval_queue import read_pending_actions

        _add_deployment(deployments_db, "technical:A1")
        _patch_factory(monkeypatch, lambda *a, **k: (
            _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)]),
            ["RELIANCE"],
        ))
        dp.step_propose_paper_trades(RUN_DATE, db_path=deployments_db)
        queued = read_pending_actions("technical", "A1", str(RUN_DATE))
        assert [a.ticker for a in queued] == ["RELIANCE"]
        assert all(a.status == "pending" for a in queued)

    def test_the_strategy_id_is_the_key_without_its_channel_prefix(self, deployments_db, monkeypatch):
        seen = []

        def _build(channel, strategy_id, run_date, *, conn=None, top_n=None):
            seen.append((channel, strategy_id))
            return _FixedAdapter(channel, []), []

        _add_deployment(deployments_db, "momentum:all_risk_b3_101-150_lb6mo_monthly_top15", channel="momentum")
        _patch_factory(monkeypatch, _build)
        dp.step_propose_paper_trades(RUN_DATE, db_path=deployments_db)
        assert seen == [("momentum", "all_risk_b3_101-150_lb6mo_monthly_top15")]


class TestOneFailureDoesNotCostTheRest:
    def test_a_refused_strategy_is_skipped_and_the_next_one_still_runs(
        self, deployments_db, monkeypatch, caplog
    ):
        from backtest.paper_trading.approval_queue import read_pending_actions
        from features.momentum_live import StrategyNotRunnableLive

        _add_deployment(deployments_db, "technical:AAA")
        _add_deployment(deployments_db, "technical:ZZZ")

        def _build(channel, strategy_id, run_date, *, conn=None, top_n=None):
            if strategy_id == "AAA":
                raise StrategyNotRunnableLive("declares filter(s) ['quality_gate']")
            return (
                _FixedAdapter("technical", [Signal(ticker="TCS", action="buy", sector="IT", conviction=0.5)]),
                ["TCS"],
            )

        _patch_factory(monkeypatch, _build)
        dp.step_propose_paper_trades(RUN_DATE, db_path=deployments_db)

        assert read_pending_actions("technical", "AAA", str(RUN_DATE)) == []
        assert [a.ticker for a in read_pending_actions("technical", "ZZZ", str(RUN_DATE))] == ["TCS"]
        assert "quality_gate" in caplog.text

    def test_an_unexpected_failure_is_logged_and_the_loop_continues(
        self, deployments_db, monkeypatch, caplog
    ):
        from backtest.paper_trading.approval_queue import read_pending_actions

        _add_deployment(deployments_db, "technical:AAA")
        _add_deployment(deployments_db, "technical:ZZZ")

        def _build(channel, strategy_id, run_date, *, conn=None, top_n=None):
            if strategy_id == "AAA":
                raise RuntimeError("feature snapshot missing")
            return (
                _FixedAdapter("technical", [Signal(ticker="TCS", action="buy", sector="IT", conviction=0.5)]),
                ["TCS"],
            )

        _patch_factory(monkeypatch, _build)
        dp.step_propose_paper_trades(RUN_DATE, db_path=deployments_db)
        assert [a.ticker for a in read_pending_actions("technical", "ZZZ", str(RUN_DATE))] == ["TCS"]
        assert "feature snapshot missing" in caplog.text


class TestSchedulerWiring:
    def test_the_step_is_dispatchable(self):
        assert dp._STEP_DISPATCH["propose_paper_trades"] is dp.step_propose_paper_trades

    def test_the_step_is_never_backfillable(self):
        """Same rule as paper_trade: a proposal for a day already past was
        never genuinely live, and backfilling one would inflate Gate 7."""
        from ingestion.scheduler.checkpoint import STEPS

        step = next(s for s in STEPS if s["name"] == "propose_paper_trades")
        assert step["is_backfillable"] is False
        assert step["depends_on"] == ["compute_features"]
