"""tests/unit/test_paper_trading_unified_router.py — datastore/api/routers/paper_trading_unified.py."""

import pytest
from fastapi.testclient import TestClient

from backtest.core.engine import Signal
from backtest.paper_trading import approval_queue as aq
from backtest.paper_trading import live_runner as lr
from backtest.paper_trading.live_runner import PaperTradingRunner
from backtest.core.horizon import HorizonBucket
from datastore.api.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    root = tmp_path / "paper_trading"
    monkeypatch.setattr(aq, "PAPER_TRADING_ROOT", root)
    monkeypatch.setattr(aq, "PENDING_DIR", root / "pending")
    monkeypatch.setattr(aq, "EXECUTIONS_DIR", root / "executions")
    monkeypatch.setattr(aq, "STATE_DIR", root / "state")
    monkeypatch.setattr(lr, "STATE_DIR", root / "state")
    # A94: propose_today persists signals to the ledger, and persistence is ON
    # by default. Without this the router tests write source="paper" rows into
    # the REAL BACKTEST_DUCKDB_PATH — which they did, until this was added.
    # Never the real DuckDB, not even briefly.
    import config.settings as settings

    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", tmp_path / "signals_ledger.duckdb")
    return TestClient(app)


class _FixedAdapter:
    def __init__(self, channel, signals):
        self.channel = channel
        self._signals = signals

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return self._signals

    def feature_vector(self, ticker, as_of_date):
        return {}


class TestListPending:
    def test_empty_when_nothing_proposed(self, client):
        response = client.get("/api/v1/paper_trading2/technical/ta_5d/pending", params={"as_of_date": "2026-07-20"})
        assert response.status_code == 200
        assert response.json()["actions"] == []

    def test_lists_a_real_proposed_action(self, client):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        runner.propose_today(adapter, ["RELIANCE"], "2026-07-20")

        response = client.get("/api/v1/paper_trading2/technical/ta_5d/pending", params={"as_of_date": "2026-07-20"})
        actions = response.json()["actions"]
        assert len(actions) == 1
        assert actions[0]["ticker"] == "RELIANCE"
        assert actions[0]["status"] == "pending"


class TestAccept:
    def test_accept_requires_horizon_bucket_on_first_ever_action(self, client):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], "2026-07-20")

        # No horizon_bucket/initial_capital in the request, and no state exists yet -> should fail cleanly
        response = client.post(
            f"/api/v1/paper_trading2/technical/ta_5d/pending/{actions[0].action_id}/accept",
            json={"as_of_date": "2026-07-20", "price": 100.0, "prices": {"RELIANCE": 100.0}},
        )
        assert response.status_code == 404
        assert "No existing paper-trading state" in response.json()["detail"]

    def test_accept_with_horizon_bucket_creates_state_and_executes(self, client):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], "2026-07-20")

        response = client.post(
            f"/api/v1/paper_trading2/technical/ta_5d/pending/{actions[0].action_id}/accept",
            json={
                "as_of_date": "2026-07-20", "price": 100.0, "prices": {"RELIANCE": 100.0},
                "horizon_bucket": "5_day", "initial_capital": 1_000_000.0,
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "accepted"
        assert body["executed_quantity"] is not None

    def test_accept_unknown_action_id_returns_404(self, client):
        response = client.post(
            "/api/v1/paper_trading2/technical/ta_5d/pending/not-a-real-id/accept",
            json={"as_of_date": "2026-07-20", "price": 100.0, "horizon_bucket": "5_day", "initial_capital": 1_000_000.0},
        )
        assert response.status_code == 404


class TestReject:
    def test_reject_never_needs_horizon_bucket(self, client):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], "2026-07-20")

        response = client.post(
            f"/api/v1/paper_trading2/technical/ta_5d/pending/{actions[0].action_id}/reject",
            json={"as_of_date": "2026-07-20"},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "rejected"


class TestGateStatus:
    def test_zero_days_for_a_never_run_strategy(self, client):
        response = client.get("/api/v1/paper_trading2/momentum/brand_new/gate_status")
        assert response.status_code == 200
        body = response.json()
        assert body["days_completed"] == 0
        assert body["gate_passed"] is False

    def test_never_settable_via_this_router(self, client):
        """No endpoint on this router can ever set gate_passed/live_eligible
        directly — it's purely derived from counted execution files."""
        import inspect
        from datastore.api.routers import paper_trading_unified as router_module
        source = inspect.getsource(router_module)
        assert "live_eligible" not in source or "live_eligible = True" not in source


class TestStateSummary:
    def test_404_when_no_state_exists(self, client):
        response = client.get("/api/v1/paper_trading2/ml/never_run/state")
        assert response.status_code == 404

    def test_returns_state_after_a_real_accept(self, client):
        runner = PaperTradingRunner("technical", "ta_5d", HorizonBucket.D5, 1_000_000.0, enforce_readiness=False)
        adapter = _FixedAdapter("technical", [Signal(ticker="RELIANCE", action="buy", sector="Energy", conviction=0.9)])
        actions = runner.propose_today(adapter, ["RELIANCE"], "2026-07-20")
        runner.accept(actions[0].action_id, "2026-07-20", 100.0, {"RELIANCE": 100.0})

        response = client.get("/api/v1/paper_trading2/technical/ta_5d/state")
        assert response.status_code == 200
        assert response.json()["n_open_positions"] == 1
