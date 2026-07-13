"""
tests/unit/test_paper_trading_pending_router.py

A65: router-level tests for the parts of `datastore/api/routers/paper_trading.py`
not already covered by tests/unit/test_paper_trading_router.py (state/trades/
equity_curve/gate_status): GET /watchlist, GET /pending, POST /pending/{id}/
reject, POST /pending/{id}/accept (sell path), POST /positions/{ticker}/sell,
POST /backdated_buy (SPEC-PT-003). Real seeded DuckDB (ohlcv_adjusted,
ml_signals) + real on-disk portfolio_state.json/pending JSON files via
TestClient(app) — no mocks.

`datastore.api.routers.ohlcv`/`signals` each import their own DUCKDB_PATH/
SIGNALS_DUCKDB_PATH copies at import time (paper_trading.py calls their
functions directly, not over HTTP) — patched separately, same reasoning as
test_technical_router.py's dual-module-patch note.
"""

import json
from datetime import date

import pytest
from fastapi.testclient import TestClient

from backtest.portfolio import PortfolioSimulator
from backtest.portfolio_state import save_portfolio_state
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import ohlcv as ohlcv_router
from datastore.api.routers import paper_trading as paper_trading_router
from datastore.api.routers import signals as signals_router
from datastore.schema import create_normalised, create_signals


@pytest.fixture
def env(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(ohlcv_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(signals_router, "SIGNALS_DUCKDB_PATH", signals_path)
    monkeypatch.setattr(paper_trading_router, "PORTFOLIO_STATE_PATH", tmp_path / "portfolio_state.json")
    monkeypatch.setattr(paper_trading_router, "EXECUTIONS_DIR", tmp_path / "executions")
    monkeypatch.setattr(paper_trading_router, "PENDING_DIR", tmp_path / "pending")
    return {"normalised": normalised_path, "signals": signals_path, "tmp_path": tmp_path}


def _seed_price(db_path, ticker, d, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [ticker, d, close, close, close, close, 1_000_000],
        )


def _seed_portfolio_with_position(path, ticker, entry_price, quantity, entry_date="2026-06-01"):
    portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
    portfolio.cash = 500_000.0
    from backtest.portfolio import Position

    portfolio.positions[ticker] = Position(
        ticker=ticker, sector="IT", entry_date=entry_date, entry_price=entry_price,
        quantity=quantity, peak_price=entry_price,
    )
    save_portfolio_state(portfolio, path, as_of_date=entry_date)


class TestHorizonWatchlist:
    def test_no_pending_dir_returns_empty(self, env):
        client = TestClient(app)
        resp = client.get("/api/v1/paper_trading/watchlist")
        assert resp.status_code == 200
        assert resp.json() == {"date": None, "models": {}}

    def test_reads_latest_watchlist_file(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        (pending_dir / "2026-06-01_watchlist.json").write_text(
            json.dumps({"date": "2026-06-01", "models": {"signal_21d": []}})
        )
        (pending_dir / "2026-06-05_watchlist.json").write_text(
            json.dumps({"date": "2026-06-05", "models": {"signal_21d": ["RELIANCE"]}})
        )
        client = TestClient(app)
        resp = client.get("/api/v1/paper_trading/watchlist")
        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-06-05"
        assert body["models"] == {"signal_21d": ["RELIANCE"]}


class TestPendingActions:
    def test_no_pending_dir_returns_empty(self, env):
        client = TestClient(app)
        resp = client.get("/api/v1/paper_trading/pending")
        assert resp.status_code == 200
        assert resp.json() == {"date": None, "actions": []}

    def test_only_pending_status_actions_returned(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        (pending_dir / "2026-06-05.json").write_text(
            json.dumps(
                [
                    {
                        "action_id": "a1", "date": "2026-06-05", "action_type": "buy",
                        "ticker": "RELIANCE", "reason": "high buy_prob", "status": "pending",
                    },
                    {
                        "action_id": "a2", "date": "2026-06-05", "action_type": "sell",
                        "ticker": "TCS", "reason": "exit_urgency", "status": "accepted",
                    },
                ]
            )
        )
        client = TestClient(app)
        resp = client.get("/api/v1/paper_trading/pending")
        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-06-05"
        assert len(body["actions"]) == 1
        assert body["actions"][0]["action_id"] == "a1"

    def test_excludes_watchlist_file(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        (pending_dir / "2026-06-05_watchlist.json").write_text(json.dumps([{"foo": "bar"}]))
        client = TestClient(app)
        resp = client.get("/api/v1/paper_trading/pending")
        assert resp.status_code == 200
        assert resp.json() == {"date": None, "actions": []}


class TestRejectPendingAction:
    def test_no_pending_actions_returns_404(self, env):
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/a1/reject")
        assert resp.status_code == 404

    def test_unknown_action_id_returns_404(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        (pending_dir / "2026-06-05.json").write_text(
            json.dumps([{"action_id": "a1", "action_type": "buy", "ticker": "RELIANCE", "status": "pending"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/not_a_real_id/reject")
        assert resp.status_code == 404

    def test_already_decided_action_returns_409(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        (pending_dir / "2026-06-05.json").write_text(
            json.dumps([{"action_id": "a1", "action_type": "buy", "ticker": "RELIANCE", "status": "accepted"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/a1/reject")
        assert resp.status_code == 409

    def test_reject_marks_status_and_persists(self, env):
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        path = pending_dir / "2026-06-05.json"
        path.write_text(
            json.dumps([{"action_id": "a1", "action_type": "buy", "ticker": "RELIANCE", "status": "pending"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/a1/reject")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "rejected"
        assert body["executed"] is False
        assert json.loads(path.read_text())[0]["status"] == "rejected"


class TestAcceptPendingSellAction:
    def test_accept_sell_action_executes_and_logs_trade(self, env):
        _seed_portfolio_with_position(
            paper_trading_router.PORTFOLIO_STATE_PATH, "RELIANCE", entry_price=100.0, quantity=50,
        )
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        path = pending_dir / "2026-06-10.json"
        path.write_text(
            json.dumps([{"action_id": "s1", "action_type": "sell", "ticker": "RELIANCE", "status": "pending"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/s1/accept")
        assert resp.status_code == 200
        body = resp.json()
        assert body["executed"] is True
        assert body["status"] == "accepted"
        assert json.loads(path.read_text())[0]["status"] == "accepted"
        executions = list((env["tmp_path"] / "executions").glob("*.csv"))
        assert len(executions) == 1

    def test_accept_unknown_action_type_returns_400(self, env):
        _seed_portfolio_with_position(
            paper_trading_router.PORTFOLIO_STATE_PATH, "RELIANCE", entry_price=100.0, quantity=50,
        )
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        path = pending_dir / "2026-06-10.json"
        path.write_text(
            json.dumps([{"action_id": "x1", "action_type": "bogus", "ticker": "RELIANCE", "status": "pending"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/x1/accept")
        assert resp.status_code == 400

    def test_accept_no_price_returns_422(self, env):
        _seed_portfolio_with_position(
            paper_trading_router.PORTFOLIO_STATE_PATH, "RELIANCE", entry_price=100.0, quantity=50,
        )
        pending_dir = env["tmp_path"] / "pending"
        pending_dir.mkdir()
        path = pending_dir / "2026-06-10.json"
        path.write_text(
            json.dumps([{"action_id": "s1", "action_type": "sell", "ticker": "RELIANCE", "status": "pending"}])
        )
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/pending/s1/accept")
        assert resp.status_code == 422


class TestSellPosition:
    def test_no_price_returns_422(self, env):
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/positions/RELIANCE/sell")
        assert resp.status_code == 422

    def test_no_portfolio_state_returns_409(self, env):
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/positions/RELIANCE/sell")
        assert resp.status_code == 409

    def test_ticker_not_held_returns_404(self, env):
        _seed_portfolio_with_position(
            paper_trading_router.PORTFOLIO_STATE_PATH, "TCS", entry_price=100.0, quantity=50,
        )
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/positions/RELIANCE/sell")
        assert resp.status_code == 404

    def test_successful_sell(self, env):
        _seed_portfolio_with_position(
            paper_trading_router.PORTFOLIO_STATE_PATH, "RELIANCE", entry_price=100.0, quantity=50,
        )
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        client = TestClient(app)
        resp = client.post("/api/v1/paper_trading/positions/RELIANCE/sell")
        assert resp.status_code == 200
        body = resp.json()
        assert body["executed"] is True
        assert body["exit_price"] == 120.0
        assert body["pnl"] > 0


class TestBackdatedBuy:
    def test_invalid_date_returns_400(self, env):
        client = TestClient(app)
        resp = client.post(
            "/api/v1/paper_trading/backdated_buy", json={"ticker": "RELIANCE", "date": "not-a-date"},
        )
        assert resp.status_code == 400

    def test_no_ohlcv_data_returns_422(self, env):
        client = TestClient(app)
        resp = client.post(
            "/api/v1/paper_trading/backdated_buy", json={"ticker": "RELIANCE", "date": "2026-06-10"},
        )
        assert resp.status_code == 422

    def test_no_portfolio_state_returns_409(self, env):
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        client = TestClient(app)
        resp = client.post(
            "/api/v1/paper_trading/backdated_buy", json={"ticker": "RELIANCE", "date": "2026-06-10"},
        )
        assert resp.status_code == 409

    def test_successful_backdated_buy(self, env):
        portfolio = PortfolioSimulator(initial_capital=1_000_000.0)
        save_portfolio_state(portfolio, paper_trading_router.PORTFOLIO_STATE_PATH, as_of_date="2026-06-01")
        _seed_price(env["normalised"], "RELIANCE", date(2026, 6, 10), 120.0)
        client = TestClient(app)
        resp = client.post(
            "/api/v1/paper_trading/backdated_buy", json={"ticker": "RELIANCE", "date": "2026-06-10"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["executed"] is True
        assert body["entry_price"] == 120.0
        executions = list((env["tmp_path"] / "executions").glob("*.csv"))
        assert len(executions) == 1
