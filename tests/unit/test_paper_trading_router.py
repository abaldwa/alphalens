"""
tests/unit/test_paper_trading_router.py

Phase: 3.x (Automated Daily Paper Trading)
Specs: SPEC-DS-002, SPEC-OBS-004
Owner: Platform / QA
Consumers: CI, pytest

Exercises the real FastAPI app against on-disk portfolio_state.json /
paper_trading/executions fixtures (not mocks), same pattern as
tests/unit/test_corporate_actions_api.py.
"""

import json

import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app
from datastore.api.routers import paper_trading as paper_trading_router


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(paper_trading_router, "PORTFOLIO_STATE_PATH", tmp_path / "portfolio_state.json")
    monkeypatch.setattr(paper_trading_router, "EXECUTIONS_DIR", tmp_path / "executions")
    return TestClient(app)


class TestGetState:
    def test_no_state_file_returns_unavailable(self, client):
        response = client.get("/api/v1/paper_trading/state")
        assert response.status_code == 200
        body = response.json()
        assert body["available"] is False
        assert body["positions"] == []

    def test_state_file_returns_positions_and_equity(self, client):
        state = {
            "as_of_date": "2026-06-29",
            "cash": 500_000.0,
            "initial_capital": 1_000_000.0,
            "positions": [
                {
                    "ticker": "TICK",
                    "sector": "IT",
                    "entry_date": "2026-06-01",
                    "entry_price": 100.0,
                    "quantity": 50,
                    "peak_price": 110.0,
                }
            ],
            "equity_curve": [{"date": "2026-06-29", "equity": 505_000.0}],
        }
        paper_trading_router.PORTFOLIO_STATE_PATH.write_text(json.dumps(state))

        response = client.get("/api/v1/paper_trading/state")
        body = response.json()
        assert body["available"] is True
        assert body["cash"] == 500_000.0
        assert body["total_equity"] == 500_000.0 + 50 * 100.0
        assert len(body["positions"]) == 1
        assert body["positions"][0]["ticker"] == "TICK"


class TestGetTrades:
    def test_no_executions_dir_returns_empty(self, client):
        response = client.get("/api/v1/paper_trading/trades")
        assert response.status_code == 200
        assert response.json() == {"trades": [], "count": 0}

    def test_reads_closed_trades_and_skips_open(self, client):
        execs = paper_trading_router.EXECUTIONS_DIR
        execs.mkdir(parents=True)
        (execs / "2026-06-29.csv").write_text(
            "date,ticker,signal_type,entry_price,quantity,entry_time,exit_price,exit_time,exit_date,exit_type,pnl,pnl_pct\n"
            "2026-06-29,TICK,BUY,100.0,50,09:15:00,110.0,15:30:00,2026-06-29,target_achieved,500.0,0.10\n"
            "2026-06-29,OPEN1,BUY,200.0,10,09:15:00,,,,,,\n"
        )
        response = client.get("/api/v1/paper_trading/trades")
        body = response.json()
        assert body["count"] == 1
        assert body["trades"][0]["ticker"] == "TICK"

    def test_date_range_filter(self, client):
        execs = paper_trading_router.EXECUTIONS_DIR
        execs.mkdir(parents=True)
        (execs / "2026-06-29.csv").write_text(
            "date,ticker,signal_type,entry_price,quantity,entry_time,exit_price,exit_time,exit_date,exit_type,pnl,pnl_pct\n"
            "2026-06-20,TICK,BUY,100.0,50,09:15:00,110.0,15:30:00,2026-06-29,target_achieved,500.0,0.10\n"
        )
        response = client.get("/api/v1/paper_trading/trades", params={"start_date": "2026-07-01"})
        assert response.json()["count"] == 0


class TestEquityCurve:
    def test_no_state_file_returns_empty(self, client):
        response = client.get("/api/v1/paper_trading/equity_curve")
        assert response.json() == {"points": []}

    def test_returns_points_from_state(self, client):
        state = {
            "as_of_date": "2026-06-29", "cash": 1.0, "initial_capital": 1.0,
            "positions": [], "equity_curve": [{"date": "2026-06-01", "equity": 1_000_000.0}],
        }
        paper_trading_router.PORTFOLIO_STATE_PATH.write_text(json.dumps(state))
        response = client.get("/api/v1/paper_trading/equity_curve")
        assert response.json()["points"] == [{"date": "2026-06-01", "equity": 1_000_000.0}]


class TestGateStatus:
    def test_no_executions_dir_zero_days(self, client):
        response = client.get("/api/v1/paper_trading/gate_status")
        body = response.json()
        assert body == {"days_count": 0, "gate_threshold": 90, "gate_cleared": False}

    def test_counts_distinct_dated_csvs(self, client):
        execs = paper_trading_router.EXECUTIONS_DIR
        execs.mkdir(parents=True)
        for d in ("2026-06-27", "2026-06-28", "2026-06-29"):
            (execs / f"{d}.csv").write_text("date\n")
        response = client.get("/api/v1/paper_trading/gate_status")
        body = response.json()
        assert body["days_count"] == 3
        assert body["gate_cleared"] is False
