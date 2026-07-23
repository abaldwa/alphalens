"""
tests/unit/test_copilot_router.py

TestClient(app) tests for datastore/api/routers/copilot.py. The `/query`
endpoint's LLM call and the `/dedup`+`/save` endpoints' registry I/O are
monkeypatched at the module level actually used by the router
(systems.copilot.spec_builder / registry) — these are the same seams
test_copilot_spec_builder.py / test_copilot_registry.py exercise directly;
here we're testing the HTTP wiring, not re-testing that logic.
"""

from fastapi.testclient import TestClient

import systems.copilot.registry as registry_mod
from datastore.api.main import app
from datastore.api.routers import copilot as copilot_router
from systems.copilot.llm_client import LLMConfigError
from systems.copilot.strategy_spec import StrategySpec

client = TestClient(app)


def test_query_returns_503_when_llm_not_configured(monkeypatch):
    def _raise(*a, **kw):
        raise LLMConfigError("OPENROUTER_API_KEY is not set.")

    monkeypatch.setattr(copilot_router.spec_builder, "build_spec", _raise)

    response = client.post("/api/v1/copilot/query", json={"text": "stocks with RSI under 30"})

    assert response.status_code == 503


def test_query_returns_spec_on_success(monkeypatch):
    monkeypatch.setattr(
        copilot_router.spec_builder,
        "build_spec",
        lambda text: StrategySpec(name="RSI Dip", description="", source_query=text),
    )

    response = client.post("/api/v1/copilot/query", json={"text": "stocks with RSI under 30"})

    assert response.status_code == 200
    assert response.json()["name"] == "RSI Dip"


def test_dedup_no_match():
    payload = {"name": "Something New", "technical": [{"feature": "rsi_14", "op": "lt", "value": 12345}]}
    response = client.post("/api/v1/copilot/dedup", json=payload)

    assert response.status_code == 200
    assert response.json()["matched"] is False


def test_save_and_list_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_mod, "STRATEGIES_DIR", tmp_path)

    save_response = client.post(
        "/api/v1/copilot/save",
        json={"spec": {"name": "My Saved Strategy", "description": "", "source_query": ""}},
    )
    assert save_response.status_code == 200
    assert save_response.json()["slug"] == "my-saved-strategy"

    list_response = client.get("/api/v1/copilot/strategies")
    assert list_response.status_code == 200
    names = [s["name"] for s in list_response.json()["strategies"]]
    assert "My Saved Strategy" in names
