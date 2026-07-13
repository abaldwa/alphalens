"""
tests/unit/test_ops_exception_catalog_endpoint.py

Phase: Pipeline & Monitoring Remediation, Phase 5
Owner: Platform / DataStore
Consumers: CI, pytest

Exercises GET /api/v1/ops/exception-catalog against the real FastAPI app.
"""

from fastapi.testclient import TestClient

from datastore.api.main import app
from ingestion.scheduler.exception_catalog import all_entries


def test_get_ops_exception_catalog_returns_every_entry():
    client = TestClient(app)
    response = client.get("/api/v1/ops/exception-catalog")
    assert response.status_code == 200
    entries = response.json()["entries"]
    assert len(entries) == len(all_entries())
    assert {e["location"] for e in entries} == {e.location for e in all_entries()}
