"""
Unit tests for Momentum Strategy Configs API endpoints.
Tests CRUD operations and historical returns lookup.
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import momentum as momentum_router
from datastore.schema import create_normalised


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """Test client with temp DB file - module scope to share DB across tests.

    Uses a manually-managed MonkeyPatch since the builtin `monkeypatch`
    fixture is function-scoped and can't be requested from a module-scoped
    fixture.
    """
    db_path = tmp_path_factory.mktemp("db") / "test_normalised.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    mp = pytest.MonkeyPatch()
    mp.setattr(momentum_router, "DUCKDB_PATH", db_path)
    yield TestClient(app)
    mp.undo()
    close_all_connections()


class TestMomentumStrategyConfigs:
    """Tests for /api/v1/momentum/configs endpoints."""

    def test_create_config(self, client):
        """Test creating a new strategy config."""
        config = {
            "band_id": 3,
            "category": "balanced",
            "lookback_months": 6,
            "top_n": 15,
            "grace_period": 2,
            "rebalance_frequency": "monthly",
            "exit_rank": 20,
            "trailing_stop_pct": 15.0,
            "downtrend_filter_pct": 10.0,
            "hmm_regime_filter": "bearish",
            "initial_capital": 1000000,
            "sip_amount": 50000,
            "start_date": "2024-01-01",
            "rebalance_day_of_month": 1,
            "portfolio_id": 1,
        }
        response = client.post("/api/v1/momentum/configs", json=config)
        assert response.status_code == 200
        data = response.json()
        assert data["config_id"] == 1
        assert data["band_id"] == 3
        assert data["category"] == "balanced"
        assert data["lookback_months"] == 6
        assert data["top_n"] == 15
        assert data["grace_period"] == 2
        assert data["rebalance_frequency"] == "monthly"
        assert data["exit_rank"] == 20
        assert data["trailing_stop_pct"] == 15.0
        assert data["downtrend_filter_pct"] == 10.0
        assert data["hmm_regime_filter"] == "bearish"
        assert data["initial_capital"] == 1000000
        assert data["sip_amount"] == 50000
        assert data["start_date"] == "2024-01-01"
        assert data["rebalance_day_of_month"] == 1
        assert data["portfolio_id"] == 1
        assert data["is_active"] is True
        assert "created_at" in data
        assert "updated_at" in data

    def test_create_multiple_categories(self, client):
        """Test creating configs for multiple categories at once."""
        # The API creates one config per category
        config = {
            "band_id": 2,
            "category": "all_risk",
            "lookback_months": 9,
            "top_n": 10,
            "grace_period": 1,
            "rebalance_frequency": "biweekly",
            "initial_capital": 500000,
            "sip_amount": 0,
            "start_date": "2024-04-01",
        }
        response = client.post("/api/v1/momentum/configs", json=config)
        assert response.status_code == 200
        data = response.json()
        assert data["band_id"] == 2
        assert data["category"] == "all_risk"

    def test_list_configs(self, client):
        """Test listing all configs."""
        response = client.get("/api/v1/momentum/configs")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 2  # At least the two we created

    def test_list_configs_with_filters(self, client):
        """Test listing configs with filters."""
        # Filter by band_id
        response = client.get("/api/v1/momentum/configs?band_id=3")
        assert response.status_code == 200
        data = response.json()
        assert all(c["band_id"] == 3 for c in data)

        # Filter by category
        response = client.get("/api/v1/momentum/configs?category=balanced")
        assert response.status_code == 200
        data = response.json()
        assert all(c["category"] == "balanced" for c in data)

        # Filter by is_active
        response = client.get("/api/v1/momentum/configs?is_active=true")
        assert response.status_code == 200
        data = response.json()
        assert all(c["is_active"] is True for c in data)

    def test_get_config(self, client):
        """Test getting a single config by ID."""
        response = client.get("/api/v1/momentum/configs/1")
        assert response.status_code == 200
        data = response.json()
        assert data["config_id"] == 1
        assert data["band_id"] == 3
        assert data["category"] == "balanced"

    def test_get_nonexistent_config(self, client):
        """Test getting a non-existent config returns 404."""
        response = client.get("/api/v1/momentum/configs/999")
        assert response.status_code == 404

    def test_update_config(self, client):
        """Test updating a config."""
        update = {
            "initial_capital": 2000000,
            "sip_amount": 100000,
            "is_active": False,
        }
        response = client.put("/api/v1/momentum/configs/1", json=update)
        assert response.status_code == 200
        data = response.json()
        assert data["initial_capital"] == 2000000
        assert data["sip_amount"] == 100000
        assert data["is_active"] is False

    def test_update_nonexistent_config(self, client):
        """Test updating a non-existent config returns 404."""
        response = client.put("/api/v1/momentum/configs/999", json={"initial_capital": 100})
        assert response.status_code == 404

    def test_update_config_no_fields(self, client):
        """Test updating with no fields returns 400."""
        response = client.put("/api/v1/momentum/configs/1", json={})
        assert response.status_code == 400

    def test_delete_config(self, client):
        """Test soft deleting a config."""
        response = client.delete("/api/v1/momentum/configs/2")
        assert response.status_code == 200
        assert response.json()["deleted"] is True

        # Verify it's soft deleted
        response = client.get("/api/v1/momentum/configs/2")
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False

    def test_delete_nonexistent_config(self, client):
        """Test deleting a non-existent config returns 404."""
        response = client.delete("/api/v1/momentum/configs/999")
        assert response.status_code == 404

    def test_unique_constraint(self, client):
        """Test that duplicate configs are rejected."""
        config = {
            "band_id": 3,
            "category": "balanced",
            "lookback_months": 6,
            "top_n": 15,
            "grace_period": 2,
            "rebalance_frequency": "monthly",
            "initial_capital": 100000,
            "sip_amount": 0,
            "start_date": "2024-01-01",
        }
        # First create should succeed
        response = client.post("/api/v1/momentum/configs", json=config)
        assert response.status_code == 200

        # Second create with same params should fail
        response = client.post("/api/v1/momentum/configs", json=config)
        assert response.status_code == 400 or response.status_code == 500  # DuckDB constraint error


class TestMomentumConfigReturns:
    """Tests for /api/v1/momentum/configs/{config_id}/returns endpoint."""

    def test_get_returns_no_dynamic_report(self, client):
        """Test returns endpoint when no dynamic report exists."""
        response = client.get("/api/v1/momentum/configs/1/returns")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    def test_get_returns_nonexistent_config(self, client):
        """Test returns for non-existent config returns 404."""
        response = client.get("/api/v1/momentum/configs/999/returns")
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
