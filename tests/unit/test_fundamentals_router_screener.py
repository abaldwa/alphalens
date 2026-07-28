"""
tests/unit/test_fundamentals_router_screener.py

Coverage for datastore/api/routers/fundamentals.py's screener/sector/peers/
scores endpoints (GET /screener, /sector/{sector}, /{ticker}/peers,
/{ticker}/scores) — previously untested (contiguous 0%-covered block).
Real feature-store Parquet files (redirects
datastore.api.utils.feature_store.FEATURES_DAILY_DIR to a tmp dir — that
module's read_feature_day/resolve_date look up their OWN module-global
FEATURES_DAILY_DIR at call time regardless of which router imported them,
so this monkeypatch affects the real read path), real TestClient, no
mocked business logic.
"""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from datastore.api import main as api_main
from datastore.api.utils import feature_store
from features.fundamental import RATIO_FEATURES, STALENESS_FEATURES
from features.governance import GOVERNANCE_FEATURES


@pytest.fixture
def feature_day(tmp_path, monkeypatch):
    monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)

    row = {"ticker": "GOODCO", "sector": "IT"}
    row.update({c: 1.5 for c in RATIO_FEATURES})
    row.update({c: 0 for c in STALENESS_FEATURES})
    row.update({c: 10.0 for c in GOVERNANCE_FEATURES})
    row2 = dict(row)
    row2["ticker"] = "PEERCO"
    df = pd.DataFrame([row, row2])
    df.to_parquet(tmp_path / "2024-06-14.parquet", index=False)
    return "2024-06-14"


@pytest.fixture
def client():
    return TestClient(api_main.app)


def _fake_universe():
    return pd.DataFrame({
        "ticker": ["GOODCO", "PEERCO"], "sector": ["IT", "IT"],
        "market_cap_cr": [50000.0, 40000.0], "adtv_cr": [50.0, 50.0],
    })


class TestScreenerEndpoint:
    def test_unknown_preset_returns_400(self, client):
        resp = client.get("/api/v1/fundamentals/screener", params={"preset": "not_a_real_preset"})
        assert resp.status_code == 400

    def test_no_feature_day_returns_empty_response(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)  # empty dir, no parquet
        resp = client.get("/api/v1/fundamentals/screener", params={"preset": "quality_compounder"})
        assert resp.status_code == 200
        assert resp.json()["tickers"] == []

    def test_real_feature_day_returns_filtered_tickers(self, client, feature_day, monkeypatch):
        monkeypatch.setattr("datastore.api.routers.fundamentals.load_universe_raw", _fake_universe)
        monkeypatch.setattr("config.training_universe.load_universe_raw", _fake_universe)
        resp = client.get("/api/v1/fundamentals/screener", params={"preset": "quality_compounder"})
        assert resp.status_code == 200
        assert resp.json()["date"] == feature_day


class TestSectorEndpoint:
    def test_no_feature_day_returns_empty_response(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)
        resp = client.get("/api/v1/fundamentals/sector/IT")
        assert resp.status_code == 200
        assert resp.json()["ticker_count"] == 0

    def test_sector_with_no_matching_tickers_returns_zero_count(self, client, feature_day, monkeypatch):
        monkeypatch.setattr("datastore.api.routers.fundamentals.load_universe_raw", _fake_universe)
        resp = client.get("/api/v1/fundamentals/sector/Pharma")
        assert resp.status_code == 200
        assert resp.json()["ticker_count"] == 0

    def test_sector_with_matching_tickers_computes_avg_ratios(self, client, feature_day, monkeypatch):
        monkeypatch.setattr("datastore.api.routers.fundamentals.load_universe_raw", _fake_universe)
        resp = client.get("/api/v1/fundamentals/sector/IT")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ticker_count"] == 2
        assert body["avg_ratios"]["revenue_growth_yoy"] == pytest.approx(1.5)


class TestPeersEndpoint:
    def test_no_feature_day_returns_empty_response(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)
        resp = client.get("/api/v1/fundamentals/GOODCO/peers")
        assert resp.status_code == 200
        assert resp.json()["peers"] == []

    def test_real_peers_returned_for_known_ticker(self, client, feature_day, monkeypatch):
        monkeypatch.setattr("datastore.api.routers.fundamentals.load_universe_raw", _fake_universe)
        resp = client.get("/api/v1/fundamentals/GOODCO/peers", params={"k": 5})
        assert resp.status_code == 200
        body = resp.json()
        assert body["sector"] == "IT"


class TestScoresEndpoint:
    def test_no_feature_day_returns_empty_response(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)
        resp = client.get("/api/v1/fundamentals/GOODCO/scores")
        assert resp.status_code == 200
        assert resp.json()["quality_score"] is None

    def test_unknown_ticker_on_a_real_feature_day_returns_unavailable(self, client, feature_day):
        resp = client.get("/api/v1/fundamentals/NOSUCHTICKER/scores")
        assert resp.status_code == 200
        assert resp.json()["quality_score"] is None

    def test_excluded_sector_strategy_scores_are_none_not_a_fabricated_number(
        self, client, tmp_path, monkeypatch,
    ):
        """[BUG FIX, 2026-07-28 second model-review, item 5] GET
        /{ticker}/scores used to compute every SCORE_FUNCTIONS composite
        with no sector filter at all — the only one of the three
        sector-exclusion call sites that skipped PRESET_EXCLUDED_SECTORS.
        A bank/NBFC ticker must get None for magic_formula/moat/qglp/etc
        (ROE/ROCE/leverage-dominated strategies structurally meaningless
        for a lender), not a real-looking but methodologically-invalid
        number, while a genuinely applicable strategy (e.g. "growth",
        which has no sector exclusion) still returns a real score."""
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path)
        row = {"ticker": "BANKCO", "sector": "Financial Services"}
        row.update({c: 1.5 for c in RATIO_FEATURES})
        row.update({c: 0 for c in STALENESS_FEATURES})
        row.update({c: 10.0 for c in GOVERNANCE_FEATURES})
        pd.DataFrame([row]).to_parquet(tmp_path / "2024-06-14.parquet", index=False)

        def _bank_universe():
            return pd.DataFrame({
                "ticker": ["BANKCO"], "sector": ["Financial Services"],
                "market_cap_cr": [50000.0], "adtv_cr": [50.0],
            })

        monkeypatch.setattr("datastore.api.routers.fundamentals.load_universe_raw", _bank_universe)

        resp = client.get("/api/v1/fundamentals/BANKCO/scores")
        assert resp.status_code == 200
        body = resp.json()
        assert body["strategy_scores"]["magic_formula"] is None
        assert body["strategy_scores"]["moat"] is None
        assert body["strategy_scores"]["quality_value"] is None
        # A strategy with no sector exclusion still computes a real score.
        assert body["strategy_scores"]["growth"] is not None
