"""
tests/unit/test_training_universe.py

A65: pure-logic tests for `config/training_universe.py`, previously
untested (57.38% coverage, no test file). Real pandas DataFrames + real
tmp_path JSON snapshots via monkeypatched TRAINING_UNIVERSE_DIR — no mocks,
no network.
"""

import json
from datetime import date

import pandas as pd
import pytest

from config import training_universe as tu


@pytest.fixture
def universe_dir(tmp_path, monkeypatch):
    d = tmp_path / "training_universe"
    monkeypatch.setattr(tu, "TRAINING_UNIVERSE_DIR", d)
    return d


def _universe(rows):
    return pd.DataFrame(rows)


class TestBuildTrainingUniverse:
    def test_ranks_by_adtv_descending_and_applies_floor(self, universe_dir):
        universe = _universe(
            [
                {"ticker": "A", "adtv_cr": 100.0}, {"ticker": "B", "adtv_cr": 50.0},
                {"ticker": "C", "adtv_cr": 10.0},
            ]
        )
        result = tu.build_training_universe(as_of=date(2026, 7, 1), universe=universe)
        assert result == ["A", "B"]

    def test_max_size_cap_applied(self, universe_dir):
        universe = _universe([{"ticker": t, "adtv_cr": 100.0 - i} for i, t in enumerate(["A", "B", "C"])])
        result = tu.build_training_universe(as_of=date(2026, 7, 1), universe=universe, max_size=2)
        assert result == ["A", "B"]

    def test_hysteresis_keeps_previously_qualified_ticker(self, universe_dir):
        tu.save_training_universe(["B"], as_of=date(2026, 6, 24))
        universe = _universe(
            [{"ticker": "A", "adtv_cr": 100.0}, {"ticker": "B", "adtv_cr": 35.0}]
        )
        # B is below adtv_floor_cr (40) but above hysteresis_floor_cr (32) and was in
        # last week's list -> kept
        result = tu.build_training_universe(
            as_of=date(2026, 7, 1), universe=universe, adtv_floor_cr=40.0, hysteresis_floor_cr=32.0,
        )
        assert set(result) == {"A", "B"}

    def test_hysteresis_does_not_save_ticker_below_hysteresis_floor(self, universe_dir):
        tu.save_training_universe(["B"], as_of=date(2026, 6, 24))
        universe = _universe(
            [{"ticker": "A", "adtv_cr": 100.0}, {"ticker": "B", "adtv_cr": 10.0}]
        )
        result = tu.build_training_universe(
            as_of=date(2026, 7, 1), universe=universe, adtv_floor_cr=40.0, hysteresis_floor_cr=32.0,
        )
        assert result == ["A"]

    def test_no_prior_snapshot_uses_floor_only(self, universe_dir):
        universe = _universe([{"ticker": "A", "adtv_cr": 41.0}, {"ticker": "B", "adtv_cr": 39.0}])
        result = tu.build_training_universe(as_of=date(2026, 7, 1), universe=universe, adtv_floor_cr=40.0)
        assert result == ["A"]


class TestSaveAndLoadTrainingUniverse:
    def test_save_creates_versioned_json_file(self, universe_dir):
        path = tu.save_training_universe(["A", "B"], as_of=date(2026, 7, 1))
        assert path.exists()
        payload = json.loads(path.read_text())
        assert payload["tickers"] == ["A", "B"]
        assert payload["count"] == 2
        assert payload["as_of"] == "2026-07-01"

    def test_load_current_returns_latest_snapshot(self, universe_dir):
        tu.save_training_universe(["A"], as_of=date(2026, 6, 1))
        tu.save_training_universe(["A", "B"], as_of=date(2026, 7, 1))
        assert tu.load_current_training_universe() == ["A", "B"]

    def test_load_current_builds_fresh_when_none_exists(self, universe_dir, monkeypatch):
        universe = _universe([{"ticker": "A", "adtv_cr": 100.0}])
        monkeypatch.setattr(tu, "load_universe_raw", lambda: universe)
        result = tu.load_current_training_universe()
        assert result == ["A"]
        # Confirms refresh_training_universe() actually persisted a snapshot.
        assert list(universe_dir.glob("training_universe_v*.json"))


class TestRefreshTrainingUniverse:
    def test_builds_and_saves(self, universe_dir, monkeypatch):
        universe = _universe([{"ticker": "A", "adtv_cr": 100.0}])
        monkeypatch.setattr(tu, "load_universe_raw", lambda: universe)
        tickers = tu.refresh_training_universe(as_of=date(2026, 7, 1))
        assert tickers == ["A"]
        assert (universe_dir / "training_universe_v20260701.json").exists()


class TestIsRecommendable:
    def test_above_floor_is_recommendable(self):
        assert tu.is_recommendable(25.0) is True

    def test_below_floor_is_not_recommendable(self):
        assert tu.is_recommendable(10.0) is False

    def test_none_is_not_recommendable(self):
        assert tu.is_recommendable(None) is False

    def test_custom_floor(self):
        assert tu.is_recommendable(15.0, floor_cr=10.0) is True


class TestFilterRecommendable:
    def test_drops_sub_floor_tickers(self):
        universe = _universe([{"ticker": "A", "adtv_cr": 100.0}, {"ticker": "B", "adtv_cr": 1.0}])
        df = pd.DataFrame({"ticker": ["A", "B"], "score": [1, 2]})
        result = tu.filter_recommendable(df, universe=universe)
        assert list(result["ticker"]) == ["A"]

    def test_ticker_not_in_universe_dropped(self):
        universe = _universe([{"ticker": "A", "adtv_cr": 100.0}])
        df = pd.DataFrame({"ticker": ["A", "UNKNOWN"], "score": [1, 2]})
        result = tu.filter_recommendable(df, universe=universe)
        assert list(result["ticker"]) == ["A"]

    def test_custom_ticker_col(self):
        universe = _universe([{"ticker": "A", "adtv_cr": 100.0}])
        df = pd.DataFrame({"sym": ["A"], "score": [1]})
        result = tu.filter_recommendable(df, ticker_col="sym", universe=universe)
        assert list(result["sym"]) == ["A"]
