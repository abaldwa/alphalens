"""
tests/unit/test_score_multibagger.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-MODEL-001, SPEC-UI-003
Owner: Platform / QA
Consumers: CI, pytest

Tests systems/ml_signal_engine/inference/score_multibagger.py's
orchestration (OHLCV panel fetch, no-data handling, write gating,
survival-column renaming) with a mocked DataStoreClient and a pre-trained
MultibaggerModel — entirely offline. The underlying building blocks
(compute_multibagger_features, MultibaggerModel itself) already have
their own dedicated test coverage (test_multibagger.py).
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.inference.score_multibagger import (
    _fetch_benchmark_wide,
    _fetch_ohlcv_panel,
    _none_if_nan,
    score_universe,
)
from systems.ml_signal_engine.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)


def _mock_client_ohlcv_rows(n_days: int = 760, start_price: float = 100.0) -> list:
    rows = []
    price = start_price
    for i in range(n_days):
        price *= 1.0005
        date = (pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)).date().isoformat()
        rows.append({"date": date, "open": price, "high": price * 1.01, "low": price * 0.99,
                     "close": price, "volume": 1_000_000})
    return rows


# Small real-ticker sample, not the full ~2,300-ticker universe — bounds
# memory/runtime for this module's training fixture (see
# tests/unit/test_multibagger.py's _TEST_TICKER_SAMPLE for the same rationale).
_TEST_TICKER_SAMPLE = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "BAJFINANCE", "MARUTI", "SUNPHARMA",
]


@pytest.fixture(scope="module")
def trained_model() -> MultibaggerModel:
    try:
        X, y, duration, event, groups, _pnd = load_multibagger_training_data_from_db(
            tickers=_TEST_TICKER_SAMPLE,
        )
    except RuntimeError as exc:
        pytest.skip(f"real multibagger training data not yet available: {exc}")
    model = MultibaggerModel(random_state=2, n_estimators=30)
    model.train_full(X, y, duration, event, groups=groups)
    return model


class TestNoneIfNan:
    def test_nan_becomes_none(self):
        assert _none_if_nan(np.nan) is None

    def test_real_value_passes_through(self):
        assert _none_if_nan(0.42) == 0.42


class TestFetchOhlcvPanel:
    def test_one_failed_ticker_does_not_abort_fetch(self):
        client = MagicMock()
        client.get_ohlcv.side_effect = lambda ticker, *a, **kw: (
            (_ for _ in ()).throw(ConnectionError("boom")) if ticker == "BADCO" else _mock_client_ohlcv_rows(10)
        )
        panel = _fetch_ohlcv_panel(client, ["GOODCO", "BADCO"], pd.Timestamp("2026-06-23"))
        assert set(panel["ticker"].unique()) == {"GOODCO"}

    def test_no_data_anywhere_returns_empty_frame_not_error(self):
        client = MagicMock()
        client.get_ohlcv.return_value = []
        panel = _fetch_ohlcv_panel(client, ["EMPTYCO"], pd.Timestamp("2026-06-23"))
        assert panel.empty


class TestFetchBenchmarkWide:
    def test_missing_benchmark_returns_none_not_error(self):
        client = MagicMock()
        client.get_ohlcv.return_value = []
        assert _fetch_benchmark_wide(client, pd.Timestamp("2026-06-23")) is None


class TestScoreUniverse:
    def test_no_ohlcv_data_marks_every_ticker_failed(self, trained_model):
        client = MagicMock()
        client.get_ohlcv.return_value = []
        results = score_universe(["NODATACO"], client=client, model=trained_model, write=False)
        assert results == {"NODATACO": False}

    def test_real_panel_scores_and_writes(self, trained_model, monkeypatch):
        client = MagicMock()
        client.get_ohlcv.side_effect = lambda ticker, *a, **kw: _mock_client_ohlcv_rows(760)
        with pd.option_context("mode.chained_assignment", None):
            results = score_universe(["GOODCO"], client=client, model=trained_model, write=True)

        assert results == {"GOODCO": True}
        client.write_multibagger_score.assert_called_once()
        written = client.write_multibagger_score.call_args[0][0]
        assert written["ticker"] == "GOODCO"
        assert 0.0 <= written["mb_probability"] <= 1.0
        assert "survival_18m" in written  # the P2.6-added 5th survival horizon

    def test_write_false_skips_api_calls(self, trained_model):
        client = MagicMock()
        client.get_ohlcv.side_effect = lambda ticker, *a, **kw: _mock_client_ohlcv_rows(760)
        results = score_universe(["GOODCO"], client=client, model=trained_model, write=False)

        assert results == {"GOODCO": True}
        client.write_multibagger_score.assert_not_called()

    def test_default_model_trains_when_none_injected(self, tmp_path, monkeypatch):
        """score_universe() must work with no injected model (the real CLI path).

        [backlog #27] score_universe now prefers a cached MultibaggerModel
        artifact (MODELS_DIR/multibagger/multibagger_current.pkl) over
        training inline — point MODELS_DIR at an empty tmp_path so this
        test exercises the fresh-train fallback, not a stray cached model
        left over from a real training run on this machine.
        """
        try:
            real_data = load_multibagger_training_data_from_db(tickers=_TEST_TICKER_SAMPLE)
        except RuntimeError as exc:
            pytest.skip(f"real multibagger training data not yet available: {exc}")

        monkeypatch.setattr("config.settings.MODELS_DIR", tmp_path)

        client = MagicMock()
        client.get_ohlcv.side_effect = lambda ticker, *a, **kw: _mock_client_ohlcv_rows(760)
        with patch(
            "systems.ml_signal_engine.inference.score_multibagger.load_multibagger_training_data_from_db",
            return_value=real_data,
        ):
            results = score_universe(["GOODCO"], client=client, write=False)
        assert results == {"GOODCO": True}

    def test_cached_model_artifact_is_loaded_without_retraining(self, tmp_path, monkeypatch):
        """[backlog #27] A cached artifact at MODELS_DIR/multibagger/multibagger_current.pkl
        must be loaded directly, without calling load_multibagger_training_data_from_db."""
        from systems.ml_signal_engine.inference.score_multibagger import _load_cached_model_or_train_fresh

        try:
            real_data = load_multibagger_training_data_from_db(tickers=_TEST_TICKER_SAMPLE)
        except RuntimeError as exc:
            pytest.skip(f"real multibagger training data not yet available: {exc}")

        X, y, duration, event, groups, _pnd = real_data
        seed_model = MultibaggerModel(random_state=9, n_estimators=10)
        seed_model.train_full(X, y, duration, event, groups=groups)

        model_dir = tmp_path / "multibagger"
        model_dir.mkdir()
        seed_model.save(str(model_dir / "multibagger_current.pkl"))

        monkeypatch.setattr("config.settings.MODELS_DIR", tmp_path)
        with patch(
            "systems.ml_signal_engine.inference.score_multibagger.load_multibagger_training_data_from_db"
        ) as mock_train:
            loaded = _load_cached_model_or_train_fresh()
            mock_train.assert_not_called()
        assert loaded._rsf is not None
