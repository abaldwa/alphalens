"""
tests/integration/test_daily_pipeline.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-MODEL-006, SPEC-DS-002, SPEC-DS-004, SPEC-PIPE-005, SPEC-SYS-002
Owner: Platform / QA
Consumers: CI, pytest

Full end-to-end integration test: trains small/fast versions of every
Phase 1 model, runs systems/ml_signal_engine/inference/daily_inference.py's
run_daily_inference() for 5 synthetic tickers across 3 dates against a
REAL in-process DataStore FastAPI app (httpx.ASGITransport — no real TCP
port, but the genuine FastAPI routing + DuckDB read/write code paths, not
mocks), then verifies the written signals are readable back through the
API exactly as a real consumer (the dashboard, a future portfolio system)
would read them.

Uses temp DuckDB/SQLite files (never the real datastore/ directory) by
monkeypatching each router module's own already-imported SIGNALS_DUCKDB_PATH
binding directly — config.settings.SIGNALS_DUCKDB_PATH itself can't be
monkeypatched after the fact, since `from config.settings import
SIGNALS_DUCKDB_PATH` (used at the top of every router module) created an
independent local binding at import time.

[AS BUILT] A real uvicorn server runs in a background thread on a free
local port, rather than httpx.ASGITransport — this httpx version's
ASGITransport only implements the async transport interface
(handle_async_request), but daily_inference.py's run_daily_inference()
makes synchronous httpx.Client calls (matching the rest of this
synchronous-style codebase), so a sync-compatible transport is needed.
A real bound socket is still no real risk here (loopback-only, random
free port, torn down at the end of each test module).
"""

import socket
import threading
import time
import warnings
from datetime import date

import httpx
import numpy as np
import pandas as pd
import pytest
import uvicorn

from datastore.api.routers import alerts as alerts_router
from datastore.api.routers import regime as regime_router
from datastore.api.routers import signals as signals_router
from datastore.schema.create_signals import create_signal_tables_schema
from features.pnd_features import PND_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.inference.daily_inference import run_daily_inference

warnings.filterwarnings("ignore")

TICKERS = [f"INTEG{i:02d}" for i in range(5)]
DATES = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def temp_signals_db(tmp_path_factory):
    """Real ml_signals DuckDB table at a temp path, wired into every router that touches it."""
    db_path = tmp_path_factory.mktemp("signals_db") / "signals.duckdb"
    create_signal_tables_schema(db_path=db_path)
    signals_router.SIGNALS_DUCKDB_PATH = db_path
    regime_router.SIGNALS_DUCKDB_PATH = db_path
    alerts_router.SIGNALS_DUCKDB_PATH = db_path
    return db_path


@pytest.fixture(scope="module")
def api_base_url(temp_signals_db):
    """A real uvicorn server on loopback, backed by the temp DuckDB file above, in a background thread."""
    from datastore.api.main import app

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("test uvicorn server did not start in time")

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def api_client(api_base_url):
    client = httpx.Client(base_url=api_base_url)
    yield client
    client.close()


@pytest.fixture(scope="module")
def trained_models(tmp_path_factory):
    """Small/fast trained PnDDetector, Signal5DModel, MetaLabeler, ExitSignalModel, HMM."""
    import joblib

    from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
    from systems.ml_signal_engine.models.exit.exit_signal import generate_synthetic_training_data as exit_synth
    from systems.ml_signal_engine.models.hmm.regime_detector import HMMRegimeDetector, compute_hmm_observables
    from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
    from systems.ml_signal_engine.models.pnd.pnd_detector import generate_synthetic_training_data as pnd_synth
    from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
    from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

    models_dir = tmp_path_factory.mktemp("models")
    rng = np.random.default_rng(7)

    # HMM
    dates = pd.bdate_range("2022-01-01", periods=200)
    close = 100 * np.cumprod(1 + rng.normal(0.0003, 0.012, len(dates)))
    market_ohlcv = pd.DataFrame(
        {
            "date": dates, "ticker": "NIFTY", "open": close, "high": close * 1.01, "low": close * 0.99,
            "close": close, "volume": rng.integers(1_000_000, 5_000_000, len(dates)).astype(float),
        }
    )
    # [AS BUILT] directory is "hmm" (model type), filename prefix is
    # "hmm_market" (specific name) — matches train_all_phase1.py's real
    # save path, not the name used for both. A live pipeline run against
    # real data caught this fixture matching daily_inference.py's
    # since-fixed _load_hmm() bug instead of the real on-disk convention.
    hmm = HMMRegimeDetector(random_state=7)
    hmm.fit(compute_hmm_observables(market_ohlcv))
    (models_dir / "hmm").mkdir(parents=True)
    joblib.dump(hmm, models_dir / "hmm" / "hmm_market_current.pkl")

    # P&D — trained on its own synthetic archive (independent of test universe)
    X_pnd, y_pnd = pnd_synth(n_positive=15, n_negative=235, n_days=90, seed=7)
    pnd = PnDDetector(random_state=7)
    pnd.train(X_pnd, y_pnd)
    (models_dir / "pnd_detector").mkdir(parents=True)
    pnd.save(str(models_dir / "pnd_detector" / "pnd_detector_current.pkl"))

    # Signal5D + MetaLabeler
    n = 300
    X_sig = pd.DataFrame(rng.normal(size=(n, len(CORE_TECHNICAL_FEATURES))), columns=CORE_TECHNICAL_FEATURES)
    score = X_sig.iloc[:, 0] - 0.5 * X_sig.iloc[:, 1] + rng.normal(scale=0.5, size=n)
    y_sig = pd.Series(np.where(score > 0.5, 1, np.where(score < -0.5, -1, 0)))
    returns = pd.Series(score * 0.02 + rng.normal(scale=0.01, size=n))
    signal_model = Signal5DModel(optuna_trials=2, random_state=7)
    signal_model.train_full(
        X_sig.iloc[:220], y_sig.iloc[:220], X_sig.iloc[220:], y_sig.iloc[220:],
        returns_train=returns.iloc[:220], returns_val=returns.iloc[220:],
    )
    (models_dir / "signal_5d").mkdir(parents=True)
    signal_model.save(str(models_dir / "signal_5d" / "signal_5d_current.pkl"))

    direction = signal_model.predict(X_sig)
    meta_labels = MetaLabeler.compute_labels(direction, returns)
    mask = meta_labels.notna()
    meta = MetaLabeler(random_state=7)
    meta.train(X_sig[mask], meta_labels[mask])
    (models_dir / "meta_labeler").mkdir(parents=True)
    meta.save(str(models_dir / "meta_labeler" / "meta_labeler_current.pkl"))

    # Exit
    X_exit, urgency, exit_type, duration, event = exit_synth(n=250, seed=7)
    exit_model = ExitSignalModel(random_state=7)
    exit_model.train_full(X_exit, urgency, exit_type, duration, event)
    (models_dir / "exit_signal").mkdir(parents=True)
    exit_model.save(str(models_dir / "exit_signal" / "exit_signal_current.pkl"))

    return models_dir, market_ohlcv


def _synthetic_inputs(run_date: date, seed: int):
    rng = np.random.default_rng(seed)
    feature_matrix = pd.DataFrame(
        rng.normal(size=(len(TICKERS), len(CORE_TECHNICAL_FEATURES))), columns=CORE_TECHNICAL_FEATURES
    )
    feature_matrix.insert(0, "ticker", TICKERS)

    pnd_feature_matrix = pd.DataFrame(
        rng.normal(scale=0.1, size=(len(TICKERS), len(PND_FEATURES))), columns=PND_FEATURES
    )
    pnd_feature_matrix.insert(0, "ticker", TICKERS)
    return feature_matrix, pnd_feature_matrix


class TestDailyPipelineEndToEnd:
    """Build prompt: 'Full end-to-end test on 5 stocks for 3 dates.'"""

    def test_runs_without_error_across_three_dates(self, api_base_url, trained_models):
        models_dir, market_ohlcv = trained_models

        for i, run_date in enumerate(DATES):
            feature_matrix, pnd_feature_matrix = _synthetic_inputs(run_date, seed=100 + i)
            result = run_daily_inference(
                run_date=run_date,
                feature_matrix=feature_matrix,
                pnd_feature_matrix=pnd_feature_matrix,
                market_ohlcv=market_ohlcv,
                api_base_url=api_base_url,
                models_dir=models_dir,
                psi_baseline={},
            )
            assert result["halted"] is False, result.get("halt_reason")
            assert result["regime"] is not None

    def test_signals_written_are_readable_via_api(self, api_base_url, api_client, trained_models):
        """Build prompt: 'Verify signals written to DataStore are readable via API.'"""
        models_dir, market_ohlcv = trained_models
        run_date = DATES[0]
        feature_matrix, pnd_feature_matrix = _synthetic_inputs(run_date, seed=200)

        run_daily_inference(
            run_date=run_date, feature_matrix=feature_matrix, pnd_feature_matrix=pnd_feature_matrix,
            market_ohlcv=market_ohlcv, api_base_url=api_base_url,
            models_dir=models_dir, psi_baseline={},
        )

        for ticker in TICKERS:
            response = api_client.get(f"/api/v1/signals/ml/{ticker}/{run_date.isoformat()}")
            assert response.status_code == 200
            rows = response.json()
            # Every ticker gets at least a pnd_detector row (written for every
            # ticker regardless of block status); signal_5d only for non-blocked ones.
            model_names = {row["model_name"] for row in rows}
            assert "pnd_detector" in model_names

        regime_response = api_client.get("/api/v1/macro/regime")
        assert regime_response.status_code == 200
        assert regime_response.json()["available"] is True


class TestPnDBlockExcludedFromTopBuys:
    """
    Build prompt: 'Verify P&D block prevents signal from appearing in
    top_buys endpoint.' Deterministic and isolated from PnDDetector's own
    trained-model behavior (which other tests in this module already cover
    indirectly): write a buy signal AND a P&D-block row for the same
    ticker directly via the API's write endpoint (SPEC-DS-004), then
    assert top_buys excludes it (SPEC-MODEL-006).
    """

    def test_pnd_blocked_ticker_excluded_from_top_buys(self, api_client):
        run_date = date(2024, 7, 1)

        api_client.post(
            "/api/v1/signals/ml/write",
            json={
                "date": run_date.isoformat(), "ticker": "CLEAN01", "model_name": "signal_5d",
                "model_version": "1.0", "signal_direction": "buy", "buy_prob": 0.70,
                "hold_prob": 0.2, "sell_prob": 0.1,
            },
        ).raise_for_status()
        api_client.post(
            "/api/v1/signals/ml/write",
            json={
                "date": run_date.isoformat(), "ticker": "PUMPED01", "model_name": "signal_5d",
                "model_version": "1.0", "signal_direction": "buy", "buy_prob": 0.99,
                "hold_prob": 0.005, "sell_prob": 0.005,
            },
        ).raise_for_status()
        api_client.post(
            "/api/v1/signals/ml/write",
            json={
                "date": run_date.isoformat(), "ticker": "PUMPED01", "model_name": "pnd_detector",
                "model_version": "1.0", "pnd_score": 85.0, "pnd_phase": "distribution", "pnd_block": True,
            },
        ).raise_for_status()

        response = api_client.get(f"/api/v1/signals/ml/top_buys/{run_date.isoformat()}")
        assert response.status_code == 200
        top_buys = response.json()
        tickers_returned = [row["ticker"] for row in top_buys]

        assert "PUMPED01" not in tickers_returned, "P&D-blocked ticker must never appear in top_buys"
        assert "CLEAN01" in tickers_returned

    def test_pnd_blocked_ticker_still_readable_via_per_ticker_endpoint(self, api_client):
        """A blocked ticker is excluded from top_buys but its P&D row is still queryable directly (audit trail)."""
        run_date = date(2024, 7, 2)
        api_client.post(
            "/api/v1/signals/ml/write",
            json={
                "date": run_date.isoformat(), "ticker": "PUMPED02", "model_name": "pnd_detector",
                "model_version": "1.0", "pnd_score": 90.0, "pnd_phase": "distribution", "pnd_block": True,
            },
        ).raise_for_status()

        response = api_client.get(f"/api/v1/signals/ml/PUMPED02/{run_date.isoformat()}")
        assert response.status_code == 200
        rows = response.json()
        assert len(rows) == 1
        assert rows[0]["pnd_block"] is True
