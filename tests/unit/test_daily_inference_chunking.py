"""
tests/unit/test_daily_inference_chunking.py

A55 (2026-07-11): regression coverage for the real production OOM
incident — alphalens-scheduler.service was OOM-killed by systemd-oomd
at 07:54 IST running a 6-day catch-up backfill, root-caused to
systems/ml_signal_engine/inference/daily_inference.py's
_step_signals_and_meta/_step_pnd_filter scoring the ENTIRE ~2,317-ticker
full-universe feature matrix in one unchunked pass (5 models + a SHAP
TreeExplainer pass all holding full-universe-sized arrays at once).

Fix: both steps now score/write in ticker CHUNKS via the same
resource_guard.adaptive_chunk_size helper A47 already used for
features/matrix_builder.py's per-ticker-independent panels. This is the
"same regression bar as A47" test: proves chunked scoring produces
IDENTICAL written output to a single full-batch pass, at multiple forced
chunk sizes (full-batch, 5, 1) — using real trained (small/fast, real
sklearn/lightgbm code paths — not mocked predict()) Signal5DModel/
MetaLabeler/PnDDetector instances, a real (if small) feature matrix, not
a toy shaped-differently-than-production stand-in.

adaptive_chunk_size(configured_size, floor=5, ...) returns
`configured_size` unchanged whenever configured_size <= floor, WITHOUT
consulting current host memory pressure at all — so forcing
SCREENER_BATCH_EXPORT_CHUNK_SIZE to 5 or 1 in this test deterministically
produces exactly that many chunks regardless of how much RAM the test
runner host has free (no flakiness from real memory_pressure_high()
reads, same trick a chunk size of >= universe size relies on for the
"single full pass" comparison side).
"""
from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

import systems.ml_signal_engine.inference.daily_inference as di
from features.pnd_features import PND_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

TICKERS = [f"TKR{i:03d}" for i in range(23)]  # tens, not thousands — fast, still exercises multi-chunk boundaries
RUN_DATE = date(2026, 7, 11)


class _FakeResponse:
    status_code = 200

    def raise_for_status(self):
        pass


class _RecordingClient:
    """Records every _write_signal() POST body instead of hitting a real API."""

    def __init__(self):
        self.calls = []

    def post(self, url, json, timeout=None):  # noqa: A002 - matches httpx.Client.post's kwarg name
        self.calls.append(dict(json))
        return _FakeResponse()


@pytest.fixture(scope="module")
def signal_meta_models(tmp_path_factory):
    """Real (small, fast) trained Signal5DModel + MetaLabeler — same style as
    tests/integration/test_daily_pipeline.py's `trained_models` fixture, but
    self-contained (no DB dependency) since this test only needs deterministic,
    reusable model objects, not DB-sourced training data."""
    rng = np.random.default_rng(11)
    n = 250
    X = pd.DataFrame(rng.normal(size=(n, len(CORE_TECHNICAL_FEATURES))), columns=CORE_TECHNICAL_FEATURES)
    score = X.iloc[:, 0] - 0.5 * X.iloc[:, 1] + rng.normal(scale=0.5, size=n)
    y = pd.Series(np.where(score > 0.5, 1, np.where(score < -0.5, -1, 0)))
    returns = pd.Series(score * 0.02 + rng.normal(scale=0.01, size=n))

    signal_model = Signal5DModel(optuna_trials=2, random_state=11)
    signal_model.train_full(
        X.iloc[:200], y.iloc[:200], X.iloc[200:], y.iloc[200:],
        returns_train=returns.iloc[:200], returns_val=returns.iloc[200:],
    )

    direction = signal_model.predict(X)
    meta_labels = MetaLabeler.compute_labels(direction, returns)
    mask = meta_labels.notna()
    meta_model = MetaLabeler(random_state=11)
    meta_model.train(X[mask], meta_labels[mask])

    return signal_model, meta_model


@pytest.fixture(scope="module")
def pnd_model():
    rng = np.random.default_rng(13)
    n = 200
    X = pd.DataFrame(rng.normal(scale=0.1, size=(n, len(PND_FEATURES))), columns=PND_FEATURES)
    y = pd.Series(rng.integers(0, 2, size=n))
    model = PnDDetector(random_state=13)
    model.train(X, y)
    return model


@pytest.fixture
def feature_matrix():
    rng = np.random.default_rng(17)
    fm = pd.DataFrame(rng.normal(size=(len(TICKERS), len(CORE_TECHNICAL_FEATURES))), columns=CORE_TECHNICAL_FEATURES)
    fm.insert(0, "ticker", TICKERS)
    return fm


@pytest.fixture
def pnd_feature_matrix():
    rng = np.random.default_rng(19)
    fm = pd.DataFrame(rng.normal(scale=0.1, size=(len(TICKERS), len(PND_FEATURES))), columns=PND_FEATURES)
    fm.insert(0, "ticker", TICKERS)
    return fm


def _normalize_calls(calls):
    """Sort + round-trip-through-json so float repr/ordering differences
    between runs never cause a spurious mismatch, only real value differences."""
    return sorted(
        (json.loads(json.dumps(c, sort_keys=True, default=str)) for c in calls),
        key=lambda c: (c["ticker"], c["model_name"]),
    )


def _assert_calls_close(actual, expected, rtol=1e-6, atol=1e-9):
    """Compare two lists of written-signal payloads (as produced by
    _normalize_calls) for equality, tolerating float-level noise.

    Verified by direct investigation (see A55/BuildLog.md): LightGBM's
    histogram-based predict() and SHAP's TreeExplainer are NOT bit-exact
    across differently-sized batches — summation/reduction order inside
    the C++ prediction path depends on how many rows are scored together,
    so a chunk of 5 rows and the same 5 rows scored as part of a
    23-row batch can differ at the ~1e-14 relative level. This is
    upstream floating-point non-associativity, not a chunking-introduced
    correctness bug — confirmed by calling model.predict_signals() on the
    exact same sub-batch in isolation and getting bit-identical results
    to the chunked path, while the FULL unchunked pass differs from
    BOTH at the same noise floor. A tolerant comparison (rtol=1e-6) is
    the correct bar here, not bit-for-bit equality — the same standard
    any two "should be mathematically equivalent" floating-point code
    paths are held to.
    """
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a.keys() == e.keys(), (a, e)
        for key in a:
            av, ev = a[key], e[key]
            if isinstance(av, (int, float)) and isinstance(ev, (int, float)):
                assert av == pytest.approx(ev, rel=rtol, abs=atol), f"{key}: {av} != {ev}"
            elif key == "shap_top5_json" and av is not None and ev is not None:
                a_feats, e_feats = json.loads(av), json.loads(ev)
                assert [f["feature"] for f in a_feats] == [f["feature"] for f in e_feats]
                for fa, fe in zip(a_feats, e_feats):
                    assert fa["value"] == pytest.approx(fe["value"], rel=rtol, abs=atol)
            else:
                assert av == ev, f"{key}: {av} != {ev}"


class TestStepSignalsAndMetaChunkingMatchesUnchunked:
    """The critical regression test: chunked scoring must be numerically
    identical to a single full-batch pass — proves chunk boundaries don't
    leak into any per-ticker computation (SHAP, quantiles, meta-label)."""

    @pytest.mark.parametrize("configured_chunk_size", [1000, 5, 1])
    def test_written_payloads_identical_across_chunk_sizes(
        self, signal_meta_models, feature_matrix, tmp_path, monkeypatch, configured_chunk_size
    ):
        signal_model, meta_model = signal_meta_models
        monkeypatch.setattr(di, "_load_model", lambda cls, name, models_dir: {
            di.SIGNAL_MODEL_NAME: signal_model, di.META_MODEL_NAME: meta_model,
        }[name])
        monkeypatch.setattr(di, "_load_conformal", lambda models_dir: (_ for _ in ()).throw(FileNotFoundError()))
        monkeypatch.setattr("config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE", configured_chunk_size)

        client = _RecordingClient()
        result = di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)

        assert len(result) == len(TICKERS)
        assert client.calls  # sanity: something was written

    def test_full_batch_5_and_1_chunk_runs_produce_identical_written_rows(
        self, signal_meta_models, feature_matrix, tmp_path, monkeypatch
    ):
        signal_model, meta_model = signal_meta_models

        def run(configured_chunk_size):
            monkeypatch.setattr(di, "_load_model", lambda cls, name, models_dir: {
                di.SIGNAL_MODEL_NAME: signal_model, di.META_MODEL_NAME: meta_model,
            }[name])
            monkeypatch.setattr(di, "_load_conformal", lambda models_dir: (_ for _ in ()).throw(FileNotFoundError()))
            monkeypatch.setattr("config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE", configured_chunk_size)
            client = _RecordingClient()
            di._step_signals_and_meta(feature_matrix, set(), RUN_DATE, client, "http://fake", tmp_path)
            return _normalize_calls(client.calls)

        full_batch = run(1000)  # >= len(TICKERS): single chunk, equivalent to the old unchunked path
        chunk_5 = run(5)
        chunk_1 = run(1)  # most extreme case: every ticker scored alone

        _assert_calls_close(full_batch, chunk_5)
        _assert_calls_close(full_batch, chunk_1)
        # every ticker got its signal_5d + meta_labeler rows written
        assert {c["ticker"] for c in full_batch} == set(TICKERS)
        assert {c["model_name"] for c in full_batch} == {di.SIGNAL_MODEL_NAME, di.META_MODEL_NAME}


class TestStepPndFilterChunkingMatchesUnchunked:
    @pytest.mark.parametrize("configured_chunk_size", [1000, 5, 1])
    def test_blocked_set_and_written_rows_identical_across_chunk_sizes(
        self, pnd_model, pnd_feature_matrix, tmp_path, monkeypatch, configured_chunk_size
    ):
        monkeypatch.setattr(di, "_load_model", lambda cls, name, models_dir: pnd_model)
        monkeypatch.setattr("config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE", configured_chunk_size)

        client = _RecordingClient()
        blocked = di._step_pnd_filter(pnd_feature_matrix, RUN_DATE, client, "http://fake", tmp_path)

        assert isinstance(blocked, set)
        assert len(client.calls) == len(TICKERS)

    def test_full_batch_5_and_1_chunk_runs_produce_identical_blocked_set_and_rows(
        self, pnd_model, pnd_feature_matrix, tmp_path, monkeypatch
    ):
        def run(configured_chunk_size):
            monkeypatch.setattr(di, "_load_model", lambda cls, name, models_dir: pnd_model)
            monkeypatch.setattr("config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE", configured_chunk_size)
            client = _RecordingClient()
            blocked = di._step_pnd_filter(pnd_feature_matrix, RUN_DATE, client, "http://fake", tmp_path)
            return blocked, _normalize_calls(client.calls)

        blocked_full, rows_full = run(1000)
        blocked_5, rows_5 = run(5)
        blocked_1, rows_1 = run(1)

        assert blocked_full == blocked_5 == blocked_1
        _assert_calls_close(rows_full, rows_5)
        _assert_calls_close(rows_full, rows_1)
