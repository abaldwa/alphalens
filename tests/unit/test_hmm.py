"""
tests/unit/test_hmm.py

Phase: 1.2 (Core Feature Computation — M-01 HMM)
Specs: 02_models.md M-01, SPEC-MODEL-003, SPEC-PIPE-004
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for systems/ml_signal_engine/models/hmm/regime_detector.py.
Synthetic fixtures only — no DuckDB/API dependency, deterministic seeds.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.models.hmm.regime_detector import (
    HMM_REGIME_FEATURES,
    MIN_OBSERVATIONS,
    OBSERVABLE_COLUMNS,
    HMMRegimeDetector,
    compute_hmm_observables,
    compute_hmm_regime_features,
)


def _make_ohlcv(tickers, n_days, seed=0, start="2023-01-01"):
    """Same generator shape as test_features_technical.py's, kept local/minimal here."""
    dates = pd.bdate_range(start=start, periods=n_days)
    frames = []
    for i, ticker in enumerate(tickers):
        rng = np.random.default_rng(seed + i)
        base_price = 100 + rng.uniform(0, 900)
        rets = rng.normal(0.0003, 0.02, n_days)
        close = base_price * np.cumprod(1 + rets)
        open_ = close * (1 + rng.normal(0, 0.005, n_days))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.005, n_days)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.005, n_days)))
        volume = rng.integers(100_000, 5_000_000, n_days).astype(float)
        frames.append(
            pd.DataFrame(
                {"date": dates, "ticker": ticker, "open": open_, "high": high, "low": low, "close": close,
                 "volume": volume}
            )
        )
    return pd.concat(frames, ignore_index=True)


def _make_two_regime_series(n_per_regime=200, seed=5):
    """
    A single-ticker series with an unambiguous structural break: a clearly
    bearish (negative drift, high vol) first half and a clearly bullish
    (positive drift, low vol) second half — used to test that fitted
    states are labeled by mean return, not by arbitrary state index.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start="2022-01-01", periods=2 * n_per_regime)

    bear_rets = rng.normal(-0.01, 0.035, n_per_regime)
    bull_rets = rng.normal(0.01, 0.008, n_per_regime)
    rets = np.concatenate([bear_rets, bull_rets])

    close = 100 * np.cumprod(1 + rets)
    open_ = close * (1 + rng.normal(0, 0.003, len(close)))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.004, len(close))))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.004, len(close))))
    volume = rng.integers(100_000, 5_000_000, len(close)).astype(float)

    df = pd.DataFrame(
        {"date": dates, "ticker": "REGIME", "open": open_, "high": high, "low": low, "close": close,
         "volume": volume}
    )
    return df, n_per_regime


class TestComputeHmmObservables:
    def test_three_observable_columns_added(self):
        ohlcv = _make_ohlcv(["A"], n_days=60, seed=1)
        out = compute_hmm_observables(ohlcv)
        for col in OBSERVABLE_COLUMNS:
            assert col in out.columns

    def test_observables_nan_during_warmup_only(self):
        ohlcv = _make_ohlcv(["A"], n_days=60, seed=1)
        out = compute_hmm_observables(ohlcv)
        # realized_vol_10d needs 10 rows, volume_ratio_20d needs 20 -> first 19 rows incomplete
        assert out["volume_ratio_20d"].iloc[:19].isna().all()
        assert out["volume_ratio_20d"].iloc[19:].notna().all()


class TestHMMRegimeDetectorFit:
    def test_fit_raises_below_min_observations(self):
        ohlcv = _make_ohlcv(["A"], n_days=40, seed=2)  # well under MIN_OBSERVATIONS after warmup
        obs = compute_hmm_observables(ohlcv)
        detector = HMMRegimeDetector(n_restarts=2, n_iter=50)
        with pytest.raises(ValueError):
            detector.fit(obs)

    def test_predict_regime_before_fit_raises(self):
        ohlcv = _make_ohlcv(["A"], n_days=60, seed=2)
        obs = compute_hmm_observables(ohlcv)
        detector = HMMRegimeDetector()
        with pytest.raises(RuntimeError):
            detector.predict_regime(obs)

    def test_three_states_and_bearish_bullish_labeling_by_mean_return(self):
        """02_models.md: 'States labelled correctly by mean return sign' — the bearish
        first half must rank lower than the bullish second half on average."""
        df, n_per_regime = _make_two_regime_series()
        obs = compute_hmm_observables(df)
        assert len(obs) >= MIN_OBSERVATIONS

        detector = HMMRegimeDetector(n_restarts=8, n_iter=300, random_state=7)
        detector.fit(obs)
        assert detector._model.n_components == 3

        regimes, probs = detector.predict_regime(obs)
        assert set(regimes.dropna().unique()) <= {0.0, 1.0, 2.0}
        assert list(probs.columns) == [f"state_prob_{r}" for r in range(3)]

        bear_half_mean = regimes.iloc[:n_per_regime].mean()
        bull_half_mean = regimes.iloc[n_per_regime:].mean()
        assert bull_half_mean > bear_half_mean


class TestComputeHmmRegimeFeatures:
    def test_output_shape_and_dtype(self):
        ohlcv = _make_ohlcv(["A", "B"], n_days=150, seed=3)
        out = compute_hmm_regime_features(ohlcv, n_restarts=2, n_iter=50)

        assert list(out.columns) == ["date", "ticker"] + HMM_REGIME_FEATURES
        assert set(out["ticker"].unique()) == {"A", "B"}
        for col in HMM_REGIME_FEATURES:
            assert out[col].dtype == np.float64

    def test_short_history_ticker_gets_nan_not_a_crash(self):
        """SPEC-FEAT-001-style graceful degradation: too little history -> NaN, never an exception."""
        ohlcv = _make_ohlcv(["SHORT"], n_days=30, seed=4)
        out = compute_hmm_regime_features(ohlcv, n_restarts=2, n_iter=50)
        assert out["hmm_regime"].isna().all()

    def test_single_ticker_input_does_not_hit_groupby_reshape_bug(self):
        """Regression guard: features/technical.py hit a pandas single-group groupby().apply()
        reshape footgun this session; compute_hmm_regime_features uses an explicit per-ticker
        loop + concat instead, so a single-ticker input must produce a per-row (not wide) result."""
        ohlcv = _make_ohlcv(["ONLY"], n_days=150, seed=6)
        out = compute_hmm_regime_features(ohlcv, n_restarts=2, n_iter=50)
        assert len(out) == 150
        assert out["ticker"].eq("ONLY").all()

    def test_regime_duration_resets_on_transition(self):
        df, n_per_regime = _make_two_regime_series()
        out = compute_hmm_regime_features(df, n_restarts=8, n_iter=300)
        duration = out["hmm_regime_duration"].dropna()
        assert duration.min() >= 1
        # duration must reset to 1 at least once somewhere a transition is flagged
        transitioned = out["hmm_regime_transition"] == 1.0
        if transitioned.any():
            first_transition_idx = out.index[transitioned][0]
            assert out.loc[first_transition_idx, "hmm_regime_duration"] == 1.0

    def test_missing_required_column_raises(self):
        bad = pd.DataFrame({"date": ["2026-01-01"], "ticker": ["A"]})
        with pytest.raises(ValueError):
            compute_hmm_regime_features(bad)
