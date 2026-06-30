"""
tests/regression/test_known_pnd.py

Phase: 1.3 (P&D Detection)
Specs: SPEC-MODEL-006
Owner: Platform / QA
Consumers: CI, pytest

CRITICAL REGRESSION TEST — the safety net for M-06, the P&D pre-filter
that runs before any buy signal reaches the user (SPEC-MODEL-006). These
3 patterns must pass on every build:

  Pattern 1: Volume 10x + price up 40% over 5 days + delivery collapse -> score >= 70
  Pattern 2: 8 consecutive upper circuits + delivery < 5%               -> score >= 80
  Pattern 3: Normal blue-chip trading (HDFC-Bank-like, stable)          -> score <= 20

The detector itself is trained on real data only, via
pnd_detector.load_pnd_training_data_from_db() (KNOWN_PND_TICKERS' real
OHLCV around their documented event windows + clean real comparison
tickers) — there is no synthetic-training-data fallback; this test skips
if that real training data isn't available yet.

The 3 OHLCV panels below are deterministic, hand-built stress-test
fixtures exercising the literal numeric pattern descriptions above (not
training data, and not a stand-in for real market history) — they let
this regression test assert exact detector behavior at known decision
boundaries (10x volume, 40% runup, 8 circuits, etc.) without depending on
whether a real historical instance of each exact boundary case exists in
the archive yet. Replacing them with real archived P&D event OHLCV is
tracked in BuildLog.md "Real data sourcing — P&D pattern regression
fixtures".
"""

import numpy as np
import pandas as pd
import pytest

from features.pnd_features import compute_pnd_features
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db


@pytest.fixture(scope="module")
def trained_detector():
    try:
        X, y = load_pnd_training_data_from_db()
    except RuntimeError as exc:
        pytest.skip(f"real P&D training data not yet available: {exc}")
    detector = PnDDetector(random_state=11)
    detector.train(X, y)
    return detector


def _score_last_day(detector: PnDDetector, ohlcv: pd.DataFrame) -> float:
    features = compute_pnd_features(ohlcv)
    last_row = features.sort_values("date").tail(1)
    full = detector.predict_full(last_row)
    return float(full["pnd_score"].iloc[0])


def _pattern_1_volume_price_delivery_spike() -> pd.DataFrame:
    """Volume 10x + price up 40% over 5 days + delivery collapse."""
    base_days = 65  # >= 60 so vol_spike_vs_60d_avg (60d rolling) is populated, not NaN
    spike_days = 5
    dates = pd.bdate_range("2024-01-01", periods=base_days + spike_days)
    rng = np.random.default_rng(100)
    base_price = 50.0
    base_close = base_price * np.cumprod(1 + rng.normal(0.0, 0.01, base_days))
    base_volume = rng.uniform(80_000, 120_000, base_days)
    base_delivery = rng.uniform(45, 65, base_days)

    # +40% cumulative over 5 days, compounding (~7%/day).
    daily_growth = 1.40 ** (1 / spike_days)
    spike_close = base_close[-1] * (daily_growth ** np.arange(1, spike_days + 1))
    spike_volume = base_volume.mean() * 10.0 * np.ones(spike_days)
    spike_delivery = np.full(spike_days, 4.0)  # collapsed delivery

    close = np.concatenate([base_close, spike_close])
    volume = np.concatenate([base_volume, spike_volume])
    delivery_pct = np.concatenate([base_delivery, spike_delivery])
    open_ = close * 0.995
    high = close * 1.01
    low = close * 0.985

    return pd.DataFrame(
        {"date": dates, "ticker": "PUMP1", "open": open_, "high": high, "low": low, "close": close,
         "volume": volume, "delivery_pct": delivery_pct}
    )


def _pattern_2_eight_circuits() -> pd.DataFrame:
    """8 consecutive upper circuits + delivery < 5%."""
    base_days = 60  # >= 60 so vol_spike_vs_60d_avg (60d rolling) is populated, not NaN
    circuit_days = 8
    dates = pd.bdate_range("2024-01-01", periods=base_days + circuit_days)
    rng = np.random.default_rng(200)
    base_price = 30.0
    base_close = base_price * np.cumprod(1 + rng.normal(0.0, 0.008, base_days))
    base_volume = rng.uniform(40_000, 80_000, base_days)
    base_delivery = rng.uniform(40, 60, base_days)

    price = base_close[-1]
    circuit_close = []
    for _ in range(circuit_days):
        price *= 1.20  # circuit-up move each day
        circuit_close.append(price)
    circuit_close = np.array(circuit_close)
    circuit_volume = base_volume.mean() * rng.uniform(6, 12, circuit_days)
    circuit_delivery = rng.uniform(1.0, 4.5, circuit_days)  # < 5%

    close = np.concatenate([base_close, circuit_close])
    volume = np.concatenate([base_volume, circuit_volume])
    delivery_pct = np.concatenate([base_delivery, circuit_delivery])
    open_ = close.copy()
    high = close.copy()
    low = close.copy()
    # Base period gets a normal intraday range; circuit days stay flat (O=H=L=C).
    open_[:base_days] = close[:base_days] * 0.997
    high[:base_days] = close[:base_days] * 1.01
    low[:base_days] = close[:base_days] * 0.99

    return pd.DataFrame(
        {"date": dates, "ticker": "CIRCUIT1", "open": open_, "high": high, "low": low, "close": close,
         "volume": volume, "delivery_pct": delivery_pct}
    )


def _pattern_3_stable_bluechip() -> pd.DataFrame:
    """Normal blue-chip trading: stable price, healthy delivery, no spikes (HDFC-Bank-like)."""
    dates = pd.bdate_range("2024-01-01", periods=120)
    rng = np.random.default_rng(300)

    base_price = 1600.0  # HDFC Bank order-of-magnitude price level
    rets = rng.normal(0.0003, 0.012, len(dates))  # low daily vol, mild positive drift
    close = base_price * np.cumprod(1 + rets)
    open_ = close * (1 + rng.normal(0, 0.003, len(dates)))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, len(dates))))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, len(dates))))
    volume = rng.uniform(2_000_000, 4_000_000, len(dates))  # steady large-cap volume, no spikes
    delivery_pct = rng.uniform(55, 70, len(dates))  # healthy, stable delivery

    return pd.DataFrame(
        {"date": dates, "ticker": "HDFCBANK", "open": open_, "high": high, "low": low, "close": close,
         "volume": volume, "delivery_pct": delivery_pct}
    )


class TestKnownPnDPatterns:
    def test_pattern_1_volume_price_delivery_spike_scores_high(self, trained_detector):
        score = _score_last_day(trained_detector, _pattern_1_volume_price_delivery_spike())
        msg = f"Pattern 1 (volume 10x + 40% price runup + delivery collapse) scored {score}, expected >= 70"
        assert score >= 70, msg

    def test_pattern_2_eight_circuits_scores_very_high(self, trained_detector):
        score = _score_last_day(trained_detector, _pattern_2_eight_circuits())
        assert score >= 80, f"Pattern 2 (8 consecutive upper circuits + delivery < 5%) scored {score}, expected >= 80"

    def test_pattern_3_stable_bluechip_scores_low(self, trained_detector):
        score = _score_last_day(trained_detector, _pattern_3_stable_bluechip())
        assert score <= 20, f"Pattern 3 (stable blue-chip trading) scored {score}, expected <= 20"

    def test_patterns_1_and_2_trigger_hard_block(self, trained_detector):
        """SPEC-MODEL-006: score > PND_BLOCK_THRESHOLD (60) must hard-block."""
        for pattern_fn in (_pattern_1_volume_price_delivery_spike, _pattern_2_eight_circuits):
            features = compute_pnd_features(pattern_fn())
            last_row = features.sort_values("date").tail(1)
            full = trained_detector.predict_full(last_row)
            assert bool(full["pnd_block"].iloc[0]) is True

    def test_pattern_3_does_not_trigger_block_or_flag(self, trained_detector):
        features = compute_pnd_features(_pattern_3_stable_bluechip())
        last_row = features.sort_values("date").tail(1)
        full = trained_detector.predict_full(last_row)
        assert bool(full["pnd_block"].iloc[0]) is False
        assert bool(full["pnd_flag"].iloc[0]) is False
