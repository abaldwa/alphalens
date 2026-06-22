"""
tests/unit/test_pnd_features.py

Phase: 1.3 (P&D Detection)
Specs: SPEC-MODEL-006, SPEC-PIPE-004
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for features/pnd_features.py. Synthetic fixtures only.
"""

import numpy as np
import pandas as pd
import pytest

from features.pnd_features import PND_FEATURES, compute_pnd_features


def _flat_ohlcv_rows(dates, ticker, prices, volumes, delivery_pcts):
    """Build O=H=L=C-or-ranged rows; `prices` controls close, with a small
    +/-1% range unless the row is meant to be circuit-locked (handled by caller)."""
    rows = []
    for d, p, v, dp in zip(dates, prices, volumes, delivery_pcts):
        rows.append(
            {"date": d, "ticker": ticker, "open": p, "high": p * 1.01, "low": p * 0.99, "close": p, "volume": v,
             "delivery_pct": dp}
        )
    return rows


def _make_normal_ohlcv(tickers, n_days, seed=0, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=n_days)
    frames = []
    for i, ticker in enumerate(tickers):
        rng = np.random.default_rng(seed + i)
        base_price = 50 + rng.uniform(0, 200)
        rets = rng.normal(0.0002, 0.015, n_days)
        close = base_price * np.cumprod(1 + rets)
        open_ = close * (1 + rng.normal(0, 0.004, n_days))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.004, n_days)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.004, n_days)))
        volume = rng.integers(50_000, 500_000, n_days).astype(float)
        delivery_pct = rng.uniform(30, 80, n_days)
        frames.append(
            pd.DataFrame(
                {"date": dates, "ticker": ticker, "open": open_, "high": high, "low": low, "close": close,
                 "volume": volume, "delivery_pct": delivery_pct}
            )
        )
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def normal_features():
    ohlcv = _make_normal_ohlcv(["A", "B", "C"], n_days=90, seed=1)
    return compute_pnd_features(ohlcv)


class TestDTypeAndFiniteness:
    def test_all_22_features_present(self, normal_features):
        assert len(PND_FEATURES) == 22
        for col in PND_FEATURES:
            assert col in normal_features.columns

    def test_all_features_float64(self, normal_features):
        for col in PND_FEATURES:
            assert normal_features[col].dtype == np.float64, f"{col} is not float64"

    def test_no_infinities(self, normal_features):
        values = normal_features[PND_FEATURES].to_numpy()
        finite_or_nan = np.isfinite(values) | np.isnan(values)
        assert finite_or_nan.all(), "found +/-inf in computed P&D features"

    def test_missing_required_column_raises(self):
        bad = pd.DataFrame({"date": ["2024-01-01"], "ticker": ["A"]})
        with pytest.raises(ValueError):
            compute_pnd_features(bad)


class TestCircuitDayDetection:
    """Prompt requirement: 5 consecutive upper circuits -> consecutive_circuit_days == 5."""

    def test_five_consecutive_upper_circuits(self):
        dates = pd.bdate_range("2024-01-01", periods=20)
        rows = []
        price = 100.0
        for i in range(15):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": price, "high": price * 1.01, "low": price * 0.99,
                 "close": price, "volume": 100_000, "delivery_pct": 50.0}
            )
            price *= 1.001
        for i in range(15, 20):
            price *= 1.05
            rows.append(
                {"date": dates[i], "ticker": "X", "open": price, "high": price, "low": price, "close": price,
                 "volume": 500_000, "delivery_pct": 5.0}
            )
        df = pd.DataFrame(rows)
        out = compute_pnd_features(df)

        assert out["consecutive_circuit_days"].iloc[-1] == 5.0
        assert out["consecutive_circuit_days"].iloc[-5:].tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]
        assert (out["consecutive_circuit_days"].iloc[:15] == 0.0).all()

    def test_circuit_run_resets_after_a_normal_day(self):
        dates = pd.bdate_range("2024-01-01", periods=20)
        rows = []
        price = 100.0
        for i in range(12):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": price, "high": price * 1.01, "low": price * 0.99,
                 "close": price, "volume": 100_000, "delivery_pct": 50.0}
            )
            price *= 1.001
        for i in range(12, 15):  # 3 circuit days
            price *= 1.05
            rows.append(
                {"date": dates[i], "ticker": "X", "open": price, "high": price, "low": price, "close": price,
                 "volume": 500_000, "delivery_pct": 5.0}
            )
        # one normal day breaks the run
        price *= 1.001
        rows.append(
            {"date": dates[15], "ticker": "X", "open": price, "high": price * 1.01, "low": price * 0.99,
             "close": price, "volume": 100_000, "delivery_pct": 50.0}
        )
        for i in range(16, 20):  # 4 more circuit days
            price *= 1.05
            rows.append(
                {"date": dates[i], "ticker": "X", "open": price, "high": price, "low": price, "close": price,
                 "volume": 500_000, "delivery_pct": 5.0}
            )
        df = pd.DataFrame(rows)
        out = compute_pnd_features(df)

        assert out["consecutive_circuit_days"].iloc[14] == 3.0  # end of first run
        assert out["consecutive_circuit_days"].iloc[15] == 0.0  # the breaking normal day
        assert out["consecutive_circuit_days"].iloc[19] == 4.0  # end of second run


class TestDeliveryCollapse:
    """Prompt requirement: high volume + low delivery flagged correctly."""

    def test_high_volume_low_delivery_flags(self):
        dates = pd.bdate_range("2024-01-01", periods=30)
        rows = []
        for i in range(25):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": 100, "high": 101, "low": 99, "close": 100,
                 "volume": 100_000, "delivery_pct": 60.0}
            )
        for i in range(25, 30):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": 100, "high": 101, "low": 99, "close": 100,
                 "volume": 500_000, "delivery_pct": 5.0}  # 5x volume, delivery far below 50% of 4w avg
            )
        df = pd.DataFrame(rows)
        out = compute_pnd_features(df)

        assert (out["delivery_collapse_flag"].iloc[25:] == 1.0).all()
        assert (out["delivery_collapse_flag"].iloc[:25] == 0.0).all()

    def test_normal_volume_low_delivery_does_not_flag(self):
        """Low delivery alone, without the volume spike, must not trip the collapse flag."""
        dates = pd.bdate_range("2024-01-01", periods=30)
        rows = []
        for i in range(25):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": 100, "high": 101, "low": 99, "close": 100,
                 "volume": 100_000, "delivery_pct": 60.0}
            )
        for i in range(25, 30):
            rows.append(
                {"date": dates[i], "ticker": "X", "open": 100, "high": 101, "low": 99, "close": 100,
                 "volume": 100_000, "delivery_pct": 5.0}  # same volume, just lower delivery
            )
        df = pd.DataFrame(rows)
        out = compute_pnd_features(df)

        assert (out["delivery_collapse_flag"].iloc[25:] == 0.0).all()


class TestVectorization:
    def test_per_ticker_independence(self, normal_features):
        ohlcv = _make_normal_ohlcv(["A", "B", "C"], n_days=90, seed=1)
        full = compute_pnd_features(ohlcv)
        subset_input = ohlcv[ohlcv["ticker"] == "A"].reset_index(drop=True)
        subset = compute_pnd_features(subset_input)

        full_a = full[full["ticker"] == "A"].reset_index(drop=True)
        pd.testing.assert_frame_equal(full_a, subset)
