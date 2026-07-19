"""
tests/unit/test_regime_signal.py

2026-07-19 full-codebase-review Fix B2: features/regime_signal.py's
realized-volatility regime classifier.
"""

import numpy as np
import pandas as pd

from features.regime_signal import HIGH_VOL, NORMAL, compute_realized_vol_regime


def _benchmark_series(daily_returns: np.ndarray, start="2020-01-01") -> pd.Series:
    dates = pd.date_range(start, periods=len(daily_returns) + 1, freq="B")
    prices = 100.0 * np.cumprod(np.concatenate([[1.0], 1.0 + daily_returns]))
    return pd.Series(prices, index=dates)


class TestComputeRealizedVolRegime:
    def test_calm_then_volatile_period_is_correctly_labeled(self):
        rng = np.random.default_rng(3)
        calm = rng.normal(0.0003, 0.003, 300)  # low daily vol
        volatile = rng.normal(0.0, 0.04, 60)  # much higher daily vol
        returns = np.concatenate([calm, volatile])
        benchmark = _benchmark_series(returns)

        regime = compute_realized_vol_regime(
            benchmark, vol_window=21, regime_lookback_days=252, high_vol_percentile=0.75
        )

        # Near the end of the volatile stretch, regime should be HIGH_VOL.
        assert regime.iloc[-1] == HIGH_VOL

    def test_insufficient_history_is_nan_not_a_guess(self):
        benchmark = _benchmark_series(np.random.default_rng(1).normal(0, 0.01, 10))
        regime = compute_realized_vol_regime(benchmark, vol_window=21, regime_lookback_days=252)
        assert regime.isna().all()

    def test_output_index_matches_input(self):
        benchmark = _benchmark_series(np.random.default_rng(2).normal(0, 0.01, 400))
        regime = compute_realized_vol_regime(benchmark)
        assert list(regime.index) == list(benchmark.index)

    def test_labels_are_only_known_values(self):
        benchmark = _benchmark_series(np.random.default_rng(4).normal(0, 0.015, 400))
        regime = compute_realized_vol_regime(benchmark)
        known = regime.dropna()
        assert set(known.unique()) <= {HIGH_VOL, NORMAL}
