"""
tests/unit/test_features_technical.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-FEAT-001, SPEC-PIPE-004, SPEC-PIPE-005
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for features/technical.py: dtype/range/finiteness guarantees,
the vectorization invariant (identical output regardless of how many other
tickers share the input panel), SPEC-FEAT-001's minimum-history NaN
behavior, and a >=500-stock performance benchmark (SPEC-PIPE-004: <15
minutes, marked slow so default `pytest -m "not slow"` runs skip it).

All fixtures here are synthetic (no DuckDB/API dependency) so this module
is fast and fully deterministic.
"""

import time

import numpy as np
import pandas as pd
import pytest

from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features


def _make_ohlcv(tickers, n_days, seed=0, start="2024-01-01"):
    """Deterministic synthetic OHLCV panel: one geometric random walk per ticker."""
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
        delivery_pct = rng.uniform(10, 90, n_days)
        frames.append(
            pd.DataFrame(
                {
                    "date": dates,
                    "ticker": ticker,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "delivery_pct": delivery_pct,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def _make_benchmark(n_days, seed=999, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=n_days)
    rng = np.random.default_rng(seed)
    out = {"date": dates}
    for name in ("nifty50", "nifty100", "nifty500"):
        rets = rng.normal(0.0002, 0.01, n_days)
        out[f"{name}_close"] = 100 * np.cumprod(1 + rets)
    return pd.DataFrame(out)


@pytest.fixture(scope="module")
def ohlcv_20_stocks():
    tickers = [f"T{i:04d}" for i in range(20)]
    return _make_ohlcv(tickers, n_days=300, seed=1)


@pytest.fixture(scope="module")
def benchmark_300d():
    return _make_benchmark(n_days=300, seed=42)


@pytest.fixture(scope="module")
def features_20_stocks(ohlcv_20_stocks, benchmark_300d):
    return compute_technical_features(ohlcv_20_stocks, benchmark_300d)


class TestFeatureCatalog:
    def test_catalog_has_77_features(self):
        """CORE_TECHNICAL_FEATURES is the resolved 77-feature list: the original
        70 (see module docstring for the 76-vs-70 discrepancy in the per-category
        spec counts) plus Category 12's 7 exit indicators."""
        assert len(CORE_TECHNICAL_FEATURES) == 77
        assert len(set(CORE_TECHNICAL_FEATURES)) == 77  # no duplicates

    def test_output_columns_match_catalog(self, features_20_stocks):
        expected = ["date", "ticker"] + CORE_TECHNICAL_FEATURES
        assert list(features_20_stocks.columns) == expected


class TestDTypeAndFiniteness:
    def test_all_features_are_float64(self, features_20_stocks):
        for col in CORE_TECHNICAL_FEATURES:
            assert features_20_stocks[col].dtype == np.float64, f"{col} is not float64"

    def test_no_infinities(self, features_20_stocks):
        values = features_20_stocks[CORE_TECHNICAL_FEATURES].to_numpy()
        finite_or_nan = np.isfinite(values) | np.isnan(values)
        assert finite_or_nan.all(), "found +/-inf in computed features"

    def test_rsi_14_in_0_100(self, features_20_stocks):
        rsi = features_20_stocks["rsi_14"].dropna()
        assert len(rsi) > 0
        assert (rsi >= 0).all() and (rsi <= 100).all()

    def test_rsi_2_in_0_100(self, features_20_stocks):
        rsi = features_20_stocks["rsi_2"].dropna()
        assert len(rsi) > 0
        assert (rsi >= 0).all() and (rsi <= 100).all()

    def test_delivery_pct_in_0_100(self, features_20_stocks):
        dp = features_20_stocks["delivery_pct"].dropna()
        assert (dp >= 0).all() and (dp <= 100).all()


class TestCategory12ExitIndicators:
    @pytest.mark.parametrize("m", [10, 20, 22])
    def test_atr_pct_is_non_negative(self, features_20_stocks, m):
        atr_pct = features_20_stocks[f"atr_{m}_pct"].dropna()
        assert len(atr_pct) > 0
        assert (atr_pct >= 0).all()

    @pytest.mark.parametrize("window", [10, 22, 55])
    def test_hh_is_at_least_the_rolling_low(self, features_20_stocks, ohlcv_20_stocks, window):
        """hh_N (rolling max of high) must be >= the rolling min of low over
        the same window — a basic sanity bound that doesn't depend on the
        exact rolling-max implementation."""
        hh = features_20_stocks[f"hh_{window}"]
        low_min = (
            ohlcv_20_stocks.sort_values(["ticker", "date"])
            .groupby("ticker", sort=False)["low"]
            .rolling(window, min_periods=window)
            .min()
            .reset_index(level=0, drop=True)
        )
        mask = hh.notna() & low_min.notna()
        assert mask.sum() > 0
        assert (hh[mask].to_numpy() >= low_min[mask].to_numpy()).all()

    def test_psar_is_finite_after_warmup(self, features_20_stocks):
        psar = features_20_stocks["psar"].dropna()
        assert len(psar) > 0
        assert (psar > 0).all()


class TestVectorization:
    """SPEC-PIPE-004: per-ticker results must not depend on which other
    tickers share the input panel — proves there's no cross-ticker leakage
    from the groupby-based vectorized implementation."""

    def test_10_stocks_vs_500_stocks_identical(self):
        tickers_500 = [f"T{i:04d}" for i in range(500)]
        ohlcv_500 = _make_ohlcv(tickers_500, n_days=260, seed=7)
        benchmark = _make_benchmark(n_days=260, seed=123)

        full = compute_technical_features(ohlcv_500, benchmark)

        subset_tickers = tickers_500[:10]
        ohlcv_10 = ohlcv_500[ohlcv_500["ticker"].isin(subset_tickers)].copy()
        subset = compute_technical_features(ohlcv_10, benchmark)

        full_subset = (
            full[full["ticker"].isin(subset_tickers)]
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )
        subset = subset.sort_values(["ticker", "date"]).reset_index(drop=True)

        pd.testing.assert_frame_equal(full_subset, subset)


class TestMinimumHistory:
    """SPEC-FEAT-001: features with an N-day lookback are NaN until a
    ticker has accumulated N observations; a ticker with insufficient
    history must not silently get a partial-window estimate."""

    def test_short_history_ticker_has_nan_lookback_features(self):
        short_ticker = "SHORT"
        long_ticker = "LONG"
        ohlcv = pd.concat(
            [
                _make_ohlcv([short_ticker], n_days=50, seed=11),
                _make_ohlcv([long_ticker], n_days=300, seed=12),
            ],
            ignore_index=True,
        )
        features = compute_technical_features(ohlcv)

        short_rows = features[features["ticker"] == short_ticker]
        # dist_from_52w_high needs a 252-day window; 50 rows can never satisfy it.
        assert short_rows["dist_from_52w_high"].isna().all()
        assert short_rows["dist_from_52w_low"].isna().all()
        assert short_rows["sma_200_ratio"].isna().all()

        long_rows = features[features["ticker"] == long_ticker].sort_values("date")
        # With 300 days of history the same columns must eventually populate.
        assert long_rows["dist_from_52w_high"].notna().any()
        assert long_rows["sma_200_ratio"].notna().any()

    def test_short_lookback_features_still_populate_for_short_history(self):
        """A 50-day ticker should still get short-window features (e.g. rsi_14, 14d) —
        only the longer lookbacks (>50d) are expected to be NaN."""
        ohlcv = _make_ohlcv(["SHORT"], n_days=50, seed=21)
        features = compute_technical_features(ohlcv)
        assert features["rsi_14"].notna().any()
        assert features["pct_rank_5d"].notna().any()


@pytest.mark.slow
class TestPerformanceBenchmark:
    """SPEC-PIPE-004: 76 (here: 70) core technical features for 500 stocks
    in < 15 minutes on reference hardware. Marked slow; excluded from the
    default `pytest tests/unit -v` run per CLAUDE.md (`-m "not slow"`)."""

    def test_500_stocks_under_15_minutes(self):
        tickers = [f"BENCH{i:04d}" for i in range(500)]
        ohlcv = _make_ohlcv(tickers, n_days=300, seed=99)
        benchmark = _make_benchmark(n_days=300, seed=999)

        start = time.time()
        result = compute_technical_features(ohlcv, benchmark)
        elapsed = time.time() - start

        assert elapsed < 15 * 60, f"500-stock feature computation took {elapsed:.1f}s (budget: 900s)"
        assert result["ticker"].nunique() == 500
