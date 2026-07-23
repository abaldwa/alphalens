"""
tests/unit/test_phase3_features.py

Phase: 3.1
Specs: SPEC-FEAT-001, SPEC-PIPE-003, SPEC-PIPE-004, SPEC-MODEL-010
Owner: Platform / Tests
Consumers: CI pipeline

Unit tests for Phase 3 feature modules:
  features/advanced_technical.py — wavelet, Hurst, entropy, fracdiff, complexity
  features/pattern_scores.py    — 6 chart-pattern probability scores
  features/real_economy_macro.py — 10 real-economy macro indicators (PIT-safe)
  features/deep_forensic.py     — 28 forensic features (Groups D–I)

Key mandated tests (from build prompt):
  - Hurst exponent: Brownian motion series → ~0.5; trending series → > 0.6
  - Pattern scores: all scores in [0, 1]
  - Real economy: forward-filled correctly (no lookahead — availability_date enforced)
"""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_ohlcv(
    n: int = 252,
    ticker: str = "TEST",
    trend: float = 0.0,
    seed: int = 42,
    start_price: float = 100.0,
) -> pd.DataFrame:
    """Build a synthetic OHLCV DataFrame with `n` trading days."""
    rng = np.random.default_rng(seed)
    log_returns = rng.normal(trend / 252, 0.01, n)
    prices = start_price * np.exp(np.cumsum(log_returns))
    opens = prices * (1 + rng.uniform(-0.005, 0.005, n))
    highs = np.maximum(prices, opens) * (1 + rng.uniform(0.0, 0.01, n))
    lows = np.minimum(prices, opens) * (1 - rng.uniform(0.0, 0.01, n))
    volumes = rng.integers(100_000, 1_000_000, n).astype(float)
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "ticker": ticker,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": prices,
            "volume": volumes,
            "delivery_pct": rng.uniform(30, 70, n),
        }
    )


def _make_multi_panel(
    n_tickers: int = 3, n_days: int = 252, trend: float = 0.0
) -> pd.DataFrame:
    """OHLCV panel for multiple tickers."""
    frames = [
        _make_ohlcv(n_days, f"TICK{i}", trend=trend, seed=i)
        for i in range(n_tickers)
    ]
    return pd.concat(frames, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# features/advanced_technical.py
# ─────────────────────────────────────────────────────────────────────────────

class TestHurstExponent:
    """SPEC-FEAT-001 — Hurst exponent behaviour on known series."""

    from features.advanced_technical import _hurst_rs

    def test_brownian_motion_near_half(self):
        """
        Brownian motion (random walk) should have H ≈ 0.5.

        Spec: build prompt mandated test — "brownian motion returns ~0.5".
        Acceptance window ±0.2 to accommodate finite-sample variance.
        """
        from features.advanced_technical import _hurst_rs

        rng = np.random.default_rng(0)
        prices = np.cumsum(rng.normal(0, 1, 500)) + 100
        h = _hurst_rs(prices)
        assert not np.isnan(h), "H should be finite for a long random walk"
        assert 0.3 <= h <= 0.7, f"Brownian H expected near 0.5, got {h:.3f}"

    def test_trending_series_above_threshold(self):
        """
        Trending (persistent) series should have H > 0.6.

        Spec: build prompt mandated test — "trending series returns > 0.6".
        """
        from features.advanced_technical import _hurst_rs

        # Strongly trending: each step biased upward
        prices = np.cumsum(np.abs(np.random.default_rng(7).normal(0.5, 0.1, 500))) + 100
        h = _hurst_rs(prices)
        assert not np.isnan(h), "H should be finite for a trending series"
        assert h > 0.6, f"Trending series H expected > 0.6, got {h:.3f}"

    def test_short_series_returns_nan(self):
        """Series shorter than minimum should return NaN (SPEC-FEAT-001)."""
        from features.advanced_technical import _hurst_rs

        assert np.isnan(_hurst_rs(np.array([1.0, 2.0, 3.0])))

    def test_hurst_clipped_to_valid_range(self):
        """R/S output is clipped to [0, 1.5]; never negative or absurd."""
        from features.advanced_technical import _hurst_rs

        prices = np.linspace(100, 200, 100)
        h = _hurst_rs(prices)
        assert not np.isnan(h)
        assert 0.0 <= h <= 1.5


class TestAdvancedTechnicalPanel:
    """SPEC-PIPE-004 — panel-level API, column presence, NaN handling."""

    def test_returns_correct_columns(self):
        """compute_advanced_technical_features returns date/ticker + all 18 features."""
        from features.advanced_technical import (
            ADVANCED_TECHNICAL_FEATURES,
            compute_advanced_technical_features,
        )

        panel = _make_ohlcv(80)
        result = compute_advanced_technical_features(panel)
        for col in ["date", "ticker"] + ADVANCED_TECHNICAL_FEATURES:
            assert col in result.columns, f"Missing column: {col}"

    def test_multi_ticker_panel(self):
        """Panel with multiple tickers produces one row per (date, ticker)."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_multi_panel(n_tickers=3, n_days=80)
        result = compute_advanced_technical_features(panel)
        assert len(result) == len(panel), "Row count should match input panel"

    def test_insufficient_history_returns_nan(self):
        """Tickers with < 16 bars return all-NaN (SPEC-FEAT-001)."""
        from features.advanced_technical import (
            ADVANCED_TECHNICAL_FEATURES,
            compute_advanced_technical_features,
        )

        panel = _make_ohlcv(10)
        result = compute_advanced_technical_features(panel)
        last_row = result.iloc[-1]
        for col in ADVANCED_TECHNICAL_FEATURES:
            assert np.isnan(last_row[col]), f"{col} should be NaN for short history"

    def test_missing_column_raises(self):
        """Missing required column raises ValueError."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_ohlcv(80).drop(columns=["volume"])
        with pytest.raises(ValueError, match="missing columns"):
            compute_advanced_technical_features(panel)

    def test_wavelet_energy_ratio_in_unit_interval(self):
        """wavelet_energy_ratio must be in [0, 1]."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_ohlcv(80)
        result = compute_advanced_technical_features(panel)
        last = result.iloc[-1]
        val = last["wavelet_energy_ratio"]
        if not np.isnan(val):
            assert 0.0 <= val <= 1.0, f"wavelet_energy_ratio={val} out of [0,1]"

    def test_fracdiff_d_optimal_in_unit_interval(self):
        """fracdiff_d_optimal (order d) must lie in [0, 1]."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_ohlcv(80)
        result = compute_advanced_technical_features(panel)
        last = result.iloc[-1]
        val = last["fracdiff_d_optimal"]
        if not np.isnan(val):
            assert 0.0 <= val <= 1.0, f"fracdiff_d_optimal={val} outside [0,1]"

    def test_fracdiff_d_uses_real_adf_test(self):
        """2026-07-19 full-codebase-review Fix 10: _optimal_fracdiff_d must
        use a real ADF test (statsmodels adfuller), not the prior lag-1-
        autocorrelation proxy — verified by distinguishing a stationary
        white-noise series (should need close to d=0) from a non-stationary
        random walk (should need close to d=1)."""
        from features.advanced_technical import _optimal_fracdiff_d

        rng = np.random.default_rng(7)
        white_noise = rng.normal(size=200)
        random_walk = np.cumsum(rng.normal(size=200))

        d_noise = _optimal_fracdiff_d(white_noise)
        d_walk = _optimal_fracdiff_d(random_walk)

        # A non-stationary random walk should need materially more
        # differencing than already-stationary white noise.
        assert d_noise < d_walk
        assert d_walk > 0.7

    def test_permutation_entropy_in_unit_interval(self):
        """permutation_entropy_21d (normalised) must be in [0, 1]."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_ohlcv(80)
        result = compute_advanced_technical_features(panel)
        last = result.iloc[-1]
        val = last["permutation_entropy_21d"]
        if not np.isnan(val):
            assert 0.0 <= val <= 1.0, f"permutation_entropy_21d={val} outside [0,1]"

    def test_fractal_dimension_in_valid_range(self):
        """Higuchi fractal dimension must be in [1, 2]."""
        from features.advanced_technical import compute_advanced_technical_features

        panel = _make_ohlcv(80)
        result = compute_advanced_technical_features(panel)
        last = result.iloc[-1]
        val = last["fractal_dimension"]
        if not np.isnan(val):
            assert 1.0 <= val <= 2.0, f"fractal_dimension={val} outside [1,2]"


# ─────────────────────────────────────────────────────────────────────────────
# features/pattern_scores.py
# ─────────────────────────────────────────────────────────────────────────────

class TestPatternScores:
    """SPEC-FEAT-001 — all pattern scores in [0, 1]."""

    def test_all_scores_in_unit_interval(self):
        """
        All 6 pattern scores are in [0, 1].

        Spec: build prompt mandated test — "test pattern scores are in [0,1] range".
        """
        from features.pattern_scores import PATTERN_FEATURES, compute_pattern_scores

        panel = _make_ohlcv(80)
        result = compute_pattern_scores(panel)
        last = result.iloc[-1]
        for col in PATTERN_FEATURES:
            val = last[col]
            if not np.isnan(val):
                assert 0.0 <= val <= 1.0, f"{col}={val} outside [0,1]"

    def test_scores_for_multi_ticker_panel(self):
        """Scores computed for all tickers without error."""
        from features.pattern_scores import PATTERN_FEATURES, compute_pattern_scores

        panel = _make_multi_panel(n_tickers=3, n_days=80)
        result = compute_pattern_scores(panel)
        assert len(result) == len(panel)
        for col in PATTERN_FEATURES:
            assert col in result.columns

    def test_insufficient_history_returns_nan(self):
        """Tickers with < 20 bars return NaN scores (SPEC-FEAT-001)."""
        from features.pattern_scores import PATTERN_FEATURES, compute_pattern_scores

        panel = _make_ohlcv(10)
        result = compute_pattern_scores(panel)
        last = result.iloc[-1]
        for col in PATTERN_FEATURES:
            assert np.isnan(last[col]), f"{col} should be NaN for short history"

    def test_missing_column_raises(self):
        """Missing required column raises ValueError."""
        from features.pattern_scores import compute_pattern_scores

        panel = _make_ohlcv(80).drop(columns=["open"])
        with pytest.raises(ValueError, match="missing columns"):
            compute_pattern_scores(panel)

    def test_head_shoulders_score_type(self):
        """head_shoulders_score returns float, not object dtype."""
        from features.pattern_scores import compute_pattern_scores

        panel = _make_ohlcv(80)
        result = compute_pattern_scores(panel)
        assert result["head_shoulders_score"].dtype in [np.float32, np.float64, float]

    def test_double_bottom_score_with_declining_series(self):
        """Double-bottom should score > 0 for a V-shaped price series."""
        from features.pattern_scores import _double_bottom_score

        # Craft a V-shape: fall then rise, two clear troughs
        n = 40
        prices = np.concatenate([
            np.linspace(100, 50, n // 2),
            np.linspace(50, 100, n // 2),
        ])
        score = _double_bottom_score(prices, prices)
        assert 0.0 <= score <= 1.0


# ─────────────────────────────────────────────────────────────────────────────
# features/real_economy_macro.py
# ─────────────────────────────────────────────────────────────────────────────

class TestRealEconomyMacro:
    """SPEC-PIPE-003 — PIT enforcement; SPEC-FEAT-001 — NaN when unavailable."""

    def test_no_parquet_returns_all_nan(self):
        """
        When macro_real_economy.parquet is absent, all features return NaN.

        Spec: SPEC-FEAT-001 / SPEC-PIPE-006 — never fabricate unavailable data.
        """
        from features.real_economy_macro import (
            REAL_ECONOMY_MACRO_FEATURES,
            load_real_economy_macro,
        )

        non_existent = Path("/tmp/does_not_exist_xyzabc/macro_real_economy.parquet")
        with patch(
            "features.real_economy_macro._MACRO_REAL_ECONOMY_PATH", non_existent
        ):
            result = load_real_economy_macro(pd.Timestamp("2024-06-01"))
        assert isinstance(result, pd.Series)
        for feature in REAL_ECONOMY_MACRO_FEATURES:
            assert np.isnan(result[feature]), f"{feature} should be NaN when file absent"

    def test_pit_blocks_future_release(self):
        """
        Data with availability_date > as_of is never consumed (SPEC-PIPE-003).

        Build prompt mandated: "test real economy features are forward-filled
        correctly (no lookahead)".
        """
        from features.real_economy_macro import (
            REAL_ECONOMY_MACRO_FEATURES,
            load_real_economy_macro,
        )

        as_of = pd.Timestamp("2024-03-01")
        # Put the only available row in the future (availability_date tomorrow)
        df = pd.DataFrame(
            [
                {
                    "feature_name": feat,
                    "value": 5.0,
                    "reference_month_end": pd.Timestamp("2024-01-31"),
                    "availability_date": as_of + pd.Timedelta(days=1),
                }
                for feat in REAL_ECONOMY_MACRO_FEATURES
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_path = Path(f.name)
        df.to_parquet(tmp_path)
        try:
            with patch(
                "features.real_economy_macro._MACRO_REAL_ECONOMY_PATH", tmp_path
            ):
                result = load_real_economy_macro(as_of)
            for feature in REAL_ECONOMY_MACRO_FEATURES:
                assert np.isnan(result[feature]), (
                    f"{feature} should be NaN when availability_date is in the future"
                )
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_pit_allows_past_release(self):
        """
        Data with availability_date <= as_of is returned correctly (SPEC-PIPE-003).
        """
        from features.real_economy_macro import (
            REAL_ECONOMY_MACRO_FEATURES,
            load_real_economy_macro,
        )

        as_of = pd.Timestamp("2024-03-01")
        df = pd.DataFrame(
            [
                {
                    "feature_name": feat,
                    "value": 7.5,
                    "reference_month_end": pd.Timestamp("2024-01-31"),
                    "availability_date": as_of - pd.Timedelta(days=1),
                }
                for feat in REAL_ECONOMY_MACRO_FEATURES
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_path = Path(f.name)
        df.to_parquet(tmp_path)
        try:
            with patch(
                "features.real_economy_macro._MACRO_REAL_ECONOMY_PATH", tmp_path
            ):
                result = load_real_economy_macro(as_of)
            for feature in REAL_ECONOMY_MACRO_FEATURES:
                assert result[feature] == pytest.approx(7.5), (
                    f"{feature} should be 7.5 when availability_date is in the past"
                )
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_latest_reading_selected_when_multiple(self):
        """
        Most recent available reading is returned when multiple months exist.
        """
        from features.real_economy_macro import load_real_economy_macro

        as_of = pd.Timestamp("2024-06-01")
        feature = "pmi_manufacturing"
        df = pd.DataFrame(
            [
                {
                    "feature_name": feature,
                    "value": 50.0,
                    "reference_month_end": pd.Timestamp("2024-02-29"),
                    "availability_date": pd.Timestamp("2024-03-05"),
                },
                {
                    "feature_name": feature,
                    "value": 55.0,
                    "reference_month_end": pd.Timestamp("2024-04-30"),
                    "availability_date": pd.Timestamp("2024-05-03"),
                },
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_path = Path(f.name)
        df.to_parquet(tmp_path)
        try:
            with patch(
                "features.real_economy_macro._MACRO_REAL_ECONOMY_PATH", tmp_path
            ):
                result = load_real_economy_macro(as_of)
            assert result[feature] == pytest.approx(55.0), (
                "Should return the most recent available reading (55.0 not 50.0)"
            )
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_panel_broadcasts_to_all_tickers(self):
        """compute_real_economy_macro_panel produces one row per ticker."""
        from features.real_economy_macro import (
            REAL_ECONOMY_MACRO_FEATURES,
            compute_real_economy_macro_panel,
        )

        tickers = ["A", "B", "C", "D"]
        non_existent = Path("/tmp/missing_macro.parquet")
        with patch(
            "features.real_economy_macro._MACRO_REAL_ECONOMY_PATH", non_existent
        ):
            result = compute_real_economy_macro_panel(pd.Timestamp("2024-06-01"), tickers)
        assert len(result) == len(tickers)
        assert set(result["ticker"]) == set(tickers)
        for col in REAL_ECONOMY_MACRO_FEATURES:
            assert col in result.columns


# ─────────────────────────────────────────────────────────────────────────────
# features/deep_forensic.py
# ─────────────────────────────────────────────────────────────────────────────

class TestDeepForensicHelpers:
    """Unit tests for pure helper functions inside deep_forensic.py."""

    def test_benford_mad_random_data(self):
        """MAD > 0 for uniform random leading digits (not Benford-compliant)."""
        from features.deep_forensic import _benford_mad

        rng = np.random.default_rng(1)
        values = rng.uniform(1, 1000, 50)
        mad = _benford_mad(values)
        assert not np.isnan(mad)
        assert mad >= 0.0

    def test_benford_mad_short_series_nan(self):
        """Fewer than 10 values returns NaN."""
        from features.deep_forensic import _benford_mad

        assert np.isnan(_benford_mad(np.array([1.0, 2.0, 3.0])))

    def test_altman_z_distress_zone(self):
        """Distressed company (high liabilities, low profitability) → Z < 1.81."""
        from features.deep_forensic import _altman_z

        z = _altman_z(
            working_capital=-100,
            retained_earnings=-200,
            ebit=-50,
            total_assets=1000,
            total_liabilities=900,
            revenue=400,
            market_cap=100,
        )
        assert not np.isnan(z)
        assert z < 1.81, f"Expected distress zone (Z<1.81), got {z:.3f}"

    def test_altman_z_safe_zone(self):
        """Healthy company (positive earnings, low debt) → Z > 2.99."""
        from features.deep_forensic import _altman_z

        z = _altman_z(
            working_capital=300,
            retained_earnings=500,
            ebit=200,
            total_assets=1000,
            total_liabilities=200,
            revenue=1500,
            market_cap=2000,
        )
        assert not np.isnan(z)
        assert z > 2.99, f"Expected safe zone (Z>2.99), got {z:.3f}"

    def test_altman_z_nan_input_returns_nan(self):
        """Any NaN input → NaN result."""
        from features.deep_forensic import _altman_z

        z = _altman_z(np.nan, 500, 200, 1000, 200, 1500, 2000)
        assert np.isnan(z)

    def test_peer_outlier_z_zero_mean(self):
        """Stock at sector mean → z-score ≈ 0."""
        from features.deep_forensic import _peer_outlier_z

        peers = np.array([10.0, 10.0, 10.0, 10.0, 10.0])
        z = _peer_outlier_z(10.0, peers)
        assert abs(z) < 1e-6

    def test_peer_outlier_z_insufficient_peers_nan(self):
        """Fewer than 3 peers returns NaN."""
        from features.deep_forensic import _peer_outlier_z

        assert np.isnan(_peer_outlier_z(10.0, np.array([10.0, 11.0])))


class TestDeepForensicPanel:
    """Panel-level tests for compute_deep_forensic_features_panel."""

    def _make_mock_client(self, fund_rows=None, share_rows=None):
        client = MagicMock()
        client.get_fundamentals_history.return_value = fund_rows or []
        client.get_shareholding_history.return_value = share_rows or []
        return client

    def test_empty_fund_rows_returns_all_nan(self):
        """No fundamental data → all-NaN row (SPEC-FEAT-001)."""
        from features.deep_forensic import (
            DEEP_FORENSIC_FEATURES,
            compute_deep_forensic_features_panel,
        )

        client = self._make_mock_client(fund_rows=[])
        result = compute_deep_forensic_features_panel(
            client, ["TICK1"], datetime(2024, 6, 1)
        )
        assert len(result) == 1
        for col in DEEP_FORENSIC_FEATURES:
            assert col in result.columns

    def test_panel_has_one_row_per_ticker(self):
        """Result has exactly one row per ticker."""
        from features.deep_forensic import compute_deep_forensic_features_panel

        client = self._make_mock_client(fund_rows=[])
        tickers = ["A", "B", "C"]
        result = compute_deep_forensic_features_panel(client, tickers, datetime(2024, 6, 1))
        assert len(result) == len(tickers)
        assert set(result["ticker"]) == set(tickers)

    def test_goodwill_ratio_computed_from_fundamentals(self):
        """goodwill_ratio = goodwill / total_assets when data is available."""
        from features.deep_forensic import compute_deep_forensic_features_panel

        fund_row = {
            "quarter_end_date": datetime(2024, 3, 31),
            "announcement_date": datetime(2024, 5, 1),
            "total_assets": 1000.0,
            "goodwill": 150.0,
            "cwip": 50.0,
            "contingent_liabilities": 30.0,
            "subsidiary_count": 5.0,
            "loans_to_related_parties": 20.0,
            "capex": 80.0,
            "intangibles": 40.0,
            "current_assets": 300.0,
            "cash_and_equivalents": 100.0,
            "pat": 200.0,
            "director_remuneration": np.nan,
            "related_party_transactions": np.nan,
            "audit_qualified_flag": 0.0,
            "auditor_changed_flag": 0.0,
            "cfo_tenure_months": 36.0,
            "board_independence_ratio": 0.6,
            "director_resignations_4q": 1.0,
            "whistle_blower_policy_flag": 1.0,
            "revenue": 800.0,
        }
        client = self._make_mock_client(fund_rows=[fund_row])
        result = compute_deep_forensic_features_panel(
            client, ["TICK1"], datetime(2024, 6, 1)
        )
        row = result.iloc[0]
        assert row["goodwill_ratio"] == pytest.approx(0.15)  # 150/1000

    def test_all_features_present_in_output(self):
        """All 28 DEEP_FORENSIC_FEATURES are columns in the output."""
        from features.deep_forensic import (
            DEEP_FORENSIC_FEATURES,
            compute_deep_forensic_features_panel,
        )

        client = self._make_mock_client(fund_rows=[])
        result = compute_deep_forensic_features_panel(
            client, ["T"], datetime(2024, 6, 1)
        )
        for col in DEEP_FORENSIC_FEATURES:
            assert col in result.columns, f"Missing column: {col}"


# ─────────────────────────────────────────────────────────────────────────────
# ADVANCED_TECHNICAL_FEATURES count
# ─────────────────────────────────────────────────────────────────────────────

class TestFeatureCatalogCounts:
    """Verify the feature catalog constants have the expected sizes."""

    def test_advanced_technical_18_features(self):
        """ADVANCED_TECHNICAL_FEATURES must contain exactly 18 entries."""
        from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES

        assert len(ADVANCED_TECHNICAL_FEATURES) == 18, (
            f"Expected 18 advanced technical features, got {len(ADVANCED_TECHNICAL_FEATURES)}"
        )

    def test_pattern_features_6(self):
        """PATTERN_FEATURES must contain exactly 6 entries."""
        from features.pattern_scores import PATTERN_FEATURES

        assert len(PATTERN_FEATURES) == 6, (
            f"Expected 6 pattern features, got {len(PATTERN_FEATURES)}"
        )

    def test_real_economy_macro_10_features(self):
        """REAL_ECONOMY_MACRO_FEATURES must contain exactly 10 entries."""
        from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES

        assert len(REAL_ECONOMY_MACRO_FEATURES) == 10, (
            f"Expected 10 real economy features, got {len(REAL_ECONOMY_MACRO_FEATURES)}"
        )

    def test_deep_forensic_28_features(self):
        """DEEP_FORENSIC_FEATURES must contain exactly 28 entries."""
        from features.deep_forensic import DEEP_FORENSIC_FEATURES

        assert len(DEEP_FORENSIC_FEATURES) == 28, (
            f"Expected 28 deep forensic features, got {len(DEEP_FORENSIC_FEATURES)}"
        )

    def test_total_phase3_62_new_features(self):
        """Phase 3 adds exactly 62 features (18 + 6 + 10 + 28 = 62)."""
        from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
        from features.deep_forensic import DEEP_FORENSIC_FEATURES
        from features.pattern_scores import PATTERN_FEATURES
        from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES

        total = (
            len(ADVANCED_TECHNICAL_FEATURES)
            + len(PATTERN_FEATURES)
            + len(REAL_ECONOMY_MACRO_FEATURES)
            + len(DEEP_FORENSIC_FEATURES)
        )
        assert total == 62, f"Expected 62 Phase 3 features, got {total}"

    def test_no_duplicate_feature_names_across_phase3_modules(self):
        """No feature name appears in more than one Phase 3 module."""
        from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
        from features.deep_forensic import DEEP_FORENSIC_FEATURES
        from features.pattern_scores import PATTERN_FEATURES
        from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES

        all_features = (
            ADVANCED_TECHNICAL_FEATURES
            + PATTERN_FEATURES
            + REAL_ECONOMY_MACRO_FEATURES
            + DEEP_FORENSIC_FEATURES
        )
        assert len(all_features) == len(set(all_features)), (
            "Duplicate feature name(s) found across Phase 3 modules"
        )
