"""
tests/unit/test_matrix_builder.py

Phase: 1.2 (Core Feature Computation)
Specs: SPEC-SOLID-005, SPEC-DS-005, SPEC-PIPE-005, SPEC-FEAT-001
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for features/matrix_builder.py's assembly logic. Uses a fake
DataStoreClient (SPEC-SOLID-005: tests inject mock implementations via
interfaces) so this suite never touches the network or DuckDB, and never
runs the expensive HMM fit (compute_hmm=False everywhere here — HMM
fitting is covered separately and thoroughly by tests/unit/test_hmm.py).
"""

import numpy as np
import pandas as pd
import pytest

from features.calendar import CALENDAR_FEATURES
from features.intraday import INTRADAY_FEATURES
from features.macro_features import MACRO_FEATURES
from features.matrix_builder import ALL_FEATURE_COLUMNS, build_feature_matrix
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES


def _make_ohlcv_rows(n_days, seed, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=n_days)
    rng = np.random.default_rng(seed)
    base_price = 100 + rng.uniform(0, 900)
    rets = rng.normal(0.0003, 0.02, n_days)
    close = base_price * np.cumprod(1 + rets)
    open_ = close * (1 + rng.normal(0, 0.005, n_days))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.005, n_days)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.005, n_days)))
    volume = rng.integers(100_000, 5_000_000, n_days)
    delivery_pct = rng.uniform(10, 90, n_days)
    return [
        {
            "date": d.strftime("%Y-%m-%d"),
            "ticker": None,  # filled by caller
            "open": float(o),
            "high": float(h),
            "low": float(low_),
            "close": float(c),
            "volume": int(v),
            "delivery_pct": float(dp),
        }
        for d, o, h, low_, c, v, dp in zip(dates, open_, high, low, close, volume, delivery_pct)
    ]


class _FakeDataStoreClient:
    """In-memory stand-in for DataStoreClient.get_ohlcv (SPEC-SOLID-005 DI)."""

    def __init__(self, rows_by_ticker, missing_tickers=()):
        self._rows_by_ticker = rows_by_ticker
        self._missing = set(missing_tickers)

    def get_ohlcv(self, ticker, from_date, to_date):
        if ticker in self._missing:
            return []
        rows = self._rows_by_ticker.get(ticker, [])
        for r in rows:
            r["ticker"] = ticker
        return rows


@pytest.fixture
def fake_client():
    rows_a = _make_ohlcv_rows(300, seed=1)
    rows_b = _make_ohlcv_rows(300, seed=2)
    bm_rows = {name: _make_ohlcv_rows(300, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
    return _FakeDataStoreClient({"AAA": rows_a, "BBB": rows_b, **bm_rows})


class TestBuildFeatureMatrix:
    def test_empty_tickers_raises(self, fake_client):
        with pytest.raises(ValueError):
            build_feature_matrix("2024-12-20", [], client=fake_client, save=False, compute_hmm=False)

    def test_shape_matches_all_feature_columns(self, fake_client):
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert mat.shape == (2, 2 + len(ALL_FEATURE_COLUMNS))
        assert list(mat.columns) == ["date", "ticker"] + ALL_FEATURE_COLUMNS

    def test_ticker_with_no_data_gets_all_nan_row_not_dropped(self, fake_client):
        """Ticker-specific columns (technical/intraday/HMM) must be NaN for a ticker whose
        OHLCV fetch failed — but market-wide columns (calendar/macro) are still broadcast,
        since they don't depend on any individual ticker's data."""
        fake_client._missing.add("ZZZ")
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "ZZZ"], client=fake_client, save=False, compute_hmm=False
        )
        assert set(mat["ticker"]) == {"AAA", "ZZZ"}
        zzz_row = mat[mat["ticker"] == "ZZZ"]
        per_ticker_cols = CORE_TECHNICAL_FEATURES + INTRADAY_FEATURES
        assert zzz_row[per_ticker_cols].isna().all(axis=None)
        # Broadcast columns are present (not dropped) regardless of per-ticker fetch outcome.
        for col in CALENDAR_FEATURES:
            assert zzz_row[col].notna().all()

    def test_macro_columns_present_even_without_macro_indicators(self, fake_client):
        """No macro_indicators seeded for this date -> macro columns NaN, but still present, not dropped."""
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA"], client=fake_client, save=False, compute_hmm=False
        )
        for col in MACRO_FEATURES:
            assert col in mat.columns
        assert mat.loc[0, "rl_regime_label"] == 0.0  # Phase 1 stub, always populated

    def test_advance_decline_ratio_outside_price_ratio_range_is_not_flagged(self, fake_client, caplog):
        """Regression guard for the false-positive found in P1.2: advance_decline_ratio
        legitimately falls outside [0.1, 10.0] for small/lopsided universes and must not
        trip the price-ratio range warning meant for sma/ema ratios."""
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        with caplog.at_level("WARNING"):
            build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        assert not any("advance_decline_ratio" in r.message for r in caplog.records)

    def test_does_not_save_when_save_false(self, fake_client, tmp_path, monkeypatch):
        import features.matrix_builder as mb

        monkeypatch.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        build_feature_matrix(target_date, ["AAA"], client=fake_client, save=False, compute_hmm=False)
        assert list(tmp_path.iterdir()) == []

    def test_saves_parquet_when_save_true(self, fake_client, tmp_path, monkeypatch):
        import features.matrix_builder as mb

        monkeypatch.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
        target_date_ts = pd.bdate_range(start="2024-01-01", periods=300)[-1]
        build_feature_matrix(
            target_date_ts.strftime("%Y-%m-%d"), ["AAA"], client=fake_client, save=True, compute_hmm=False
        )
        expected = tmp_path / f"{target_date_ts.date().isoformat()}.parquet"
        assert expected.exists()
        roundtrip = pd.read_parquet(expected)
        assert roundtrip.shape[0] == 1
