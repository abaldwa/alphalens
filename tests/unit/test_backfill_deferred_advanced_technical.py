"""
tests/unit/test_backfill_deferred_advanced_technical.py

Unit tests for scripts/backfill_deferred_advanced_technical.py — the
job that fills in the 17 advanced_technical features the fast live daily
pipeline path (advanced_technical_used_only=True, default since 2026-08-04)
leaves NaN. Tests merge correctness (other columns untouched) and
idempotency (already-covered dates skipped), matching the existing
test_precompute_technical_screener_matches.py test-shape convention.
"""

from datetime import date

import numpy as np
import pandas as pd

from scripts.backfill_deferred_advanced_technical import (
    DEFERRED_COLUMNS,
    _already_covers,
    backfill_one_date,
    run_backfill,
)


def _make_ohlcv_rows(n_days, seed, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=n_days)
    rng = np.random.default_rng(seed)
    base_price = 100 + rng.uniform(0, 900)
    rets = rng.normal(0.0003, 0.02, n_days)
    close = base_price * np.cumprod(1 + rets)
    volume = rng.integers(100_000, 5_000_000, n_days)
    return [
        {
            "date": d.strftime("%Y-%m-%d"), "ticker": None,
            "open": float(c), "high": float(c), "low": float(c), "close": float(c),
            "volume": int(v), "delivery_pct": 50.0,
        }
        for d, c, v in zip(dates, close, volume)
    ]


class _FakeClient:
    def __init__(self, rows_by_ticker):
        self._rows_by_ticker = rows_by_ticker

    def get_ohlcv(self, ticker, from_date, to_date):
        rows = self._rows_by_ticker.get(ticker, [])
        for r in rows:
            r["ticker"] = ticker
        return rows


def _fast_path_matrix_row(ticker: str, hurst_val: float = 0.55) -> dict:
    """A row shaped like what advanced_technical_used_only=True produces:
    hurst_exp_21d populated, the other 17 columns NaN — plus a couple of
    unrelated columns (sma_200_ratio) to prove those survive the merge
    untouched."""
    row = {"date": "2026-08-04", "ticker": ticker, "hurst_exp_21d": hurst_val, "sma_200_ratio": 1.05}
    for col in DEFERRED_COLUMNS:
        row[col] = np.nan
    return row


class TestAlreadyCovers:
    def test_missing_file_is_not_covered(self, tmp_path):
        assert _already_covers(tmp_path / "2026-08-04.parquet") is False

    def test_all_nan_wavelet_trend_is_not_covered(self, tmp_path):
        path = tmp_path / "2026-08-04.parquet"
        pd.DataFrame([_fast_path_matrix_row("AAA")]).to_parquet(path)
        assert _already_covers(path) is False

    def test_populated_wavelet_trend_is_covered(self, tmp_path):
        path = tmp_path / "2026-08-04.parquet"
        row = _fast_path_matrix_row("AAA")
        row["wavelet_trend"] = 0.1
        pd.DataFrame([row]).to_parquet(path)
        assert _already_covers(path) is True

    def test_corrupt_parquet_is_not_covered(self, tmp_path):
        path = tmp_path / "2026-08-04.parquet"
        path.write_text("not a real parquet file")
        assert _already_covers(path) is False


class TestBackfillOneDate:
    def test_no_existing_parquet_returns_false(self, tmp_path, monkeypatch):
        import config.settings as settings

        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", tmp_path)
        result = backfill_one_date(date(2026, 8, 4), client=_FakeClient({}))
        assert result is False

    def test_already_covered_date_is_skipped(self, tmp_path, monkeypatch):
        import config.settings as settings

        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", tmp_path)
        path = tmp_path / "2026-08-04.parquet"
        row = _fast_path_matrix_row("AAA")
        row["wavelet_trend"] = 0.1
        pd.DataFrame([row]).to_parquet(path)

        result = backfill_one_date(date(2026, 8, 4), client=_FakeClient({}))
        assert result is False

    def test_merges_deferred_columns_without_touching_others(self, tmp_path, monkeypatch):
        import config.settings as settings
        import config.universe as universe_mod

        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", tmp_path)
        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA", "BBB"])

        # Target date must be a real trading day present in the synthetic
        # OHLCV data (its last bar), since backfill_one_date filters
        # compute_advanced_technical_features's output to date == target.
        target_ts = pd.bdate_range(start="2025-06-01", periods=300)[-1]
        target_date = target_ts.date()
        path = tmp_path / f"{target_date.isoformat()}.parquet"
        original = pd.DataFrame([
            {**_fast_path_matrix_row("AAA"), "date": target_date.isoformat()},
            {**_fast_path_matrix_row("BBB"), "date": target_date.isoformat()},
        ])
        original.to_parquet(path)

        rows = {
            "AAA": _make_ohlcv_rows(300, seed=1, start="2025-06-01"),
            "BBB": _make_ohlcv_rows(300, seed=2, start="2025-06-01"),
        }
        client = _FakeClient(rows)

        result = backfill_one_date(target_date, client=client)
        assert result is True

        updated = pd.read_parquet(path)
        assert list(updated.columns) == list(original.columns), "column order must be preserved"
        assert updated["date"].tolist() == original["date"].tolist()
        # hurst_exp_21d and sma_200_ratio (not a deferred column) untouched.
        pd.testing.assert_series_equal(updated["hurst_exp_21d"], original["hurst_exp_21d"])
        pd.testing.assert_series_equal(updated["sma_200_ratio"], original["sma_200_ratio"])
        # At least one deferred column now has real (non-NaN) values.
        assert updated["wavelet_trend"].notna().any()

    def test_no_ohlcv_data_returns_false(self, tmp_path, monkeypatch):
        import config.settings as settings
        import config.universe as universe_mod

        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", tmp_path)
        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA"])

        path = tmp_path / "2026-08-04.parquet"
        pd.DataFrame([_fast_path_matrix_row("AAA")]).to_parquet(path)

        result = backfill_one_date(date(2026, 8, 4), client=_FakeClient({}))
        assert result is False


class TestRunBackfill:
    def test_counts_updated_and_skipped_across_range(self, tmp_path, monkeypatch):
        import config.settings as settings
        import config.universe as universe_mod

        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", tmp_path)
        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA"])

        # Day 1: needs backfill.
        (tmp_path / "2026-08-03.parquet").write_bytes(b"")
        pd.DataFrame([_fast_path_matrix_row("AAA")]).to_parquet(tmp_path / "2026-08-03.parquet")
        # Day 2: already covered.
        row = _fast_path_matrix_row("AAA")
        row["wavelet_trend"] = 0.2
        pd.DataFrame([row]).to_parquet(tmp_path / "2026-08-04.parquet")

        rows = {"AAA": _make_ohlcv_rows(300, seed=1, start="2025-06-01")}
        client = _FakeClient(rows)

        result = run_backfill(date(2026, 8, 3), date(2026, 8, 4), client=client)
        assert result == {"n_updated": 1, "n_skipped": 1}
