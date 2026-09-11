"""
tests/unit/test_baseline_runner.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-DS-007
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/quality/baseline_runner.py, against a temp
directory of per-date feature Parquets (mirrors FEATURES_DAILY_DIR).
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from ingestion.quality import baseline_runner


def _write_feature_parquet(features_dir, d, tickers, feature_cols):
    """Writes one date's feature Parquet with the given CORE_TECHNICAL_FEATURES subset."""
    rows = {
        "date": [d] * len(tickers),
        "ticker": tickers,
    }
    for i, col in enumerate(feature_cols):
        rows[col] = [10.0 + i + j for j in range(len(tickers))]
    pd.DataFrame(rows).to_parquet(features_dir / f"{d.isoformat()}.parquet")


# ===== load_feature_history =====


def test_load_feature_history_returns_expected_columns_and_rows(tmp_path, monkeypatch):
    feature_cols = ["pct_rank_5d", "sma_20_ratio"]
    monkeypatch.setattr(baseline_runner, "CORE_TECHNICAL_FEATURES", feature_cols)
    for i in range(5):
        _write_feature_parquet(tmp_path, date(2026, 1, 1) + timedelta(days=i), ["AAA"], feature_cols)

    df = baseline_runner.load_feature_history(features_dir=tmp_path, end_date=date(2026, 1, 10), years=2)

    assert list(df.columns) == ["date", "ticker", "pct_rank_5d", "sma_20_ratio"]
    assert len(df) == 5
    assert set(df["ticker"]) == {"AAA"}


def test_load_feature_history_filters_to_window(tmp_path, monkeypatch):
    feature_cols = ["pct_rank_5d"]
    monkeypatch.setattr(baseline_runner, "CORE_TECHNICAL_FEATURES", feature_cols)
    in_window = date(2026, 1, 1)
    out_of_window = date(2020, 1, 1)
    _write_feature_parquet(tmp_path, in_window, ["AAA"], feature_cols)
    _write_feature_parquet(tmp_path, out_of_window, ["AAA"], feature_cols)

    df = baseline_runner.load_feature_history(features_dir=tmp_path, end_date=date(2026, 1, 10), years=2)

    assert len(df) == 1
    assert pd.Timestamp(df.iloc[0]["date"]) == pd.Timestamp(in_window)


def test_load_feature_history_only_keeps_columns_present_in_core_technical_features(tmp_path, monkeypatch):
    """Parquets may have extra non-technical columns (fundamentals etc.) — only the
    CORE_TECHNICAL_FEATURES subset that's actually present should be kept."""
    monkeypatch.setattr(baseline_runner, "CORE_TECHNICAL_FEATURES", ["pct_rank_5d", "not_in_this_parquet"])
    d = date(2026, 1, 1)
    pd.DataFrame({"date": [d], "ticker": ["AAA"], "pct_rank_5d": [1.0], "some_other_feature": [2.0]}).to_parquet(
        tmp_path / f"{d.isoformat()}.parquet"
    )

    df = baseline_runner.load_feature_history(features_dir=tmp_path, end_date=date(2026, 1, 10), years=2)

    assert list(df.columns) == ["date", "ticker", "pct_rank_5d"]


def test_load_feature_history_raises_file_not_found_when_empty(tmp_path, monkeypatch):
    """SPEC-PIPE-005: no feature Parquets in the window must raise a clear, actionable error."""
    monkeypatch.setattr(baseline_runner, "CORE_TECHNICAL_FEATURES", ["pct_rank_5d"])
    with pytest.raises(FileNotFoundError, match="Run the feature backfill"):
        baseline_runner.load_feature_history(features_dir=tmp_path, end_date=date(2026, 1, 10), years=2)


# ===== run() =====


def test_run_computes_and_persists_baseline(monkeypatch, tmp_path):
    feature_cols = ["pct_rank_5d", "sma_20_ratio"]
    monkeypatch.setattr(baseline_runner, "CORE_TECHNICAL_FEATURES", feature_cols)
    features_dir = tmp_path / "daily"
    features_dir.mkdir()
    for i in range(10):
        _write_feature_parquet(features_dir, date(2026, 1, 1) + timedelta(days=i), ["AAA", "BBB"], feature_cols)

    baseline_pkl = tmp_path / "stats_baseline.pkl"
    monkeypatch.setattr("ingestion.quality.drift_monitor.PSI_BASELINE_PATH", baseline_pkl)

    result = baseline_runner.run(features_dir=features_dir, end_date=date(2026, 1, 20), years=2)

    assert set(result.keys()) == {"pct_rank_5d", "sma_20_ratio"}
    assert baseline_pkl.exists()
