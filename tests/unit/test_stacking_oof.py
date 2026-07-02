"""
tests/unit/test_stacking_oof.py

Phase: 3.2 (M-13 Stacking Ensemble — OOF training)
Specs: SPEC-MODEL-003, SPEC-MODEL-013

Unit tests for scripts/train_stacking.py's pure glue logic: fold-boundary
reconstruction, schema-derived feature-column selection, and the
SEQ_LEN-window slicing that feeds TFT/BiLSTM scoring in _build_deep_oof().

These are deterministic, hand-built fixtures (small parquet files, a fake
model class) exercising the alignment logic in isolation — not a
substitute for real market data (CLAUDE.md Absolute Rule 6): no code
under test here trains or scores on anything but the small real-shaped
DataFrames constructed below, and none of this ships in an application
code path.
"""

import numpy as np
import pandas as pd
import pytest

from scripts.train_stacking import (
    _build_deep_oof,
    _feature_cols_from_schema,
    _reconstruct_fold_boundaries,
    _select_fold,
)
from systems.ml_signal_engine.models.deep.tft_model import SEQ_LEN


class TestReconstructFoldBoundaries:
    def test_boundaries_increase_and_cap_at_n_files(self):
        boundaries = _reconstruct_fold_boundaries(n_files=300, n_folds=3)
        assert len(boundaries) == 3
        assert boundaries == sorted(boundaries)
        assert boundaries[-1] <= 300

    def test_small_n_files_still_respects_min_fold_files(self):
        boundaries = _reconstruct_fold_boundaries(n_files=50, n_folds=3)
        assert all(b <= 50 for b in boundaries)


class TestSelectFold:
    def test_picks_latest_fold_covering_date(self):
        dates = pd.to_datetime(pd.date_range("2024-01-01", periods=300, freq="D"))
        boundaries = [100, 200, 300]
        # position 250 -> only fold_boundaries[0]=100 and [1]=200 are <= 250+1(right)
        fold = _select_fold(dates[250], dates, boundaries)
        assert fold == 1

    def test_falls_back_to_fold_zero_for_earliest_dates(self):
        dates = pd.to_datetime(pd.date_range("2024-01-01", periods=300, freq="D"))
        boundaries = [100, 200, 300]
        fold = _select_fold(dates[5], dates, boundaries)
        assert fold == 0


class TestFeatureColsFromSchema:
    def test_drops_id_columns_keeps_features_in_order(self, tmp_path):
        df = pd.DataFrame(
            {
                "date": ["2026-01-01", "2026-01-01"],
                "ticker": ["AAA", "BBB"],
                "feat_1": [1.0, 2.0],
                "feat_2": [3.0, 4.0],
            }
        )
        path = tmp_path / "2026-01-01.parquet"
        df.to_parquet(path)
        cols = _feature_cols_from_schema(path)
        assert cols == ["feat_1", "feat_2"]


class _FakeDeepModel:
    """Duck-types TFTSignalModel/BiLSTMSignalModel's load()/predict_proba() without torch."""

    def __init__(self) -> None:
        self._loaded_path = None

    def load(self, path: str) -> None:
        self._loaded_path = path

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        # Deterministic: always predicts Hold with high confidence, one row per
        # sequence in X — enough to prove the calling code assembled valid
        # (n, SEQ_LEN, n_features) windows and can consume real model output.
        n = X.shape[0]
        return np.tile(np.array([0.1, 0.8, 0.1], dtype=np.float32), (n, 1))


class TestBuildDeepOOF:
    def _write_daily_parquets(self, tmp_path, n_days: int, tickers=("AAA", "BBB")):
        dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
        for d in dates:
            rows = []
            for t in tickers:
                rows.append({"date": d, "ticker": t, "feat_1": 1.0, "feat_2": 2.0})
            pd.DataFrame(rows).to_parquet(tmp_path / f"{d.date()}.parquet")
        return dates

    def test_scores_rows_with_full_lookback_and_skips_short_history(self, tmp_path, monkeypatch):
        import scripts.train_stacking as ts

        n_days = SEQ_LEN + 10
        dates = self._write_daily_parquets(tmp_path, n_days)
        monkeypatch.setattr(ts, "FEATURES_DAILY_DIR", tmp_path)

        model_dir = tmp_path / "models"
        model_dir.mkdir()
        # One fake checkpoint file so glob finds a "fold0" for tft_signal_21d_v20260101.
        (model_dir / "tft_signal_21d_v20260101_fold0.pt").write_bytes(b"")

        target_keys = pd.DataFrame(
            {
                "date": [dates[SEQ_LEN - 1], dates[SEQ_LEN + 2]],  # first is too early (no full window), second is valid
                "ticker": ["AAA", "AAA"],
            }
        )

        result = _build_deep_oof("tft", _FakeDeepModel, target_keys, model_dir)

        # Only the row with >= SEQ_LEN prior days should survive.
        assert len(result) == 1
        assert result.iloc[0]["ticker"] == "AAA"
        assert result.iloc[0]["date"] == dates[SEQ_LEN + 2]
        assert pytest.approx(result.iloc[0]["proba_hold"], abs=1e-6) == 0.8

    def test_missing_ticker_is_dropped_not_fabricated(self, tmp_path, monkeypatch):
        import scripts.train_stacking as ts

        n_days = SEQ_LEN + 5
        dates = self._write_daily_parquets(tmp_path, n_days, tickers=("AAA",))
        monkeypatch.setattr(ts, "FEATURES_DAILY_DIR", tmp_path)

        model_dir = tmp_path / "models"
        model_dir.mkdir()
        (model_dir / "bilstm_signal_21d_v20260101_fold0.pt").write_bytes(b"")

        target_keys = pd.DataFrame({"date": [dates[-1]], "ticker": ["ZZZ_NOT_PRESENT"]})
        result = _build_deep_oof("bilstm", _FakeDeepModel, target_keys, model_dir)
        assert result.empty

    def test_raises_when_no_checkpoint_found(self, tmp_path, monkeypatch):
        import scripts.train_stacking as ts

        dates = self._write_daily_parquets(tmp_path, SEQ_LEN + 5)
        monkeypatch.setattr(ts, "FEATURES_DAILY_DIR", tmp_path)
        model_dir = tmp_path / "empty_models"
        model_dir.mkdir()

        target_keys = pd.DataFrame({"date": [dates[-1]], "ticker": ["AAA"]})
        with pytest.raises(FileNotFoundError):
            _build_deep_oof("tft", _FakeDeepModel, target_keys, model_dir)
