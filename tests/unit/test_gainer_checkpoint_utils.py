"""
tests/unit/test_gainer_checkpoint_utils.py

Coverage for systems/ml_signal_engine_gainer/inference/checkpoint_utils.py
— previously untested (0% coverage). Real tmp-path parquet files, no
mocking.
"""

import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.inference import checkpoint_utils as cu


@pytest.fixture(autouse=True)
def _isolated_checkpoint_root(tmp_path, monkeypatch):
    monkeypatch.setattr(cu, "CHECKPOINT_ROOT", tmp_path / "checkpoints")


class TestTickerChunks:
    def test_splits_into_correct_chunk_sizes(self):
        tickers = [f"T{i:03d}" for i in range(10)]
        chunks = list(cu.ticker_chunks(tickers, chunk_size=4))
        assert [len(c) for c in chunks] == [4, 4, 2]

    def test_chunks_are_sorted(self):
        tickers = ["ZEBRA", "ALPHA", "MID"]
        chunks = list(cu.ticker_chunks(tickers, chunk_size=10))
        assert chunks == [["ALPHA", "MID", "ZEBRA"]]

    def test_empty_tickers_yields_no_chunks(self):
        assert list(cu.ticker_chunks([])) == []


class TestCheckpointPath:
    def test_creates_stage_directory_and_returns_expected_path(self):
        path = cu.checkpoint_path("gainer_signal_6d", "labeled_features", "chunk0")
        assert path.parent.exists()
        assert path.name == "chunk_chunk0.parquet"
        assert "gainer_signal_6d" in str(path)
        assert "labeled_features" in str(path)


class TestSaveLoadCheckpointRoundTrip:
    def test_save_then_load_returns_equal_dataframe(self):
        df = pd.DataFrame({"ticker": ["A", "B"], "value": [1.0, 2.0]})
        path = cu.checkpoint_path("multibagger_2x_12m", "labeled_features", "chunk0")
        cu.save_checkpoint(df, path)

        loaded = cu.load_checkpoint(path)
        pd.testing.assert_frame_equal(loaded, df)

    def test_save_uses_atomic_rename_no_tmp_file_left_behind(self):
        df = pd.DataFrame({"x": [1]})
        path = cu.checkpoint_path("p", "s", "c1")
        cu.save_checkpoint(df, path)
        assert path.exists()
        assert not path.with_suffix(".parquet.tmp").exists()

    def test_load_checkpoint_missing_file_returns_none(self):
        path = cu.checkpoint_path("p", "s", "does_not_exist")
        assert cu.load_checkpoint(path) is None

    def test_load_checkpoint_corrupt_file_returns_none_not_raise(self, tmp_path):
        path = cu.checkpoint_path("p", "s", "corrupt")
        path.write_text("not a real parquet file")
        assert cu.load_checkpoint(path) is None


class TestLoadAllCheckpoints:
    def test_concatenates_every_chunk_for_stage(self):
        for i, ticker in enumerate(["A", "B", "C"]):
            df = pd.DataFrame({"ticker": [ticker], "value": [i]})
            path = cu.checkpoint_path("gainer_signal_6d", "labeled_features", f"chunk{i}")
            cu.save_checkpoint(df, path)

        combined = cu.load_all_checkpoints("gainer_signal_6d", "labeled_features")
        assert len(combined) == 3
        assert sorted(combined["ticker"]) == ["A", "B", "C"]

    def test_missing_stage_directory_returns_empty_dataframe(self):
        combined = cu.load_all_checkpoints("never_ran", "no_such_stage")
        assert combined.empty

    def test_stage_directory_exists_but_no_chunks_returns_empty(self):
        path = cu.checkpoint_path("p2", "s2", "placeholder")
        path.parent.mkdir(parents=True, exist_ok=True)  # dir exists, no chunk files
        combined = cu.load_all_checkpoints("p2", "s2")
        assert combined.empty
