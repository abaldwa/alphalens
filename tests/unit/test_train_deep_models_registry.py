"""
tests/unit/test_train_deep_models_registry.py

Regression coverage for A38 (FeatureBacklog.md): tft/bilstm had a real,
working training CLI (train_deep_models.py) that had never been run and
never wrote a datastore/models/registry.json entry — so even a
successful run stayed invisible to pipeline_scheduler.py's overdue-retrain
check. Fixed 2026-07-09: schedule_overnight_training() in tft_model.py/
bilstm_model.py now returns {"folds_trained", "last_model_path"}, and
train_deep_models.py::_update_registry() writes last_trained_date/
training_interval_days from it, mirroring train_all_phase1.py's
_save_model() convention.

These tests exercise _update_registry() and the _train_tft/_train_bilstm
wrappers directly (fast, no real torch training) rather than running a
full overnight training job.
"""
import argparse
import json


from systems.ml_signal_engine.inference.train_deep_models import (
    _train_bilstm,
    _train_tft,
    _update_registry,
)


class TestUpdateRegistry:
    def test_writes_new_entry_when_folds_trained(self, tmp_path):
        result = {"folds_trained": 3, "last_model_path": "datastore/models/tft_signal_21d_v20260709_fold2.pt"}
        _update_registry("tft", str(tmp_path), result, horizon_days=21)

        registry = json.loads((tmp_path / "registry.json").read_text())
        assert registry["tft"]["folds_trained"] == 3
        assert registry["tft"]["horizon_days"] == 21
        assert registry["tft"]["saved_path"] == result["last_model_path"]
        assert "last_trained_date" in registry["tft"]
        assert "training_interval_days" in registry["tft"]

    def test_noop_when_zero_folds_trained(self, tmp_path):
        result = {"folds_trained": 0, "last_model_path": None}
        _update_registry("bilstm", str(tmp_path), result, horizon_days=21)

        assert not (tmp_path / "registry.json").exists()

    def test_does_not_clobber_existing_last_trained_date_on_zero_folds(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "bilstm": {"last_trained_date": "2026-01-01", "training_interval_days": 30},
            "tft": {"last_trained_date": "2026-02-02", "training_interval_days": 30},
        }))

        _update_registry("bilstm", str(tmp_path), {"folds_trained": 0, "last_model_path": None}, horizon_days=21)

        registry = json.loads(registry_path.read_text())
        assert registry["bilstm"]["last_trained_date"] == "2026-01-01"
        assert registry["tft"]["last_trained_date"] == "2026-02-02"

    def test_merges_with_existing_registry_entries(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "signal_5d": {"last_trained_date": "2026-07-06", "training_interval_days": 30},
        }))

        _update_registry("tft", str(tmp_path), {"folds_trained": 1, "last_model_path": "x.pt"}, horizon_days=21)

        registry = json.loads(registry_path.read_text())
        assert registry["signal_5d"]["last_trained_date"] == "2026-07-06"
        assert "tft" in registry

    def test_overwrites_prior_tft_entry_on_successful_retrain(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "tft": {"last_trained_date": "2026-01-01", "training_interval_days": 30, "folds_trained": 1},
        }))

        _update_registry("tft", str(tmp_path), {"folds_trained": 5, "last_model_path": "new.pt"}, horizon_days=21)

        registry = json.loads(registry_path.read_text())
        assert registry["tft"]["folds_trained"] == 5
        assert registry["tft"]["last_trained_date"] != "2026-01-01"


class TestTrainWrappersCallUpdateRegistry:
    """_train_tft/_train_bilstm must call schedule_overnight_training then
    _update_registry with the right model_name — the actual A38 wiring."""

    def _args(self, tmp_path) -> argparse.Namespace:
        return argparse.Namespace(
            horizon=21, folds=2, quick=True, output_dir=str(tmp_path),
        )

    def test_train_tft_registers_under_tft_key(self, tmp_path, monkeypatch):
        fake_result = {"folds_trained": 2, "last_model_path": "tft_fold1.pt"}
        monkeypatch.setattr(
            "systems.ml_signal_engine.models.deep.tft_model.schedule_overnight_training",
            lambda **kwargs: fake_result,
        )
        _train_tft(self._args(tmp_path))
        registry = json.loads((tmp_path / "registry.json").read_text())
        assert registry["tft"]["folds_trained"] == 2
        assert "bilstm" not in registry

    def test_train_bilstm_registers_under_bilstm_key(self, tmp_path, monkeypatch):
        fake_result = {"folds_trained": 1, "last_model_path": "bilstm_fold0.pt"}
        monkeypatch.setattr(
            "systems.ml_signal_engine.models.deep.bilstm_model.schedule_overnight_training",
            lambda **kwargs: fake_result,
        )
        _train_bilstm(self._args(tmp_path))
        registry = json.loads((tmp_path / "registry.json").read_text())
        assert registry["bilstm"]["folds_trained"] == 1
        assert "tft" not in registry
