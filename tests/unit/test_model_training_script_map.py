"""
tests/unit/test_model_training_script_map.py

Regression coverage for A38 (FeatureBacklog.md): _MODEL_TRAINING_SCRIPT_MAP
in ingestion/scheduler/pipeline_scheduler.py used to map "tft"/"bilstm" to
None ("Phase 3, not built yet"), so the weekly model_training scheduler job
(_execute_model_training_job) would never trigger them even after
train_deep_models.py became a real, working CLI. Fixed 2026-07-09: both
now map to systems.ml_signal_engine.inference.train_deep_models.
"""
import importlib.util

from ingestion.scheduler.pipeline_scheduler import _MODEL_TRAINING_SCRIPT_MAP


def test_tft_and_bilstm_are_no_longer_mapped_to_none():
    assert _MODEL_TRAINING_SCRIPT_MAP["tft"] is not None
    assert _MODEL_TRAINING_SCRIPT_MAP["bilstm"] is not None


def test_tft_and_bilstm_map_to_train_deep_models():
    assert _MODEL_TRAINING_SCRIPT_MAP["tft"] == "systems.ml_signal_engine.inference.train_deep_models"
    assert _MODEL_TRAINING_SCRIPT_MAP["bilstm"] == "systems.ml_signal_engine.inference.train_deep_models"


def test_every_mapped_module_resolves():
    """Same check _trigger_model_retrain performs before subprocess.run — a
    stale/renamed module string should fail loudly here, not silently at
    3am during the real weekly retrain job."""
    for model_name, module in _MODEL_TRAINING_SCRIPT_MAP.items():
        if module is None:
            continue
        spec = importlib.util.find_spec(module)
        assert spec is not None, f"'{model_name}' maps to unresolvable module '{module}'"


def test_tft_and_bilstm_share_one_module_for_dedup():
    """Both map to the same module string on purpose — _execute_model_training_job's
    seen_scripts dedup loop must only invoke one subprocess even if both are overdue
    in the same cycle (same pattern as train_all_phase1 covering 6 registry keys)."""
    assert _MODEL_TRAINING_SCRIPT_MAP["tft"] == _MODEL_TRAINING_SCRIPT_MAP["bilstm"]
