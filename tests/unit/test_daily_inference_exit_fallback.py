"""
tests/unit/test_daily_inference_exit_fallback.py

Regression coverage for A39 (FeatureBacklog.md): _step_exit in
systems/ml_signal_engine/inference/daily_inference.py used to load
ExitSignalModel unconditionally via _load_model(), which called
joblib.load() on a hardcoded path with no existence check. The first time
paper trading opened a position and no ExitSignalModel had ever been
trained (true in production as of 2026-07-09 — MIN_CLOSED_POSITIONS=200
closed trades never accumulated), this raised FileNotFoundError inside
_step_exit, which run_daily_inference's `except Exception: raise` then
propagated out uncaught, halting the entire daily inference pipeline.

Fixed 2026-07-09: added _load_exit_model(), which falls back to
RuleBasedExitPolicy() (the same mechanical, no-arg-instantiable exit
policy scripts/run_daily_paper_trading.py already uses via
_load_exit_policy()) when {EXIT_MODEL_NAME}_current.pkl does not exist.
"""
import pandas as pd
import pytest

from systems.ml_signal_engine.inference.daily_inference import (
    EXIT_MODEL_NAME,
    _load_exit_model,
    _step_exit,
)
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy


class TestLoadExitModel:
    def test_falls_back_to_rule_based_when_no_trained_model_exists(self, tmp_path):
        model = _load_exit_model(tmp_path)
        assert isinstance(model, RuleBasedExitPolicy)

    def test_does_not_raise_filenotfounderror(self, tmp_path):
        # Pre-fix: this raised FileNotFoundError and killed run_daily_inference.
        try:
            _load_exit_model(tmp_path)
        except FileNotFoundError:
            pytest.fail("_load_exit_model must not raise FileNotFoundError — should fall back")

    def test_loads_trained_model_when_present(self, tmp_path, monkeypatch):
        loaded_paths = []

        class FakeExitSignalModel:
            def load(self, path):
                loaded_paths.append(path)

        monkeypatch.setattr(
            "systems.ml_signal_engine.inference.daily_inference.ExitSignalModel",
            FakeExitSignalModel,
        )
        model_dir = tmp_path / EXIT_MODEL_NAME
        model_dir.mkdir()
        (model_dir / f"{EXIT_MODEL_NAME}_current.pkl").write_bytes(b"fake")

        model = _load_exit_model(tmp_path)
        assert isinstance(model, FakeExitSignalModel)
        assert loaded_paths == [str(model_dir / f"{EXIT_MODEL_NAME}_current.pkl")]


class TestStepExitUsesFallback:
    def test_step_exit_does_not_crash_when_no_trained_exit_model(self, tmp_path):
        """End-to-end: _step_exit must complete (not raise) using
        RuleBasedExitPolicy when no ExitSignalModel has ever been trained —
        the exact scenario that used to halt run_daily_inference in
        production the first time paper trading opened a position."""
        position_context = pd.DataFrame({
            "ticker": ["RELIANCE", "TCS"],
            "entry_price": [2500.0, 3500.0],
            "days_held": [3, 10],
            "unrealised_pnl_pct": [0.05, -0.02],
            "days_to_next_earnings": [30, 15],
            "drawdown_from_peak": [0.01, 0.03],
            "momentum_3m": [0.1, -0.05],
            "pnd_score": [0.0, 0.0],
            "hmm_regime": [0, 1],
        })

        class FakeClient:
            def post(self, *args, **kwargs):
                class R:
                    status_code = 200

                    def raise_for_status(self):
                        pass

                return R()

        urgent = _step_exit(position_context, __import__("datetime").date(2026, 7, 9), FakeClient(), "http://fake", tmp_path)
        assert isinstance(urgent, list)

    def test_step_exit_returns_empty_list_for_empty_position_context(self, tmp_path):
        urgent = _step_exit(pd.DataFrame(), __import__("datetime").date(2026, 7, 9), None, "http://fake", tmp_path)
        assert urgent == []
