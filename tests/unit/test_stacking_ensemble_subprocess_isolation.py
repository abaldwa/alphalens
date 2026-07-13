"""
tests/unit/test_stacking_ensemble_subprocess_isolation.py

A40 (2026-07-13) — verifies the subprocess-isolation wiring for
StackingEnsemble's training path, mirroring ML21's pattern
(_trigger_model_retrain in ingestion/scheduler/pipeline_scheduler.py):

- scripts/train_stacking.py's new --dry-run flag runs as a real `python -m`
  subprocess and exits 0 without performing any real training or touching
  any DuckDB file (verified by pointing --output-dir at a tmp_path, so
  even the STARTED/COMPLETED status-marker JSON never lands under the
  real datastore/models/ directory).
- pipeline_scheduler.trigger_stacking_ensemble_retrain() invokes that same
  subprocess path correctly (module resolves, cwd=repo root, returns 0).

No mocks over the subprocess boundary — this is a real `python -m
scripts.train_stacking --dry-run` invocation, just scoped so it can't do
anything expensive or touch production data.
"""

import json
import subprocess
import sys
from pathlib import Path

from ingestion.scheduler.pipeline_scheduler import trigger_stacking_ensemble_retrain

_REPO_ROOT = Path(__file__).resolve().parents[2]


class TestTrainStackingDryRun:
    def test_dry_run_exits_zero_and_writes_status_markers_only(self, tmp_path):
        result = subprocess.run(
            [
                sys.executable, "-m", "scripts.train_stacking",
                "--dry-run", "--output-dir", str(tmp_path),
            ],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr
        assert "Dry run OK" in result.stdout

        status_path = tmp_path / "train_stacking.status.json"
        assert status_path.exists()
        status = json.loads(status_path.read_text())
        assert status["status"] == "COMPLETED"
        assert "dry-run" in status["detail"]


class TestTriggerStackingEnsembleRetrain:
    def test_dry_run_invokes_subprocess_and_returns_zero(self, tmp_path):
        rc = trigger_stacking_ensemble_retrain(dry_run=True, output_dir=str(tmp_path))
        assert rc == 0
        status_path = tmp_path / "train_stacking.status.json"
        assert status_path.exists()
        assert json.loads(status_path.read_text())["status"] == "COMPLETED"
