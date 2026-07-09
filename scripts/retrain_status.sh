#!/usr/bin/env bash
# Quick status check for the full model retraining run kicked off 2026-07-06.
# Usage: scripts/retrain_status.sh [logfile]
set -euo pipefail
cd "$(dirname "$0")/.."

LOG="${1:-logs/retrain_all_20260706.log}"

echo "=== Collated per-stage summary ==="
PYTHONPATH=. .venv/bin/python scripts/retrain_collate.py "$LOG"

echo
echo "=== Is the runner process still alive? ==="
if pgrep -f "systems.ml_signal_engine.inference.(train_all_phase1|retrain_phase2|train_multibagger)" > /dev/null; then
    pgrep -af "systems.ml_signal_engine.inference.(train_all_phase1|retrain_phase2|train_multibagger)"
else
    echo "No training subprocess currently running (either between stages, finished, or crashed — check log tail below)."
fi

echo
echo "=== Last 20 log lines ==="
tail -n 20 "$LOG"

echo
echo "=== Registry status (datastore/models/registry.json) ==="
PYTHONPATH=. .venv/bin/python scripts/model_training_status.py
