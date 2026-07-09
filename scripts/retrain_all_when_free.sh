#!/usr/bin/env bash
# Waits until the DuckDB write lock on alphalens.duckdb is free (i.e. the
# live daily_pipeline scheduler isn't mid-job), then runs the full model
# retrain batch (train_all_phase1 -> retrain_phase2 -> train_multibagger).
# Polls every 5 minutes; gives up after 12 hours.
set -uo pipefail
cd "$(dirname "$0")/.."

DB=datastore/normalised/alphalens.duckdb
LOG="logs/retrain_all_20260706.log"
MAX_WAIT_SECONDS=$((12 * 3600))
POLL_SECONDS=300
waited=0

lock_free() {
    PYTHONPATH=. .venv/bin/python -c "
import duckdb, sys
try:
    c = duckdb.connect('$DB', read_only=True)
    c.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null
}

echo "=== retrain_all_when_free: polling for DB lock to clear, $(date -Iseconds) ===" >> "$LOG"
while ! lock_free; do
    if [ "$waited" -ge "$MAX_WAIT_SECONDS" ]; then
        echo "=== retrain_all_when_free: gave up after 12h waiting for lock, $(date -Iseconds) ===" >> "$LOG"
        exit 1
    fi
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
done
echo "=== retrain_all_when_free: DB lock clear after ${waited}s, starting retrain batch $(date -Iseconds) ===" >> "$LOG"

{
    echo "=== train_all_phase1 (hmm_market, pnd_detector, signal_5d, signal_21d, meta_labeler, conformal_signal5d) START $(date -Iseconds) ==="
    PYTHONPATH=. .venv/bin/python -m systems.ml_signal_engine.inference.train_all_phase1 --trials 100
    rc=$?
    echo "=== train_all_phase1 END $(date -Iseconds) exit=$rc ==="

    echo "=== retrain_phase2 (signal_63d + refresh 5d/21d) START $(date -Iseconds) ==="
    PYTHONPATH=. .venv/bin/python -m systems.ml_signal_engine.inference.retrain_phase2
    rc=$?
    echo "=== retrain_phase2 END $(date -Iseconds) exit=$rc ==="

    echo "=== train_multibagger START $(date -Iseconds) ==="
    PYTHONPATH=. .venv/bin/python -m systems.ml_signal_engine.inference.train_multibagger
    rc=$?
    echo "=== train_multibagger END $(date -Iseconds) exit=$rc ==="

    echo "=== ALL RETRAINING COMPLETE $(date -Iseconds) ==="
} >> "$LOG" 2>&1
