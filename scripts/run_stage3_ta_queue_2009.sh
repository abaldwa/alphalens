#!/usr/bin/env bash
# Stage 3: the 130-job technical sweep from 2009-04-01, run in parallel.
#
# WHY THE RETRY SETTINGS ARE RAISED
# ---------------------------------
# Jobs do NOT collide with each other — batch_common.exclusive_backtest_lock is
# a system-wide flock that already serializes every job's DB tail. The collision
# seen on 2026-08-11 (template C4, job 12 of a 2-worker run, "16 retries
# exhausted") was job-tail vs the API SERVER: datastore/api/routers/backtest_runs.py
# opens BACKTEST_DUCKDB_PATH read_only on every /backtest-runs request, and a
# DuckDB reader holds a shared lock that blocks the tail's exclusive write.
#
# We cannot simply stop the API for the duration — _fetch_real_ohlcv pulls OHLCV
# through the DataStore API, so the jobs depend on it being up.
#
# Defaults are 16 attempts / 1s base / 10s cap, i.e. only ~135s of tolerance.
# One dashboard poll landing badly exhausts that. With 1 worker the tails are
# rare enough to get away with it; at 4 workers they are frequent. Raising to
# 48 attempts / 20s cap gives roughly 15 minutes, which comfortably outlasts any
# read the API can hold. These are plain env overrides (config/settings.py) —
# no code change, and nothing persists past this run.
#
# --continue-on-failure is deliberate: with 130 jobs, halting the whole sweep on
# one transient lock loss wastes hours. The queue keeps a progress file, so
# re-invoking with the SAME --report-suffix resumes and retries only what did
# not complete. ALWAYS re-run once after this finishes and confirm 130/130.
set -uo pipefail
cd /home/amit/projects/AlphaLens
export PYTHONPATH=/home/amit/projects/AlphaLens

# Process-parallel workers must not each spawn a BLAS thread pool.
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

export DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS="${DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS:-48}"
export DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S="${DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S:-1.0}"
export DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S="${DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S:-20.0}"

QUEUE="${QUEUE:-backtest/queues/ta_combined_2009.json}"
WORKERS="${WORKERS:-5}"
SUFFIX="${SUFFIX:-ta2009_combined}"

echo "=== STAGE3: $(date +%F' '%T) queue=$QUEUE workers=$WORKERS suffix=$SUFFIX ==="
echo "=== STAGE3: duckdb retry attempts=$DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS cap=${DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S}s ==="

.venv/bin/python3 backtest/run_strategy_queue.py \
    --queue-file "$QUEUE" \
    --max-workers "$WORKERS" \
    --report-suffix "$SUFFIX" \
    --continue-on-failure
STATUS=$?

echo "=== STAGE3: queue exited with status $STATUS at $(date +%F' '%T) ==="
exit $STATUS
