#!/bin/bash
set -e

# Launch Phase 2: 4 parallel backtest processes for remaining R-family strategies
# Queues: R7, R8, R10, R11 (160 jobs total)

echo "Launching Phase 2: 4 parallel processes (R7, R8, R10, R11)"
date

QUEUES=(
    "backtest/queues/r7_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r8_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r10_coverage_matrix_5band_2topn.json"
    "backtest/queues/r11_coverage_matrix_5band_2topn.json"
)

mkdir -p datastore/backtest_store/temp_dbs_phase2
mkdir -p execution_logs/parallel_db_logs_phase2

# Launch 4 processes
for idx in {0..3}; do
    queue_file="${QUEUES[$idx]}"
    db_path="datastore/backtest_store/temp_dbs_phase2/backtest_proc_${idx}.duckdb"
    log_file="execution_logs/parallel_db_logs_phase2/proc_${idx}.log"

    echo "Starting process $idx: $queue_file → $db_path"

    PYTHONPATH=. BACKTEST_DUCKDB_PATH="$db_path" \
    nohup .venv/bin/python backtest/run_sweep_inprocess.py \
        --queue-file "$queue_file" \
        --report-suffix "phase2_proc_${idx}_$(date +%s)" \
        > "$log_file" 2>&1 &

    PID=$!
    echo "PID $PID" >> "$log_file"
    echo "Process $idx started (PID $PID)"

    sleep 5
done

echo "All 4 Phase 2 processes launched (160 jobs total)"
