#!/bin/bash
set -e

# Launch 4 parallel backtest processes with isolated DBs (reduced for system stability)
# VSCode crashes from systemd-oomd with 8 processes; 4 reduces peak RAM usage

echo "Launching 4 parallel backtest processes with isolated DuckDB..."
date

# Queue files to process (prioritize highest-value strategies)
QUEUES=(
    "backtest/queues/r0_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r5_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r9_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r12_coverage_matrix_5band_2topn.json"
)

mkdir -p datastore/backtest_store/temp_dbs
mkdir -p execution_logs/parallel_db_logs

# Launch 4 processes
for idx in {0..3}; do
    queue_file="${QUEUES[$idx]}"
    db_path="datastore/backtest_store/temp_dbs/backtest_proc_${idx}.duckdb"
    log_file="execution_logs/parallel_db_logs/proc_${idx}.log"

    echo "Starting process $idx: $queue_file → $db_path"

    PYTHONPATH=. BACKTEST_DUCKDB_PATH="$db_path" \
    nohup .venv/bin/python backtest/run_sweep_inprocess.py \
        --queue-file "$queue_file" \
        --report-suffix "proc_${idx}_$(date +%s)" \
        > "$log_file" 2>&1 &

    PID=$!
    echo "PID $PID" >> "$log_file"
    echo "Process $idx started (PID $PID)"

    # Stagger starts to avoid thundering herd
    sleep 5
done

echo "All 4 processes launched. Monitoring logs in execution_logs/parallel_db_logs/"
