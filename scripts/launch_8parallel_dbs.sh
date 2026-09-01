#!/bin/bash
set -e

# Launch 8 parallel backtest processes with isolated DBs (avoid API saturation)
# Stagger starts by 5s to prevent thundering herd on initial API calls

echo "Launching 8 parallel backtest processes with isolated DuckDB..."
date

# Queue files to process (prioritize stalled coverage_matrix + failed validation)
QUEUES=(
    "backtest/queues/r0_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r5_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r7_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r8_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r9_coverage_matrix_5band_2topn_3lookback.json"
    "backtest/queues/r10_coverage_matrix_5band_2topn.json"
    "backtest/queues/r11_coverage_matrix_5band_2topn.json"
    "backtest/queues/r12_coverage_matrix_5band_2topn.json"
)

mkdir -p datastore/backtest_store/temp_dbs
mkdir -p execution_logs/parallel_db_logs

# Launch 8 processes
for idx in {0..7}; do
    queue_file="${QUEUES[$idx]}"
    db_path="datastore/backtest_store/temp_dbs/backtest_proc_${idx}.duckdb"
    log_file="execution_logs/parallel_db_logs/proc_${idx}.log"

    echo "Starting process $idx: $queue_file → $db_path"

    PYTHONPATH=. BACKTEST_DUCKDB_PATH="$db_path" \
    nohup python3 backtest/run_sweep_inprocess.py \
        --queue-file "$queue_file" \
        --report-suffix "proc_${idx}_$(date +%s)" \
        > "$log_file" 2>&1 &

    PID=$!
    echo "PID $PID" >> "$log_file"
    echo "Process $idx started (PID $PID)"

    # Stagger starts to avoid thundering herd
    sleep 5
done

echo "All 8 processes launched. Monitoring logs in execution_logs/parallel_db_logs/"
