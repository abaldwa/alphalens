#!/bin/bash
set -e

# Unified Backtest Launch Script
# Stages 1-5: Pilot validation + M13 matrix + variant expansion
# Timeline: ~12-14 hours total

QUEUE_FILE="backtest/queues/unified_m1_m13_consolidated.json"
LOG_DIR="execution_logs"
RESULTS_DIR="backtest/reports"

echo "=========================================="
echo "AlphaLens Unified Backtest Suite"
echo "Started: $(date)"
echo "=========================================="
echo ""

# Pre-flight checks
echo "[PREFLIGHT] Checking environment..."
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 not found"
    exit 1
fi

if [ ! -f "$QUEUE_FILE" ]; then
    echo "ERROR: Queue file not found: $QUEUE_FILE"
    exit 1
fi

# Check DuckDB lock
echo "[PREFLIGHT] Checking DuckDB lock status..."
if fuser ~/.local/share/AlphaLens/data/*.duckdb 2>/dev/null; then
    echo "WARNING: DuckDB appears to be in use. Proceeding cautiously."
    sleep 5
fi

# Create cache directory
mkdir -p backtest/cache/ohlcv_snapshots

# Create directories
mkdir -p "$LOG_DIR" "$RESULTS_DIR"

# Start backtest queue
echo ""
echo "[STAGE_1-5] Launching unified backtest queue..."
echo "  Queue file: $QUEUE_FILE"
echo "  Total jobs: 124"
echo "  Expected runtime: 12-14 hours"
echo "  Stages: Pilot (4) + M13 Matrix (60) + Results Aggregation"
echo ""

# Run in background with nohup for robustness
PYTHONPATH=. nohup python3 backtest/run_strategy_queue.py --queue-file "$QUEUE_FILE" \
    > "$LOG_DIR/unified_backtest_$(date +%Y%m%d_%H%M%S).log" 2>&1 &

QUEUE_PID=$!
echo "[QUEUE_RUNNING] Process ID: $QUEUE_PID"
echo ""
echo "To monitor progress:"
echo "  tail -f $LOG_DIR/unified_backtest_*.log"
echo ""
echo "To check DuckDB lock holders:"
echo "  fuser ~/.local/share/AlphaLens/data/*.duckdb"
echo ""
echo "To check running processes:"
echo "  ps aux | grep run_strategy_queue"
echo ""

# Save PID for later reference
echo "$QUEUE_PID" > ".unified_backtest_pid"

echo "[NEXT_STEP] After backtest completes, run:"
echo "  python3 scripts/aggregate_unified_results.py"
echo ""
