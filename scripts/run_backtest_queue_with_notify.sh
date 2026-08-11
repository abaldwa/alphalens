#!/bin/bash
# scripts/run_backtest_queue_with_notify.sh <report-suffix>
#
# ExecStart target for the alphalens-backtest-queue@<report-suffix>.service
# systemd --user template unit — one instance per queue definition file at
# backtest/reports/queue_defs/<report-suffix>.json, so the SAME auto-resume
# + completion-popup behavior covers any queue: Technical, Fundamental,
# Momentum, and ML (backtest/run_strategy_queue.py's job "kind" — orchestrator
# vs iterative_retrain — is what selects the channel/subprocess per job, not
# this script; it's channel-agnostic by construction).
#
# Runs the queue (resume-aware since the 2026-07-25 fix, so re-invoking
# after a crash/reboot only re-runs jobs not already marked "completed"),
# then:
#   - if the queue is fully cleared (no queued/running jobs left in the
#     progress file), pops a desktop notification and disables + stops this
#     instance so it stops auto-resuming on future boots/logins — user
#     request: "auto-resume... till the queue is cleared."
#   - otherwise (process was killed mid-run) exits normally; systemd's
#     Restart=on-failure / next-boot start picks the remainder back up.
set -uo pipefail

REPORT_SUFFIX="${1:?usage: run_backtest_queue_with_notify.sh <report-suffix>}"
REPO_DIR="/home/amit/projects/AlphaLens"
QUEUE_FILE="$REPO_DIR/backtest/reports/queue_defs/${REPORT_SUFFIX}.json"
PROGRESS_FILE="$REPO_DIR/backtest/reports/strategy_queue_progress_${REPORT_SUFFIX}.json"

if [ ! -f "$QUEUE_FILE" ]; then
  echo "run_backtest_queue_with_notify: no queue file at $QUEUE_FILE — nothing to run" >&2
  exit 1
fi

cd "$REPO_DIR"
export PYTHONPATH="$REPO_DIR"

# Parallelism. Default stays 1 (the long-standing serial behaviour); override
# per-invocation with BACKTEST_QUEUE_WORKERS.
#
# [2026-08-11] Measured before enabling: the cgroup showed 7 GB "in use", which
# looked like a memory wall and was the reason parallelism was rejected earlier
# that day. memory.stat showed the truth -- anon 1.82 GB, file 5.15 GB, i.e.
# 73% reclaimable page cache -- with memory.pressure some/full avg10 = 0.00.
# The jobs were never memory-bound; CPU sat 80% idle across 14 cores.
#
# BLAS PINNING IS NOT OPTIONAL HERE. Each worker's numpy/scipy would otherwise
# spawn one BLAS thread per core, so N workers oversubscribe the CPU N-fold and
# run SLOWER than serial. Pinning to 1 thread per worker was worth +62% on the
# feature-compute side for exactly this reason.
BACKTEST_QUEUE_WORKERS="${BACKTEST_QUEUE_WORKERS:-1}"
if [ "$BACKTEST_QUEUE_WORKERS" -gt 1 ]; then
  export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
         VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
  echo "run_backtest_queue_with_notify: ${BACKTEST_QUEUE_WORKERS} workers, BLAS pinned to 1 thread each"
fi

"$REPO_DIR/.venv/bin/python" -m backtest.run_strategy_queue \
  --queue-file "$QUEUE_FILE" \
  --continue-on-failure \
  --max-workers "$BACKTEST_QUEUE_WORKERS" \
  --report-suffix "$REPORT_SUFFIX"
run_exit=$?

remaining=$("$REPO_DIR/.venv/bin/python" - "$PROGRESS_FILE" <<'PYEOF'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except (FileNotFoundError, json.JSONDecodeError):
    print(-1)
    sys.exit(0)
statuses = [j["status"] for j in d["jobs"]]
print(sum(1 for s in statuses if s in ("queued", "running")))
PYEOF
)

if [ "$remaining" = "0" ]; then
  export DISPLAY=:0
  export XDG_RUNTIME_DIR="/run/user/$(id -u)"
  notify-send -u critical "AlphaLens Backtest Queue" \
    "All strategies cleared — every job in ${REPORT_SUFFIX} reached a terminal state. Auto-resume has been turned off." \
    || true
  systemctl --user disable --now "alphalens-backtest-queue@${REPORT_SUFFIX}.service" || true
fi

exit "$run_exit"
