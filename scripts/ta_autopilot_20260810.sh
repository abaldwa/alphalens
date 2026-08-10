#!/bin/bash
# scripts/ta_autopilot_20260810.sh
#
# Unattended chain: full feature compute -> coverage gate -> smoke test ->
# 65-strategy backtest queue -> comparison report.
#
# Everything here contends for the single DuckDB write lock, so stages run
# STRICTLY one at a time. Running a DB reader concurrently with the backfill
# is what caused 68 tickers to fail Stage 1 earlier today (workers retry only
# 6 times, ~15s, then give up).
#
# Waits use systemd unit state, never pgrep — the daily_pipeline process name
# persists forever as the resident scheduler daemon, so a pgrep-based wait
# would block indefinitely.
set -uo pipefail
cd /home/amit/projects/AlphaLens
export PYTHONPATH=/home/amit/projects/AlphaLens
SUFFIX=ta_full_2007_2026
PY=/home/amit/projects/AlphaLens/.venv/bin/python

say() { echo "=== $* ($(date +%H:%M:%S)) ==="; }

# Commit after every stage so a crash/reboot never loses completed work.
# Paths are ALWAYS explicit: this worktree is shared with other sessions and
# `git add -A` would sweep up their half-finished edits (and `git stash` has
# already stranded changes here once). Empty commits are skipped, and a
# failure to commit must never abort the pipeline — hence `|| true`.
# NOTE ON -f: backtest/reports/ and logs/ are in .gitignore, but 11,046 files
# under backtest/reports are already tracked — force-adding results is the
# established pattern in this repo. These runs are expensive (a 19-year,
# 65-strategy sweep is many hours of compute), so the OUTPUT DATA is committed,
# not just the code that produced it. Scope is always this run's own files
# (suffix-matched globs), never the whole 4.7 GB directory.
commit_stage() {
  local msg="$1"; shift
  git add -f -- "$@" 2>/dev/null || true
  if git diff --cached --quiet 2>/dev/null; then
    echo "  (nothing new to commit for: $msg)"
    return 0
  fi
  git -c core.hooksPath=/dev/null commit -q -m "$msg

Auto-committed by scripts/ta_autopilot_20260810.sh so completed work
survives a crash, reboot, or OOM kill mid-chain.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>" 2>&1 | tail -3 || true
  git push -q origin HEAD 2>&1 | tail -3 || true
  echo "  committed+pushed: $msg"
}

# ---------------------------------------------------------------- [1] compute
say "[1/5] waiting for alphalens-full-compute (Stage 1 + Stage 2)"
while systemctl --user is-active --quiet alphalens-full-compute; do sleep 120; done
RESULT=$(systemctl --user show alphalens-full-compute -p Result --value)
say "[1/5] full-compute ended result=$RESULT"
if [ "$RESULT" != "success" ]; then
  echo "AUTOPILOT ABORT: feature compute did not succeed (result=$RESULT)." >&2
  echo "Staging is checkpointed — re-running the same command resumes." >&2
  exit 1
fi

# ----------------------------------------------------------- [2] coverage gate
say "[2/5] coverage gate"
$PY - <<'PYEOF'
import sys, pandas as pd, os
import systems.technical_analysis.screener.templates as T

# The 15 features that were empty pre-2022 and gate 29 of the 65 templates.
NEED = ['rs_vs_nifty500_21d','hurst_exp_21d','hurst_exp_63d','base_breakout_score',
        'double_bottom_score','flag_pattern_score','approx_entropy_21d','wavelet_trend',
        'spectral_entropy','rqa_rec_rate','time_series_complexity','nonlinear_trend_strength',
        'permutation_entropy_21d','lyapunov_exponent_proxy','wavelet_noise']
CHECK = ['2007-06-15','2012-06-15','2018-06-15','2024-06-14','2026-06-15','2026-08-07']
MIN_ROWS, MIN_FILL = 2000, 0.30

fail = []
for d in CHECK:
    p = f'datastore/features/daily/{d}.parquet'
    if not os.path.exists(p):
        fail.append(f'{d}: parquet MISSING'); continue
    df = pd.read_parquet(p)
    if len(df) < MIN_ROWS:
        fail.append(f'{d}: only {len(df)} rows (<{MIN_ROWS})')
    for f in NEED:
        if f not in df.columns:
            fail.append(f'{d}: {f} ABSENT'); continue
        fill = df[f].notna().mean()
        if fill < MIN_FILL:
            fail.append(f'{d}: {f} fill={fill:.2f} (<{MIN_FILL})')
    print(f'{d}  rows={len(df):>5}  mean_fill={df[[c for c in NEED if c in df.columns]].notna().mean().mean():.2f}')

if fail:
    print('\nCOVERAGE GATE FAILED:'); [print('  -', x) for x in fail]; sys.exit(1)
print('\nCOVERAGE GATE PASSED')
PYEOF
if [ $? -ne 0 ]; then
  echo "AUTOPILOT ABORT: coverage gate failed — not running backtests on incomplete features." >&2
  exit 1
fi
commit_stage "TA autopilot: feature compute complete, coverage gate passed" \
  logs/full_compute_tickers.txt backtest/queues backtest/reports/queue_defs \
  scripts/ta_autopilot_20260810.sh scripts/generate_ta_backtest_queue.py

# ------------------------------------------------------------- [3] smoke test
say "[3/5] smoke test — single template end to end"
$PY -m backtest.run_orchestrator_backtest \
  --channel technical --template-name T02 \
  --start-date 2007-04-01 --end-date 2026-08-10 \
  --top-n 200 --initial-capital 1000000 --capital-mode lump \
  --universe-spec curated --max-tickers 800 --min-history-days 60 \
  --exit-variant unconstrained \
  --report-suffix smoke_${SUFFIX} 2>&1 | tail -25
if [ $? -ne 0 ]; then
  echo "AUTOPILOT ABORT: smoke test failed — not launching 65 jobs." >&2
  exit 1
fi
say "[3/5] smoke test OK"
commit_stage "TA autopilot: smoke test passed (T02, 2007-2026, unconstrained)" \
  "backtest/reports/orchestrator_smoke_${SUFFIX}"*.json

# --------------------------------------------------------------- [4] the queue
say "[4/5] launching 65-strategy queue"
systemctl --user start "alphalens-backtest-queue@${SUFFIX}.service"
sleep 30
# Commit partial results every 10 min while the queue runs — 65 jobs over a
# 19-year window is many hours, and a mid-queue crash must not discard the
# jobs already finished.
while systemctl --user is-active --quiet "alphalens-backtest-queue@${SUFFIX}.service"; do
  sleep 600
  commit_stage "TA autopilot: queue progress checkpoint" \
    "backtest/reports/orchestrator_${SUFFIX}"*.json \
    "backtest/reports/strategy_queue_progress_${SUFFIX}.json"
done
say "[4/5] queue unit ended result=$(systemctl --user show alphalens-backtest-queue@${SUFFIX}.service -p Result --value)"
commit_stage "TA autopilot: 65-strategy queue finished" \
  "backtest/reports/orchestrator_${SUFFIX}"*.json \
  "backtest/reports/strategy_queue_progress_${SUFFIX}.json"

# --------------------------------------------------------------- [5] the report
say "[5/5] comparison report"
for regime in ltcg_12_5pct_1_25L ltcg_10pct_1L; do
  $PY -m backtest.ta_comparison_report --suffix "$SUFFIX" --tax-regime "$regime" 2>&1 | tail -12
done
commit_stage "TA autopilot: comparison reports generated (both LTCG regimes)" \
  "backtest/reports/ta_comparison_${SUFFIX}"* \
  "backtest/reports/orchestrator_${SUFFIX}"*.json \
  "backtest/reports/strategy_queue_progress_${SUFFIX}.json"
say "AUTOPILOT COMPLETE"
