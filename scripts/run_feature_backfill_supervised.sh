#!/bin/bash
# scripts/run_feature_backfill_supervised.sh
#
# ExecStart target for the alphalens-feature-backfill.service systemd
# --user unit (see below) — supervises scripts/feature_backfill.py so an
# OOM kill / crash mid-run gets automatically restarted instead of
# requiring a human to notice and relaunch manually (this laptop has real,
# repeated precedent for exactly that this session — systemd-oomd killed
# the whole desktop session mid-backfill more than once).
#
# --force-only-once handling: scripts/feature_backfill.py itself now
# tracks, per run_id, whether --force has already been "spent" (a sentinel
# file under datastore/features/daily/.<run-id>.force_applied — see that
# script's module docstring / --force help text). That means THIS wrapper
# can always pass --force unconditionally on every attempt (first launch
# and every automatic restart alike) without ever redoing already-finished
# work after a restart — the script itself downgrades --force to a no-op
# once it's already been applied once for this run_id. This wrapper does
# NOT need its own separate "strip --force after attempt 1" logic as a
# result; it is deliberately simple.
#
# Runs in a loop so a restart happens even if this wrapper's own systemd
# unit does not have Restart= configured (defense in depth) — but the
# systemd unit below IS also configured with Restart=on-failure, so the
# wrapper process itself gets relaunched if IT is what got killed (e.g.
# systemd-oomd picks this bash process rather than the python child).
#
# Env vars (all optional, mirror scripts/feature_backfill.py's own CLI
# flags so this wrapper doesn't need to be edited per-run):
#   RUN_ID          (default: alphalens-feature-backfill — a fixed,
#                    non-timestamped id so the --force-only-once sentinel
#                    and panel_staging resume both key off the SAME run_id
#                    across every restart of this systemd unit; override
#                    if launching a genuinely new/separate run)
#   FROM_DATE       (default: script's own default, 2007-01-03)
#   TO_DATE         (default: script's own default, today)
#   FORCE           ("1" to pass --force; default unset/no --force — only
#                    set this for a genuinely fresh run, since the
#                    downstream sentinel logic only protects against
#                    REDUNDANT re-forcing, not against forcing a run that
#                    never wanted --force in the first place)
#   PANEL_WORKERS   (default: 8 — NOT 12; 12 simultaneous workers was
#                    implicated in a sharp temporary memory dip during a
#                    real launch this session on this 16GB laptop with a
#                    real OOM-kill history)
#   NO_HMM          ("1" to pass --no-hmm; default unset)
#   MAX_RESTARTS    (default: 0 = unlimited; set a positive integer to cap
#                    restart attempts, e.g. for a bounded smoke test)
set -uo pipefail

REPO_DIR="/home/amit/projects/AlphaLens"
cd "$REPO_DIR"
export PYTHONPATH="$REPO_DIR"

RUN_ID="${RUN_ID:-alphalens-feature-backfill}"
PANEL_WORKERS="${PANEL_WORKERS:-8}"
RESTART_SEC="${RESTART_SEC:-30}"
MAX_RESTARTS="${MAX_RESTARTS:-0}"

ARGS=(--run-id "$RUN_ID" --panel-workers "$PANEL_WORKERS")
[ -n "${FROM_DATE:-}" ] && ARGS+=(--from-date "$FROM_DATE")
[ -n "${TO_DATE:-}" ] && ARGS+=(--to-date "$TO_DATE")
[ "${FORCE:-0}" = "1" ] && ARGS+=(--force)
[ "${NO_HMM:-0}" = "1" ] && ARGS+=(--no-hmm)

attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "run_feature_backfill_supervised: attempt $attempt — .venv/bin/python3 scripts/feature_backfill.py ${ARGS[*]}"
  "$REPO_DIR/.venv/bin/python3" scripts/feature_backfill.py "${ARGS[@]}"
  exit_code=$?

  if [ "$exit_code" -eq 0 ]; then
    echo "run_feature_backfill_supervised: completed successfully (exit 0) on attempt $attempt"
    exit 0
  fi

  echo "run_feature_backfill_supervised: attempt $attempt exited $exit_code (crash/kill) — retrying in ${RESTART_SEC}s"

  if [ "$MAX_RESTARTS" != "0" ] && [ "$attempt" -ge "$MAX_RESTARTS" ]; then
    echo "run_feature_backfill_supervised: reached MAX_RESTARTS=$MAX_RESTARTS — giving up"
    exit "$exit_code"
  fi

  sleep "$RESTART_SEC"
done
