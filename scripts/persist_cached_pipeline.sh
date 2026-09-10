#!/bin/bash
# scripts/persist_cached_pipeline.sh
#
# [2026-09-10] Companion to scripts/evening_cache_pipeline.sh. Run this ONLY
# when you explicitly decide to publish everything that pipeline cached
# locally. Never auto-triggered by anything — that's the whole point: fetch
# runs freely from 6pm onward regardless of who holds the DB lock, and
# publish happens on your say-so, whenever the lock is actually free.
#
# Usage:
#   bash scripts/persist_cached_pipeline.sh \
#       --nse-xbrl-cache datastore/cache/evening_20260910/nse_xbrl_delta.json \
#       --corp-actions-cache datastore/cache/evening_20260910/corporate_actions.parquet \
#       --fyers-from 2026-09-05 --fyers-to 2026-09-10
#
# Any --*-cache arg can be omitted to skip that job (e.g. if that fetch
# failed or wasn't run). FYERS dates are required if you want it persisted;
# omit both --fyers-from/--fyers-to to skip FYERS persist entirely.
set -a
source /home/amit/projects/AlphaLens/.env
set +a
cd /home/amit/projects/AlphaLens

LOG="/tmp/persist_cached_pipeline.log"
exec > >(tee -a "$LOG")
exec 2>&1

NSE_XBRL_CACHE=""
CORP_ACTIONS_CACHE=""
FYERS_FROM=""
FYERS_TO=""
MF_HOLDINGS_MONTHS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nse-xbrl-cache) NSE_XBRL_CACHE="$2"; shift 2 ;;
    --corp-actions-cache) CORP_ACTIONS_CACHE="$2"; shift 2 ;;
    --fyers-from) FYERS_FROM="$2"; shift 2 ;;
    --fyers-to) FYERS_TO="$2"; shift 2 ;;
    --mf-holdings-months) shift; while [[ $# -gt 0 && "$1" != --* ]]; do MF_HOLDINGS_MONTHS="$MF_HOLDINGS_MONTHS $1"; shift; done ;;
    *) echo "Unknown arg: $1"; exit 2 ;;
  esac
done

echo "=========================================="
echo "PERSIST CACHED PIPELINE (requires DB write lock)"
echo "Started: $(date)"
echo "=========================================="

if fuser datastore/normalised/alphalens.duckdb 2>/dev/null; then
  echo "✗ DB lock still held by another process — aborting."
  echo "  Check: fuser datastore/normalised/alphalens.duckdb"
  exit 1
fi
echo "✓ Lock free, proceeding"

FAILED=0

# [2026-09-10] Every successful persist below also records the matching
# pipeline_checkpoints step (record_backfill_checkpoints.py) — otherwise
# force_run_date_sync (used by catchup_pipeline_for_dates.py further down)
# has no way to know this data already exists and will redundantly
# re-pull the full universe from FYERS/NSE for these dates. Confirmed
# live: without this, a 3-date catch-up got stuck 10+ minutes re-pulling
# ~2317 tickers for data already published minutes earlier. Only the
# steps whose real download genuinely happened get checkpointed this way
# — validation/computation steps (fyers_health_check, adjust_prices,
# derive_fundamentals_ratios, data_integrity_check) are left for
# catchup_pipeline_for_dates.py to actually run, since faking those would
# risk skipping a genuinely necessary recompute.
if [[ -n "$FYERS_FROM" && -n "$FYERS_TO" ]]; then
  echo ""
  echo "[FYERS] Persisting cached OHLCV ($FYERS_FROM..$FYERS_TO)"
  PYTHONPATH=. timeout 1800 .venv/bin/python3 scripts/fyers_multiday_backfill.py \
    --start-date "$FYERS_FROM" --end-date "$FYERS_TO" --mode persist
  if [ $? -eq 0 ]; then
    echo "✓ DONE"
    .venv/bin/python3 scripts/record_backfill_checkpoints.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO" --steps download_fyers_daily
  else
    echo "✗ FAILED"; ((FAILED++))
  fi

  echo ""
  echo "[DELIVERY] Upserting delivery_qty/delivery_pct from bhavcopy ($FYERS_FROM..$FYERS_TO)"
  timeout 1800 .venv/bin/python3 scripts/backfill_delivery_from_bhavcopy.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO"
  if [ $? -eq 0 ]; then
    echo "✓ DONE"
    .venv/bin/python3 scripts/record_backfill_checkpoints.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO" --steps download_bhavcopy
  else
    echo "✗ FAILED"; ((FAILED++))
  fi

  echo ""
  echo "[INDEX] Persisting index OHLCV ($FYERS_FROM..$FYERS_TO)"
  timeout 600 .venv/bin/python3 scripts/backfill_index_ohlcv.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO"
  if [ $? -eq 0 ]; then
    echo "✓ DONE"
    .venv/bin/python3 scripts/record_backfill_checkpoints.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO" --steps download_index_ohlcv
  else
    echo "✗ FAILED"; ((FAILED++))
  fi
else
  echo ""
  echo "[FYERS/DELIVERY/INDEX] Skipped — no --fyers-from/--fyers-to given"
fi

if [[ -n "$NSE_XBRL_CACHE" ]]; then
  echo ""
  echo "[NSE XBRL] Persisting cached fundamentals delta from $NSE_XBRL_CACHE"
  timeout 600 .venv/bin/python3 scripts/backfill_fundamentals_nse_xbrl.py --persist-from-cache "$NSE_XBRL_CACHE"
  [ $? -eq 0 ] && echo "✓ DONE" || { echo "✗ FAILED"; ((FAILED++)); }
else
  echo ""
  echo "[NSE XBRL] Skipped — no --nse-xbrl-cache given"
fi

if [[ -n "$CORP_ACTIONS_CACHE" ]]; then
  echo ""
  echo "[CORPORATE ACTIONS] Persisting cached rows from $CORP_ACTIONS_CACHE"
  timeout 300 .venv/bin/python3 scripts/backfill_corporate_actions.py --persist-from-cache "$CORP_ACTIONS_CACHE"
  CA_RC=$?
  if [ $CA_RC -eq 0 ]; then
    echo "✓ DONE"
    if [[ -n "$FYERS_FROM" && -n "$FYERS_TO" ]]; then
      .venv/bin/python3 scripts/record_backfill_checkpoints.py --from-date "$FYERS_FROM" --to-date "$FYERS_TO" --steps download_corporate_actions
    fi
  else
    echo "✗ FAILED"; ((FAILED++))
  fi
else
  echo ""
  echo "[CORPORATE ACTIONS] Skipped — no --corp-actions-cache given"
fi

echo ""
echo "[TRENDLYNE] Persisting cached fundamentals (if any cache exists)"
timeout 600 .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --phase 2
[ $? -eq 0 ] && echo "✓ DONE" || { echo "⚠ SKIPPED/FAILED (no cache, or already empty)"; }

if [[ -n "$MF_HOLDINGS_MONTHS" ]]; then
  echo ""
  echo "[MF HOLDINGS] Persisting cached months:$MF_HOLDINGS_MONTHS"
  timeout 600 .venv/bin/python3 scripts/backfill_mf_holdings.py --months $MF_HOLDINGS_MONTHS --mode persist
  [ $? -eq 0 ] && echo "✓ DONE" || { echo "✗ FAILED"; ((FAILED++)); }
else
  echo ""
  echo "[MF HOLDINGS] Skipped — no --mf-holdings-months given"
fi

# [2026-09-10] Raw data being current isn't the same as the pipeline being
# current — features and momentum signals for the newly-published dates
# don't exist until this runs too. ML model inference (run_models) is
# deliberately skipped by default (user decision — revisiting the ML
# strategy approach soon); pass INCLUDE_ML=1 in the environment to include it.
if [[ -n "$FYERS_FROM" && -n "$FYERS_TO" ]]; then
  echo ""
  echo "[CATCHUP] Feature generation + momentum signals ($FYERS_FROM..$FYERS_TO)"
  echo "  --from-prerequisites: download_fyers_daily/download_bhavcopy/download_index_ohlcv/"
  echo "  download_corporate_actions are already checkpointed above (real data, real download,"
  echo "  just via this script instead of the live pipeline) — only the genuinely untouched"
  echo "  steps (F&O, macro, large deals) and validation/compute steps actually run here."
  CATCHUP_FLAGS="--from-prerequisites"
  if [[ "${INCLUDE_ML:-0}" == "1" ]]; then
    CATCHUP_FLAGS="$CATCHUP_FLAGS --include-ml"
    echo "  INCLUDE_ML=1 — will also run ML model inference"
  fi
  timeout 7200 .venv/bin/python3 scripts/catchup_pipeline_for_dates.py \
    --from-date "$FYERS_FROM" --to-date "$FYERS_TO" $CATCHUP_FLAGS
  [ $? -eq 0 ] && echo "✓ DONE" || { echo "✗ FAILED (some dates) — see output above, re-run individually to retry"; ((FAILED++)); }
else
  echo ""
  echo "[CATCHUP] Skipped — no --fyers-from/--fyers-to given (nothing new to compute features for)"
fi

echo ""
echo "=========================================="
echo "PERSIST CACHED PIPELINE COMPLETE ($FAILED failure(s))"
echo "Completed: $(date)"
echo "=========================================="
