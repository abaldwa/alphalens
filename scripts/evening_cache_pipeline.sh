#!/bin/bash
# scripts/evening_cache_pipeline.sh
#
# [2026-09-10] "Cache everything locally starting 6pm, persist only when I
# allow it" — user-requested architecture. Every job in this script fetches
# from its external source and writes to a LOCAL cache (parquet/JSON/raw
# file) ONLY. Not one of them requests the DuckDB write lock, so this can
# run for as long as needed (hours, overnight, across a running momentum
# campaign or anything else holding the DB) without ever contending for it.
#
# Jobs run in PARALLEL (2026-09-10 fix) — they're independent sources with
# independent cache locations (FYERS parquet cache, datastore/raw/bhavcopy/,
# NSE XBRL JSON, corporate-actions parquet, Trendlyne JSON) and none of them
# touch the DB, so there's no reason to serialize them. FYERS (2000+ tickers)
# is the long pole; running the other four alongside it costs nothing extra
# in wall-clock time. Each job's own internal concurrency (e.g. FYERS'
# 6-way ThreadPoolExecutor) is unaffected — this just stops making unrelated
# JOBS wait on each other.
#
# Companion script: scripts/persist_cached_pipeline.sh — run that ONLY when
# you explicitly decide to publish everything cached here. Nothing in this
# script auto-triggers that one.
#
# Usage: bash scripts/evening_cache_pipeline.sh
# Intended trigger: 6:00 PM IST (matches config.settings.PIPELINE_WINDOW_START),
# run manually or via a cron/systemd timer — NOT auto-chained to persist.
set -a
source /home/amit/projects/AlphaLens/.env
set +a
cd /home/amit/projects/AlphaLens

CACHE_ROOT="datastore/cache/evening_$(date +%Y%m%d)"
mkdir -p "$CACHE_ROOT"
LOG="/tmp/evening_cache_pipeline.log"
exec > >(tee -a "$LOG")
exec 2>&1


# NOTE: TO_DATE is intentionally always "today" here — the actual
# not-fetchable-before-6pm clipping now lives in
# ingestion.scheduler.gap_detector.latest_fetchable_date(), which both
# fyers_multiday_backfill.py and backfill_bhavcopy_raw.py apply to
# --end-date/--to-date themselves. Keeping the guard in the Python layer
# (not here) means it's enforced no matter how/when those scripts are
# invoked, not just when launched through this one orchestrator.
TO_DATE="$(date +%Y-%m-%d)"
LAST_DB_DATE="$(.venv/bin/python3 -c "
import duckdb
conn = duckdb.connect('datastore/normalised/alphalens.duckdb', read_only=True)
print(conn.execute('SELECT MAX(date) FROM ohlcv_adjusted').fetchone()[0])
conn.close()
" 2>/dev/null)"
FROM_DATE="$(.venv/bin/python3 -c "
from datetime import date, timedelta
print(date.fromisoformat('$LAST_DB_DATE') + timedelta(days=1))
")"

# MF holdings: separate, monthly-granularity gap (last full month in DB ->
# last month whose disclosure is actually public yet, per the ~5th-of-
# next-month SPEC-PIPE-003 delay).
MF_MISSING_MONTHS="$(.venv/bin/python3 -c "
import duckdb
from datetime import date
from config.settings import MF_HOLDINGS_AVAILABILITY_DELAY_DAYS
conn = duckdb.connect('datastore/normalised/alphalens.duckdb', read_only=True)
last = conn.execute('SELECT MAX(month) FROM mf_holdings').fetchone()[0]
conn.close()
today = date.today()
months = []
y, m = last.year, last.month
while True:
    m += 1
    if m == 13:
        m = 1; y += 1
    disclosed_from = date(y + (1 if m == 12 else 0), (m % 12) + 1, MF_HOLDINGS_AVAILABILITY_DELAY_DAYS)
    if disclosed_from > today:
        break
    months.append(f'{y:04d}-{m:02d}')
print(' '.join(months))
")"

echo "=========================================="
echo "EVENING CACHE PIPELINE — fetch only, zero DB writes, PARALLEL"
echo "Started: $(date)"
echo "Gap: $LAST_DB_DATE (last DB date) -> backfilling $FROM_DATE..$TO_DATE"
echo "Cache root: $CACHE_ROOT"
echo "=========================================="

JOB_LOG_DIR="/tmp/evening_cache_jobs_$(date +%s)"
mkdir -p "$JOB_LOG_DIR"
echo "Per-job logs: $JOB_LOG_DIR/"
echo ""

declare -A PIDS
declare -A LOGS

start_job() {
  local name="$1"; shift
  local logfile="$JOB_LOG_DIR/${name}.log"
  ("$@" > "$logfile" 2>&1) &
  PIDS["$name"]=$!
  LOGS["$name"]="$logfile"
  echo "[STARTED] $name (PID ${PIDS[$name]}) -> $logfile"
}

start_job "fyers_ohlcv" \
  env PYTHONPATH=. timeout 3600 .venv/bin/python3 scripts/fyers_multiday_backfill.py \
    --start-date "$FROM_DATE" --end-date "$TO_DATE" --mode fetch

start_job "bhavcopy_raw" \
  timeout 1800 .venv/bin/python3 scripts/backfill_bhavcopy_raw.py \
    --from-date "$FROM_DATE" --to-date "$TO_DATE"

start_job "nse_xbrl" \
  timeout 3600 .venv/bin/python3 scripts/backfill_fundamentals_nse_xbrl.py \
    --cache-file "$CACHE_ROOT/nse_xbrl_delta.json"

start_job "corporate_actions" \
  timeout 1800 .venv/bin/python3 scripts/backfill_corporate_actions.py \
    --from-date "$FROM_DATE" --to-date "$TO_DATE" \
    --cache-file "$CACHE_ROOT/corporate_actions.parquet"

start_job "trendlyne" \
  timeout 3600 .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --phase 1

if [ -n "$MF_MISSING_MONTHS" ]; then
  start_job "mf_holdings" \
    timeout 3600 .venv/bin/python3 scripts/backfill_mf_holdings.py --months $MF_MISSING_MONTHS --mode fetch
else
  echo "[SKIPPED] mf_holdings — no missing months (already current)"
fi

echo ""
echo "All jobs launched in parallel. Waiting for completion..."
echo ""

FAILED=0
for name in "${!PIDS[@]}"; do
  wait "${PIDS[$name]}"
  rc=$?
  if [ $rc -eq 0 ]; then
    echo "[$name] ✓ CACHED (see ${LOGS[$name]})"
  else
    echo "[$name] ✗ FAILED (exit $rc) — see ${LOGS[$name]}"
    ((FAILED++))
  fi
done

echo ""
echo "=========================================="
echo "EVENING CACHE PIPELINE COMPLETE ($FAILED failure(s))"
echo "Completed: $(date)"
echo ""
echo "Nothing was written to DuckDB. All fetched data sits in local cache:"
echo "  - FYERS OHLCV:        parquet cache inside fyers_multiday_backfill's TICKER_CACHE_DIR"
echo "  - Bhavcopy raw:       datastore/raw/bhavcopy/"
echo "  - NSE XBRL delta:     $CACHE_ROOT/nse_xbrl_delta.json"
echo "  - Corporate actions:  $CACHE_ROOT/corporate_actions.parquet"
echo "  - Trendlyne:          datastore/cache/trendlyne/"
echo "  - MF holdings:        datastore/normalised/mf_holdings/*.parquet (months: ${MF_MISSING_MONTHS:-none pending})"
echo ""
echo "When ready to publish, run:"
echo "  bash scripts/persist_cached_pipeline.sh --nse-xbrl-cache $CACHE_ROOT/nse_xbrl_delta.json --corp-actions-cache $CACHE_ROOT/corporate_actions.parquet --fyers-from $FROM_DATE --fyers-to $TO_DATE"
if [ -n "$MF_MISSING_MONTHS" ]; then
  echo "  .venv/bin/python3 scripts/backfill_mf_holdings.py --months $MF_MISSING_MONTHS --mode persist"
fi
echo "=========================================="
