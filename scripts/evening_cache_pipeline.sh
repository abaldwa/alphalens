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

echo "=========================================="
echo "EVENING CACHE PIPELINE — fetch only, zero DB writes"
echo "Started: $(date)"
echo "Gap: $LAST_DB_DATE (last DB date) -> backfilling $FROM_DATE..$TO_DATE"
echo "Cache root: $CACHE_ROOT"
echo "=========================================="

JOB=0
TOTAL=5
FAILED=0

((JOB++))
echo ""
echo "[$JOB/$TOTAL] FYERS OHLCV — fetch to parquet cache ($FROM_DATE..$TO_DATE)"
PYTHONPATH=. timeout 3600 .venv/bin/python3 scripts/fyers_multiday_backfill.py \
  --start-date "$FROM_DATE" --end-date "$TO_DATE" --mode fetch
[ $? -eq 0 ] && echo "✓ CACHED" || { echo "✗ FAILED"; ((FAILED++)); }

((JOB++))
echo ""
echo "[$JOB/$TOTAL] Raw bhavcopy landing zone ($FROM_DATE..$TO_DATE) — always cache-only"
timeout 1800 .venv/bin/python3 scripts/backfill_bhavcopy_raw.py --from-date "$FROM_DATE" --to-date "$TO_DATE"
[ $? -eq 0 ] && echo "✓ CACHED" || { echo "✗ FAILED"; ((FAILED++)); }

((JOB++))
echo ""
echo "[$JOB/$TOTAL] NSE XBRL fundamentals — scan + fetch to JSON cache"
timeout 3600 .venv/bin/python3 scripts/backfill_fundamentals_nse_xbrl.py \
  --cache-file "$CACHE_ROOT/nse_xbrl_delta.json"
[ $? -eq 0 ] && echo "✓ CACHED" || { echo "✗ FAILED"; ((FAILED++)); }

((JOB++))
echo ""
echo "[$JOB/$TOTAL] Corporate actions — fetch to parquet cache ($FROM_DATE..$TO_DATE)"
timeout 1800 .venv/bin/python3 scripts/backfill_corporate_actions.py \
  --from-date "$FROM_DATE" --to-date "$TO_DATE" \
  --cache-file "$CACHE_ROOT/corporate_actions.parquet"
[ $? -eq 0 ] && echo "✓ CACHED" || { echo "✗ FAILED"; ((FAILED++)); }

((JOB++))
echo ""
echo "[$JOB/$TOTAL] Trendlyne fundamentals — Phase 1 scrape to JSON cache"
timeout 3600 .venv/bin/python3 scripts/trendlyne_backfill_two_phase.py --phase 1
[ $? -eq 0 ] && echo "✓ CACHED" || { echo "⚠ FAILED/BLOCKED (WAF?) — see log"; ((FAILED++)); }

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
echo ""
echo "When ready to publish, run:"
echo "  bash scripts/persist_cached_pipeline.sh --nse-xbrl-cache $CACHE_ROOT/nse_xbrl_delta.json --corp-actions-cache $CACHE_ROOT/corporate_actions.parquet --fyers-from $FROM_DATE --fyers-to $TO_DATE"
echo "=========================================="
