#!/usr/bin/env bash
# Waits for the in-flight 2008-2016 recompute loop to exit, then recomputes 2007.
#
# 2007 was skipped by the main loop: an earlier kill left the sentinel
# datastore/features/daily/.fyers_recompute_2007_2016_2007.force_applied behind,
# so --force was silently downgraded to skip-if-exists and only 1 of 249 dates
# was processed. That sentinel has been removed, and this pass uses a fresh
# run-id as belt-and-braces.
#
# The backtest starts 2009-04-01, so 2007's feature parquets are not read by it.
# Recomputing them anyway (user, 2026-08-12) keeps the feature store consistent
# with the corrected OHLCV rather than leaving a stale year that would silently
# poison any future run that happens to start earlier.
set -uo pipefail
cd /home/amit/projects/AlphaLens

until ! pgrep -f run_fyers_feature_recompute_2007_2016.sh >/dev/null 2>&1; do
    sleep 60
done

echo "=== CHAIN: 2008-2016 loop finished, starting 2007 at $(date +%H:%M:%S) ==="
YEARS="2007" PANEL_WORKERS="${PANEL_WORKERS:-6}" \
    scripts/run_fyers_feature_recompute_2007_2016.sh
echo "=== CHAIN: 2007 done at $(date +%H:%M:%S) ==="
