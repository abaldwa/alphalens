#!/usr/bin/env bash
# Stage 2 of the 2007-start rebuild.
#
# The Fyers-primary backfill (2026-08-11/12) rewrote ~1.5M ticker-days of
# ohlcv_adjusted across 2007-2016, correcting adjustment factors that had
# manufactured impossible returns (BAJFINANCE entered at Rs 0.04 in 2010 vs a
# real Rs 3.16, fabricating +18,610%). Every feature parquet for those years
# was derived from the pre-backfill prices and is therefore stale.
#
# RECOMPUTE_TARGETS_DIR/<year>.parquet records the changed cells. As with the
# 2017-2025 pass, virtually every trading day of every year contains at least
# one changed ticker, so a --force'd whole-year recompute is both the correct
# and the cheapest granularity — selecting individual dates would save nothing.
#
# BLAS is pinned to one thread per worker: the panel workers are already
# process-parallel, so letting each spawn its own thread pool oversubscribes
# the box and measured ~62% slower on this hardware.
set -uo pipefail
cd /home/amit/projects/AlphaLens
export PYTHONPATH=/home/amit/projects/AlphaLens

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

YEARS="${YEARS:-2007 2008 2009 2010 2011 2012 2013 2014 2015 2016}"
WORKERS="${PANEL_WORKERS:-6}"

for YEAR in $YEARS; do
    echo "=== FYERS_RECOMPUTE: starting year $YEAR ($(date +%H:%M:%S)) ==="
    .venv/bin/python3 scripts/feature_backfill.py \
        --from-date "${YEAR}-01-01" \
        --to-date "${YEAR}-12-31" \
        --force \
        --advanced-technical-used-only \
        --skip-slow-categories \
        --no-hmm \
        --panel-workers "$WORKERS" \
        --run-id "fyers_recompute_2007_2016_${YEAR}"
    STATUS=$?
    if [ $STATUS -ne 0 ]; then
        echo "=== FYERS_RECOMPUTE: year $YEAR FAILED (exit $STATUS) ==="
        exit $STATUS
    fi
    echo "=== FYERS_RECOMPUTE: year $YEAR done ($(date +%H:%M:%S)) ==="
done
echo "=== FYERS_RECOMPUTE: all years complete ==="
