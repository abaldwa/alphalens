#!/usr/bin/env bash
# Year-by-year feature/parquet recompute for the (ticker, date) cells that
# changed during the Fyers-primary OHLCV backfill (2026-08-04). Recomputes
# by whole trading day (the panel-builder's atomic unit) for every year
# where the RECOMPUTE_TARGETS_DIR/<year>.parquet manifest shows at least
# one changed ticker-day — in practice that is virtually every trading day
# of every year (see check: 145-252 of ~248 dates per year), so a
# --force'd whole-year run over exactly those years is the correct and
# efficient granularity rather than trying to select individual dates.
set -uo pipefail
cd /home/amit/projects/AlphaLens
export PYTHONPATH=/home/amit/projects/AlphaLens

YEARS="2025 2024 2023 2022 2021 2020 2019 2018 2017"

for YEAR in $YEARS; do
    echo "=== FYERS_RECOMPUTE: starting year $YEAR ==="
    .venv/bin/python3 scripts/feature_backfill.py \
        --from-date "${YEAR}-01-01" \
        --to-date "${YEAR}-12-31" \
        --force \
        --advanced-technical-used-only \
        --skip-slow-categories \
        --no-hmm \
        --panel-workers 8 \
        --run-id "fyers_recompute_v2_${YEAR}"
    STATUS=$?
    if [ $STATUS -ne 0 ]; then
        echo "=== FYERS_RECOMPUTE: year $YEAR FAILED (exit $STATUS) ==="
        exit $STATUS
    fi
    echo "=== FYERS_RECOMPUTE: year $YEAR done ==="
done
echo "=== FYERS_RECOMPUTE: all years complete ==="
